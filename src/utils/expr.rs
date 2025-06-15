use std;
use std::f64::consts;
use std::ops::Deref;
use std::rc::Rc;
use std::str::FromStr;

use fnv::FnvHashMap;

use crate::utils::shuntingyard::to_rpn;
use crate::utils::tokenizer::tokenize;
use crate::model::south::{FuncEvalError, Operation, Token, Token::*, factorial, ContextProvider, Error, Expr};
type ContextHashMap<K, V> = FnvHashMap<K, V>;

/**
 * @api {Expr} /Expr Expr
 * @apiPrivate
 * @apiGroup A_Object
 * @apiSuccess {Token[]} rpn rpn
 */
/// Representation of a parsed expression.
///
/// The expression is internally stored in the [reverse Polish notation (RPN)][RPN] as a sequence
/// of `Token`s.
///
/// Methods `bind`, `bind_with_context`, `bind2`, ... can be used to create  closures from
/// the expression that then can be passed around and used as any other `Fn` closures.
///
/// let func = "x^2".parse::<Expr>().unwrap().bind("x").unwrap();
/// let r = Some(2.).map(func);
/// assert_eq!(r, Some(4.));
///
/// [RPN]: https://en.wikipedia.org/wiki/Reverse_Polish_notation

impl Expr {
    pub fn new() -> Expr {
        Expr::default()
    }

    pub fn from_vec(rpn: Vec<Token>) -> Expr {
        Expr { rpn }
    }

    /// Evaluates the expression.
    pub fn eval(&self) -> Result<f64, Error> {
        self.eval_with_context(builtin())
    }

    /// Evaluates the expression with variables given by the argument.
    pub fn eval_with_context<C: ContextProvider>(&self, ctx: C) -> Result<f64, Error> {
        let mut stack = Vec::with_capacity(16);
        if self.rpn.is_empty() {
            return Err(Error::EmptyExpression);
        }

        for token in &self.rpn {
            match *token {
                Var(ref n) => {
                    match ctx.get_var(n) { 
                        Some(v) => {
                            stack.push(v);
                        } 
                        _ => {
                            return Err(Error::UnknownVariable(n.clone()));
                        }
                    }
                }
                Number(f) => stack.push(f),
                Binary(op) => {
                    let right = stack.pop().unwrap();
                    let left = stack.pop().unwrap();
                    let r = match op {
                        Operation::Plus => left + right,
                        Operation::Minus => left - right,
                        Operation::Times => left * right,
                        Operation::Div => left / right,
                        Operation::Rem => left % right,
                        Operation::Pow => left.powf(right),
                        // added by dsf, 2021.3
                        Operation::LessThan => {
                            if left < right {
                                1.0
                            } else {
                                0.0
                            }
                        }
                        Operation::GreatThan => {
                            if left > right {
                                1.0
                            } else {
                                0.0
                            }
                        }
                        Operation::LtOrEqual => {
                            if left <= right {
                                1.0
                            } else {
                                0.0
                            }
                        }
                        Operation::GtOrEqual => {
                            if left >= right {
                                1.0
                            } else {
                                0.0
                            }
                        }
                        Operation::Equal => {
                            if left == right {
                                1.0
                            } else {
                                0.0
                            }
                        }
                        Operation::Unequal => {
                            if left != right {
                                1.0
                            } else {
                                0.0
                            }
                        }
                        Operation::And => {
                            if (left > 0.0) && (right > 0.0) {
                                1.0
                            } else {
                                0.0
                            }
                        }
                        Operation::Or => {
                            if (left > 0.0) || (right > 0.0) {
                                1.0
                            } else {
                                0.0
                            }
                        }
                        Operation::BitAnd => (left as i64 & right as i64) as f64,
                        Operation::BitOr => (left as i64 | right as i64) as f64,
                        Operation::BitXor => (left as i64 ^ right as i64) as f64,
                        Operation::BitShl => ((left as i64) << (right as i64)) as f64,
                        Operation::BitShr => ((left as i64) >> (right as i64)) as f64,
                        Operation::BitAt => {
                            #[allow(clippy::manual_range_contains)]
                            if right < 1. || right > 64. {
                                return Err(Error::EvalError(format!(
                                    "Operation \"@\" ERROR:the {:?} bit doesn't exist.",
                                    right
                                )));
                            }
                            if (left as i64) & 2_i64.pow(right as u32 - 1) != 0 {
                                1.0
                            } else {
                                0.0
                            }
                        }
                        _ => {
                            return Err(Error::EvalError(format!(
                                "Unimplemented binary operation: {:?}",
                                op
                            )));
                        }
                    };
                    stack.push(r);
                }
                Unary(op) => {
                    let x = stack.pop().unwrap();
                    let r = match op {
                        Operation::Plus => x,
                        Operation::Minus => -x,
                        Operation::Not => {
                            if x > 0.0 {
                                0.0
                            } else {
                                1.0
                            }
                        },
                        Operation::BitNot => !(x as i64) as f64,
                        Operation::Fact => {
                            // Check to make sure x has no fractional component (can be converted to int without loss)
                            match factorial(x) {
                                Ok(res) => res,
                                Err(e) => return Err(Error::EvalError(String::from(e))),
                            }
                        }
                        _ => {
                            let msg = format!("Unimplemented unary operation: {:?}", op);
                            return Err(Error::EvalError(msg));
                        }
                    };
                    stack.push(r);
                }
                Func(ref n, Some(i)) => {
                    if stack.len() < i {
                        let msg = format!("eval: stack does not have enough arguments for function token {:?}", token);
                        return Err(Error::EvalError(msg));
                    }
                    match ctx.eval_func(n, &stack[stack.len() - i..]) {
                        Ok(r) => {
                            let nl = stack.len() - i;
                            stack.truncate(nl);
                            stack.push(r);
                        }
                        Err(e) => return Err(Error::Function(n.to_owned(), e)),
                    }
                }
                Func(ref n, None) => match ctx.eval_func(n, &[]) {
                    Ok(r) => {
                        stack.push(r);
                    }
                    Err(e) => return Err(Error::Function(n.to_owned(), e)),
                },
                _ => return Err(Error::EvalError(format!("Unrecognized token: {:?}", token))),
            }
        }

        let mut r = stack.pop().expect("Stack is empty, this is impossible.");
        if !stack.is_empty() {
            return Err(Error::EvalError(format!("There are still {} items on the stack.", stack.len())));
        }
        // inf
        if r.is_infinite() {
            // warn!("the result of the expression is inf");
            if r.is_sign_positive() {
                r = f64::MAX;
            } else {
                r = f64::MIN;
            }
        }
        Ok(r)
    }
    pub fn push_vec(&mut self, expr: Expr) {
        if let Some(mut token) = self.rpn.pop() {
            if let Tensor(size) = &mut token {
                if let Some(n) = size {
                    *n += 1;
                } else {
                    *size = Some(1);
                }
            }
            self.rpn.extend(expr.rpn);
            self.rpn.push(token);
        }
    }

    pub fn split_fun(mut self) -> Option<Vec<Expr>> {
        let token = self.rpn.pop()?;
        let size : usize;
        if let Func(_, n) = token {
            if let Some(n) = n {
                size = n;
            } else {
                return Some(vec![]);
            }
        } else {
            return None;
        }
        self.split(size)
    }
    pub fn split_vec(mut self) -> Option<Vec<Expr>> {
        let token = self.rpn.pop()?;
        let size : usize;
        if let Tensor(n) = token {
            if let Some(n) = n {
                size = n;
            } else {
                return Some(vec![]);
            }
        } else {
            return None;
        }
        self.split(size)
    }
    fn split(self, size: usize) -> Option<Vec<Expr>> {
        let mut r = Vec::with_capacity(16);
        // 对模型进行检查
        for t in self.rpn {
            match t {
                Var(_) => r.push(Expr::from_vec(vec![t])),
                Number(_) => r.push(Expr::from_vec(vec![t])),
                Binary(_) => {
                    let expr1 = r.pop().unwrap().rpn;
                    let mut expr2 = r.pop().unwrap().rpn;
                    expr2.extend(expr1);
                    expr2.push(t);
                    r.push(Expr::from_vec(expr2));
                }
                Unary(_) => {
                    r.last_mut().unwrap().rpn.push(t);
                }
                Tensor(size) => {
                    match size {
                        None => {},
                        Some(i) => {
                            if i == 0 {
                                r.push(Expr::from_vec(vec![t]));
                                continue;
                            }
                            let first = r.len() - i;
                            let mut expr = r[first].clone();
                            for j in 1..i {
                                expr.rpn.extend(r[first + j].clone().rpn);
                            }
                            expr.rpn.push(t);
                            let nl = r.len() - i;
                            r.truncate(nl);
                            r.push(expr);
                        }
                    }
                }
                Func(_, Some(i)) => {
                    let first = r.len() - i;
                    let mut expr = r[first].clone();
                    for j in 1..i {
                        expr.rpn.extend(r[first + j].clone().rpn);
                    }
                    expr.rpn.push(t);
                    let nl = r.len() - i;
                    r.truncate(nl);
                    r.push(expr);
                }
                Func(_, None) => {
                    r.last_mut().unwrap().rpn.push(t);
                },
                _ => return None,
            }
        }
        if r.len() == size {
            Some(r)
        } else {
            None
        }
    }

    /// check expression is valid
    pub fn check_validity(&self) -> bool {
        let mut stack = Vec::with_capacity(16);
        // 对模型进行检查
        for token in &self.rpn {
            match *token {
                Var(_) => stack.push(0u8),
                Number(_) => stack.push(0u8),
                Binary(_) => {
                    if stack.len() < 2 {
                        return false;
                    }
                    stack.truncate(stack.len() - 1);
                }
                Unary(_) => {
                    if stack.is_empty() {
                        return false;
                    }
                }
                Tensor(size) => {
                    match size {
                        None => {},
                        Some(i) => {
                            if stack.len() < i {
                                return false;
                            }
                            let nl = stack.len() - i + 1;
                            stack.truncate(nl);
                        }
                    }
                }
                Func(_, Some(i)) => {
                    if stack.len() < i {
                        return false;
                    }
                    let nl = stack.len() - i;
                    stack.truncate(nl);
                    stack.push(0u8);
                }
                Func(_, None) => stack.push(0u8),
                _ => return false,
            }
        }
        stack.len() == 1
    }

    /// Creates a function of one variable based on this expression, with default constants and
    /// functions.
    ///
    /// Binds the input of the returned closure to `var`.
    ///
    /// # Failure
    ///
    /// Returns `Err` if there is a variable in the expression that is not provided by the default
    /// context or `var`.
    pub fn bind<'a>(self, var: &str) -> Result<impl Fn(f64) -> f64 + 'a + use<'a>, Error> {
        self.bind_with_context(builtin(), var)
    }

    /// Creates a function of one variable based on this expression.
    ///
    /// Binds the input of the returned closure to `var`.
    ///
    /// # Failure
    ///
    /// Returns `Err` if there is a variable in the expression that is not provided by `ctx` or
    /// `var`.
    pub fn bind_with_context<'a, C>(
        self,
        ctx: C,
        var: &str,
    ) -> Result<impl Fn(f64) -> f64 + 'a + use<'a, C>, Error>
    where
        C: ContextProvider + 'a,
    {
        self.check_context(((var, 0.), &ctx))?;
        let var = var.to_owned();
        Ok(move |x| {
            self.eval_with_context(((&var, x), &ctx))
                .expect("Expr::bind")
        })
    }

    /// Creates a function of two variables based on this expression, with default constants and
    /// functions.
    ///
    /// Binds the inputs of the returned closure to `var1` and `var2`.
    ///
    /// # Failure
    ///
    /// Returns `Err` if there is a variable in the expression that is not provided by the default
    /// context or `var`.
    pub fn bind2<'a>(self, var1: &str, var2: &str) -> Result<impl Fn(f64, f64) -> f64 + 'a + use<'a>, Error> {
        self.bind2_with_context(builtin(), var1, var2)
    }

    /// Creates a function of two variables based on this expression.
    ///
    /// Binds the inputs of the returned closure to `var1` and `var2`.
    ///
    /// # Failure
    ///
    /// Returns `Err` if there is a variable in the expression that is not provided by `ctx` or
    /// `var`.
    pub fn bind2_with_context<'a, C>(
        self,
        ctx: C,
        var1: &str,
        var2: &str,
    ) -> Result<impl Fn(f64, f64) -> f64 + 'a + use<'a, C>, Error>
    where
        C: ContextProvider + 'a,
    {
        self.check_context(([(var1, 0.), (var2, 0.)], &ctx))?;
        let var1 = var1.to_owned();
        let var2 = var2.to_owned();
        Ok(move |x, y| {
            self.eval_with_context(([(&var1, x), (&var2, y)], &ctx))
                .expect("Expr::bind2")
        })
    }

    /// Creates a function of three variables based on this expression, with default constants and
    /// functions.
    ///
    /// Binds the inputs of the returned closure to `var1`, `var2` and `var3`.
    ///
    /// # Failure
    ///
    /// Returns `Err` if there is a variable in the expression that is not provided by the default
    /// context or `var`.
    pub fn bind3<'a>(
        self,
        var1: &str,
        var2: &str,
        var3: &str,
    ) -> Result<impl Fn(f64, f64, f64) -> f64 + 'a + use<'a>, Error> {
        self.bind3_with_context(builtin(), var1, var2, var3)
    }

    /// Creates a function of three variables based on this expression.
    ///
    /// Binds the inputs of the returned closure to `var1`, `var2` and `var3`.
    ///
    /// # Failure
    ///
    /// Returns `Err` if there is a variable in the expression that is not provided by `ctx` or
    /// `var`.
    pub fn bind3_with_context<'a, C>(
        self,
        ctx: C,
        var1: &str,
        var2: &str,
        var3: &str,
    ) -> Result<impl Fn(f64, f64, f64) -> f64 + 'a + use<'a, C>, Error>
    where
        C: ContextProvider + 'a,
    {
        self.check_context(([(var1, 0.), (var2, 0.), (var3, 0.)], &ctx))?;
        let var1 = var1.to_owned();
        let var2 = var2.to_owned();
        let var3 = var3.to_owned();
        Ok(move |x, y, z| {
            self.eval_with_context(([(&var1, x), (&var2, y), (&var3, z)], &ctx))
                .expect("Expr::bind3")
        })
    }

    /// Creates a function of four variables based on this expression, with default constants and
    /// functions.
    ///
    /// Binds the inputs of the returned closure to `var1`, `var2`, `var3` and `var4`.
    ///
    /// # Failure
    ///
    /// Returns `Err` if there is a variable in the expression that is not provided by the default
    /// context or `var`.
    pub fn bind4<'a>(
        self,
        var1: &str,
        var2: &str,
        var3: &str,
        var4: &str,
    ) -> Result<impl Fn(f64, f64, f64, f64) -> f64 + 'a + use<'a>, Error> {
        self.bind4_with_context(builtin(), var1, var2, var3, var4)
    }

    /// Creates a function of four variables based on this expression.
    ///
    /// Binds the inputs of the returned closure to `var1`, `var2`, `var3` and `var4`.
    ///
    /// # Failure
    ///
    /// Returns `Err` if there is a variable in the expression that is not provided by `ctx` or
    /// `var`.
    pub fn bind4_with_context<'a, C>(
        self,
        ctx: C,
        var1: &str,
        var2: &str,
        var3: &str,
        var4: &str,
    ) -> Result<impl Fn(f64, f64, f64, f64) -> f64 + 'a + use<'a, C>, Error>
    where
        C: ContextProvider + 'a,
    {
        self.check_context(([(var1, 0.), (var2, 0.), (var3, 0.), (var4, 0.)], &ctx))?;
        let var1 = var1.to_owned();
        let var2 = var2.to_owned();
        let var3 = var3.to_owned();
        let var4 = var4.to_owned();
        Ok(move |x1, x2, x3, x4| {
            self.eval_with_context(([(&var1, x1), (&var2, x2), (&var3, x3), (&var4, x4)], &ctx))
                .expect("Expr::bind4")
        })
    }

    /// Creates a function of five variables based on this expression, with default constants and
    /// functions.
    ///
    /// Binds the inputs of the returned closure to `var1`, `var2`, `var3`, `var4` and `var5`.
    ///
    /// # Failure
    ///
    /// Returns `Err` if there is a variable in the expression that is not provided by the default
    /// context or `var`.
    pub fn bind5<'a>(
        self,
        var1: &str,
        var2: &str,
        var3: &str,
        var4: &str,
        var5: &str,
    ) -> Result<impl Fn(f64, f64, f64, f64, f64) -> f64 + 'a + use<'a>, Error> {
        self.bind5_with_context(builtin(), var1, var2, var3, var4, var5)
    }

    /// Creates a function of five variables based on this expression.
    ///
    /// Binds the inputs of the returned closure to `var1`, `var2`, `var3`, `var4` and `var5`.
    ///
    /// # Failure
    ///
    /// Returns `Err` if there is a variable in the expression that is not provided by `ctx` or
    /// `var`.
    pub fn bind5_with_context<'a, C>(
        self,
        ctx: C,
        var1: &str,
        var2: &str,
        var3: &str,
        var4: &str,
        var5: &str,
    ) -> Result<impl Fn(f64, f64, f64, f64, f64) -> f64 + 'a + use<'a, C>, Error>
    where
        C: ContextProvider + 'a,
    {
        self.check_context((
            [(var1, 0.), (var2, 0.), (var3, 0.), (var4, 0.), (var5, 0.)],
            &ctx,
        ))?;
        let var1 = var1.to_owned();
        let var2 = var2.to_owned();
        let var3 = var3.to_owned();
        let var4 = var4.to_owned();
        let var5 = var5.to_owned();
        Ok(move |x1, x2, x3, x4, x5| {
            self.eval_with_context((
                [
                    (&var1, x1),
                    (&var2, x2),
                    (&var3, x3),
                    (&var4, x4),
                    (&var5, x5),
                ],
                &ctx,
            ))
            .expect("Expr::bind5")
        })
    }

    /// Binds the input of the returned closure to elements of `vars`.
    ///
    /// # Failure
    ///
    /// Returns `Err` if there is a variable in the expression that is not provided by the default
    /// context or `var`.
    pub fn bindn<'a>(self, vars: &'a [&str]) -> Result<impl Fn(&[f64]) -> f64 + 'a, Error> {
        self.bindn_with_context(builtin(), vars)
    }

    /// Creates a function of N variables based on this expression.
    ///
    /// Binds the input of the returned closure to the elements of `vars`.
    ///
    /// # Failure
    ///
    /// Returns `Err` if there is a variable in the expression that is not provided by `ctx` or
    /// `var`.
    pub fn bindn_with_context<'a, C>(
        self,
        ctx: C,
        vars: &'a [&str],
    ) -> Result<impl Fn(&[f64]) -> f64 + 'a, Error>
    where
        C: ContextProvider + 'a,
    {
        let n = vars.len();
        self.check_context((
            vars.iter().zip(vec![0.; n].into_iter()).collect::<Vec<_>>(),
            &ctx,
        ))?;
        let vars = vars.iter().map(|v| v.to_owned()).collect::<Vec<_>>();
        Ok(move |x: &[f64]| {
            self.eval_with_context((
                vars.iter()
                    .zip(x.iter())
                    .map(|(v, x)| (v, *x))
                    .collect::<Vec<_>>(),
                &ctx,
            ))
            .expect("Expr::bindn")
        })
    }

    /// Checks that the value of every variable in the expression is specified by the context `ctx`.
    ///
    /// # Failure
    ///
    /// Returns `Err` if a missing variable is detected.
    fn check_context<C: ContextProvider>(&self, ctx: C) -> Result<(), Error> {
        for t in &self.rpn {
            match *t {
                Var(ref name) => {
                    if ctx.get_var(name).is_none() {
                        return Err(Error::UnknownVariable(name.clone()));
                    }
                }
                Func(ref name, Some(i)) => {
                    let v = vec![0.; i];
                    if let Err(e) = ctx.eval_func(name, &v) {
                        return Err(Error::Function(name.to_owned(), e));
                    }
                }
                Func(_, None) => {
                    return Err(Error::EvalError(format!(
                        "expr::check_context: Unexpected token: {:?}",
                        *t
                    )));
                }
                LParen | RParen | Binary(_) | Unary(_) | Comma | Number(_) => {}
                _ => {}
            }
        }
        Ok(())
    }
}

/// Evaluates a string with built-in constants and functions.
pub fn eval_str<S: AsRef<str>>(expr: S) -> Result<f64, Error> {
    let expr = Expr::from_str(expr.as_ref())?;
    expr.eval_with_context(builtin())
}

impl FromStr for Expr {
    type Err = Error;
    /// Constructs an expression by parsing a string.
    fn from_str(s: &str) -> Result<Self, Error> {
        match tokenize(s) {
            Ok(tokens) => match to_rpn(&tokens) {
                Ok(rpn) => Ok(Expr { rpn }),
                Err(e) => Err(Error::RPNError(e)),
            },
            Err(e) => Err(Error::ParseError(e)),
        }
    }
}

/// Evaluates a string with the given context.
///
/// No built-ins are defined in this case.
pub fn eval_str_with_context<S: AsRef<str>, C: ContextProvider>(
    expr: S,
    ctx: C,
) -> Result<f64, Error> {
    let expr = Expr::from_str(expr.as_ref())?;

    expr.eval_with_context(ctx)
}

impl Deref for Expr {
    type Target = [Token];

    fn deref(&self) -> &[Token] {
        &self.rpn
    }
}


#[doc(hidden)]
#[cfg(feature = "with_rand")]
pub fn random() -> f64 {
    use rand::Rng;
    rand::rng().random::<f64>()
}

#[doc(hidden)]
#[cfg(feature = "with_rand")]
pub fn random2(lower: f64, upper: f64) -> f64 {
    use rand::Rng;
    rand::rng().random_range(lower..upper)
}

#[doc(hidden)]
pub fn max_array(xs: &[f64]) -> f64 {
    xs.iter().fold(f64::NEG_INFINITY, |m, &x| m.max(x))
}

#[doc(hidden)]
pub fn min_array(xs: &[f64]) -> f64 {
    xs.iter().fold(f64::INFINITY, |m, &x| m.min(x))
}

/// Returns the built-in constants and functions in a form that can be used as a `ContextProvider`.
#[doc(hidden)]
pub fn builtin<'a>() -> Context<'a> {
    // TODO: cache this (lazy_static)
    Context::new()
}

/// A structure for storing variables/constants and functions to be used in an expression.
///
/// # Example
///
/// use {eval_str_with_context, Context};
///
/// let mut ctx = Context::new(); // builtins
/// ctx.var("x", 3.)
///    .func("f", |x| 2. * x)
///    .funcn("sum", |xs| xs.iter().sum(), ..);
///
/// assert_eq!(eval_str_with_context("pi + sum(1., 2.) + f(x)", &ctx),
///            Ok(std::f64::consts::PI + 1. + 2. + 2. * 3.));
/// ```
#[derive(Clone)]
pub struct Context<'a> {
    vars: ContextHashMap<String, f64>,
    funcs: ContextHashMap<String, GuardedFunc<'a>>,
    // tensors: ContextHashMap<String, Tensor<'a, f32>>,
}

impl<'a> Context<'a> {
    /// Creates a context with built-in constants and functions.
    pub fn new() -> Context<'a> {
        thread_local!(static DEFAULT_CONTEXT: Context<'static> = {
            let mut ctx = Context::empty();
            ctx.var("pi", consts::PI);
            ctx.var("PI", consts::PI);
            ctx.var("e", consts::E);
            #[cfg(feature = "with_rand")]
            ctx.func0("rand", random);
            ctx.func1("sqrt", f64::sqrt);
            ctx.func1("exp", f64::exp);
            ctx.func1("ln", f64::ln);
            ctx.func1("log10", f64::log10);
            ctx.func1("abs", f64::abs);
            ctx.func1("sin", f64::sin);
            ctx.func1("cos", f64::cos);
            ctx.func1("tan", f64::tan);
            ctx.func1("asin", f64::asin);
            ctx.func1("acos", f64::acos);
            ctx.func1("atan", f64::atan);
            ctx.func1("sinh", f64::sinh);
            ctx.func1("cosh", f64::cosh);
            ctx.func1("tanh", f64::tanh);
            ctx.func1("asinh", f64::asinh);
            ctx.func1("acosh", f64::acosh);
            ctx.func1("atanh", f64::atanh);
            ctx.func1("floor", f64::floor);
            ctx.func1("ceil", f64::ceil);
            ctx.func1("round", f64::round);
            ctx.func1("signum", f64::signum);
            ctx.func2("atan2", f64::atan2);
            #[cfg(feature = "with_rand")]
            ctx.func2("rand2", random2);
            ctx.funcn("max", max_array, 1..);
            ctx.funcn("min", min_array, 1..);
            ctx
        });

        DEFAULT_CONTEXT.with(|ctx| ctx.clone())
    }

    /// Creates an empty contexts.
    pub fn empty() -> Context<'a> {
        Context {
            vars: ContextHashMap::default(),
            funcs: ContextHashMap::default(),
            // tensors: ContextHashMap::default(),
        }
    }

    /// Adds a new variable/constant.
    pub fn var<S: Into<String>>(&mut self, var: S, value: f64) -> &mut Self {
        self.vars.insert(var.into(), value);
        self
    }

    /// Adds a new function of one argument.
    pub fn func0<S, F>(&mut self, name: S, func: F) -> &mut Self
    where
        S: Into<String>,
        F: Fn() -> f64 + 'a,
    {
        self.funcs.insert(name.into(), Rc::new(move |_| Ok(func())));
        self
    }

    /// Adds a new function of one argument.
    pub fn func1<S, F>(&mut self, name: S, func: F) -> &mut Self
    where
        S: Into<String>,
        F: Fn(f64) -> f64 + 'a,
    {
        self.funcs.insert(
            name.into(),
            Rc::new(move |args: &[f64]| {
                if args.len() == 1 {
                    Ok(func(args[0]))
                } else {
                    Err(FuncEvalError::NumberArgs(1))
                }
            }),
        );
        self
    }

    // pub fn tensor<S: Into<String>>(&mut self, var: S, tensor: Tensor<'a, f32>) -> &mut Self
    // {
    //     self.tensors.insert(var.into(), tensor);
    //     self
    // }
    //
    /// Adds a new function of two arguments.
    pub fn func2<S, F>(&mut self, name: S, func: F) -> &mut Self
    where
        S: Into<String>,
        F: Fn(f64, f64) -> f64 + 'a,
    {
        self.funcs.insert(
            name.into(),
            Rc::new(move |args: &[f64]| {
                if args.len() == 2 {
                    Ok(func(args[0], args[1]))
                } else {
                    Err(FuncEvalError::NumberArgs(2))
                }
            }),
        );
        self
    }

    /// Adds a new function of three arguments.
    pub fn func3<S, F>(&mut self, name: S, func: F) -> &mut Self
    where
        S: Into<String>,
        F: Fn(f64, f64, f64) -> f64 + 'a,
    {
        self.funcs.insert(
            name.into(),
            Rc::new(move |args: &[f64]| {
                if args.len() == 3 {
                    Ok(func(args[0], args[1], args[2]))
                } else {
                    Err(FuncEvalError::NumberArgs(3))
                }
            }),
        );
        self
    }

    /// Adds a new function of a variable number of arguments.
    ///
    /// `n_args` specifies the allowed number of variables by giving an exact number `n` or a range
    /// `n..m`, `..`, `n..`, `..m`. The range is half-open, exclusive on the right, as is common in
    /// Rust standard library.
    ///
    /// # Example
    ///
    /// let mut ctx = Context::empty();
    ///
    /// // require exactly 2 arguments
    /// ctx.funcn("sum_two", |xs| xs[0] + xs[1], 2);
    ///
    /// // allow an arbitrary number of arguments
    /// ctx.funcn("sum", |xs| xs.iter().sum(), ..);
    /// ```
    pub fn funcn<S, F, N>(&mut self, name: S, func: F, n_args: N) -> &mut Self
    where
        S: Into<String>,
        F: Fn(&[f64]) -> f64 + 'a,
        N: ArgGuard,
    {
        self.funcs.insert(name.into(), n_args.to_arg_guard(func));
        self
    }
}

impl<'a> Default for Context<'a> {
    fn default() -> Self {
        Context::new()
    }
}

type GuardedFunc<'a> = Rc<dyn Fn(&[f64]) -> Result<f64, FuncEvalError> + 'a>;

/// Trait for types that can specify the number of required arguments for a function with a
/// variable number of arguments.
///
/// # Example
///
/// let mut ctx = Context::empty();
///
/// // require exactly 2 arguments
/// ctx.funcn("sum_two", |xs| xs[0] + xs[1], 2);
///
/// // allow an arbitrary number of arguments
/// ctx.funcn("sum", |xs| xs.iter().sum(), ..);
/// ```
pub trait ArgGuard {
    fn to_arg_guard<'a, F: Fn(&[f64]) -> f64 + 'a>(self, func: F) -> GuardedFunc<'a>;
}

impl ArgGuard for usize {
    fn to_arg_guard<'a, F: Fn(&[f64]) -> f64 + 'a>(self, func: F) -> GuardedFunc<'a> {
        Rc::new(move |args: &[f64]| {
            if args.len() == self {
                Ok(func(args))
            } else {
                Err(FuncEvalError::NumberArgs(1))
            }
        })
    }
}

impl ArgGuard for std::ops::RangeFrom<usize> {
    fn to_arg_guard<'a, F: Fn(&[f64]) -> f64 + 'a>(self, func: F) -> GuardedFunc<'a> {
        Rc::new(move |args: &[f64]| {
            if args.len() >= self.start {
                Ok(func(args))
            } else {
                Err(FuncEvalError::TooFewArguments)
            }
        })
    }
}

impl ArgGuard for std::ops::RangeTo<usize> {
    fn to_arg_guard<'a, F: Fn(&[f64]) -> f64 + 'a>(self, func: F) -> GuardedFunc<'a> {
        Rc::new(move |args: &[f64]| {
            if args.len() < self.end {
                Ok(func(args))
            } else {
                Err(FuncEvalError::TooManyArguments)
            }
        })
    }
}

impl ArgGuard for std::ops::Range<usize> {
    fn to_arg_guard<'a, F: Fn(&[f64]) -> f64 + 'a>(self, func: F) -> GuardedFunc<'a> {
        Rc::new(move |args: &[f64]| {
            if args.len() >= self.start && args.len() < self.end {
                Ok(func(args))
            } else if args.len() < self.start {
                Err(FuncEvalError::TooFewArguments)
            } else {
                Err(FuncEvalError::TooManyArguments)
            }
        })
    }
}

impl ArgGuard for std::ops::RangeFull {
    fn to_arg_guard<'a, F: Fn(&[f64]) -> f64 + 'a>(self, func: F) -> GuardedFunc<'a> {
        Rc::new(move |args: &[f64]| Ok(func(args)))
    }
}

impl<'a> ContextProvider for Context<'a> {
    fn get_var(&self, name: &str) -> Option<f64> {
        self.vars.get(name).cloned()
    }
    fn eval_func(&self, name: &str, args: &[f64]) -> Result<f64, FuncEvalError> {
        self.funcs
            .get(name)
            .map_or(Err(FuncEvalError::UnknownFunction), |f| f(args))
    }
}
