
use crate::utils::shuntingyard::Associativity::*;
use crate::model::south::{RPNError, Token};

#[derive(Debug, Clone, Copy)]
pub enum Associativity {
    Left,
    Right,
    NA,
}

/// Returns the operator precedence and associativity for a given token.
fn prec_assoc(token: &Token) -> (u32, Associativity) {
    use self::Associativity::*;
    use crate::model::south::Operation::*;
    use crate::model::south::Token::*;
    match *token {
        Binary(op) => match op {
            Or => (3, Left),
            And => (4, Left),
            BitOr => (5, Left),
            BitXor => (6, Left),
            BitAnd => (7, Left),
            Equal | Unequal => (8, Left),
            LessThan | GreatThan | LtOrEqual | GtOrEqual => (9, Left),
            BitShl | BitShr => (10, Left),
            Plus | Minus => (11, Left),
            Times | Div | LeftDiv | Rem | DotTimes | DotDiv => (12, Left),
            BitAt => (13, Left),
            Pow | DotPow  => (14, Right),
            _ => unimplemented!(),
        }
        Unary(op) => match op {
            Plus | Minus | Not | BitNot => (13, NA),
            Fact => (15, NA),
            Transpose =>  (16, NA),
            _ => unimplemented!(),
        }
        Var(_) | Str(_) | Number(_) | Func(..) | Tensor(_) | LParen | RParen | BigLParen | BigRParen
        | RBracket | Comma => (0, NA),
    }
}

/// Converts a tokenized infix expression to reverse Polish notation.
///
/// # Failure
///
/// Returns `Err` if the input expression is not well-formed.
pub fn to_rpn(input: &[Token]) -> Result<Vec<Token>, RPNError> {
    use crate::model::south::Token::*;

    let mut output = Vec::with_capacity(input.len());
    let mut stack = Vec::with_capacity(input.len());

    for (index, token) in input.iter().enumerate() {
        let token = token.clone();
        match token {
            Number(_) | Var(_) => output.push(token),
            Unary(_) => stack.push((index, token)),
            Binary(_) => {
                let pa1 = prec_assoc(&token);
                while !stack.is_empty() {
                    let pa2 = prec_assoc(&stack.last().unwrap().1);
                    match (pa1, pa2) {
                        ((i, Left), (j, _)) if i <= j => {
                            output.push(stack.pop().unwrap().1);
                        }
                        ((i, Right), (j, _)) if i < j => {
                            output.push(stack.pop().unwrap().1);
                        }
                        _ => {
                            break;
                        }
                    }
                }
                stack.push((index, token))
            }
            LParen => stack.push((index, token)),
            RParen => {
                let mut found = false;
                while let Some((_, t)) = stack.pop() {
                    match t {
                        LParen => {
                            found = true;
                            break;
                        }
                        Func(name, nargs) => {
                            found = true;
                            output.push(Func(name, Some(nargs.unwrap_or(0) + 1)));
                            break;
                        }
                        _ => output.push(t),
                    }
                }
                if !found {
                    return Err(RPNError::MismatchedRParen(index));
                }
            }
            RBracket => {
                let mut found = false;
                while let Some((_, t)) = stack.pop() {
                    match t {
                        Tensor(size) => {
                            found = true;
                            output.push(Tensor(Some(size.unwrap_or(0) + 1)));
                            break;
                        }
                        _ => output.push(t),
                    }
                }
                if !found {
                    return Err(RPNError::MismatchedRBracket(index));
                }
            }
            Comma => {
                let mut found = false;
                while let Some((i, t)) = stack.pop() {
                    match t {
                        LParen => {
                            return Err(RPNError::UnexpectedComma(index));
                        }
                        Func(name, nargs) => {
                            found = true;
                            stack.push((i, Func(name, Some(nargs.unwrap_or(0) + 1))));
                            break;
                        }
                        Tensor(size) => {
                            found = true;
                            stack.push((i, Tensor(Some(size.unwrap_or(0) + 1))));
                            break;
                        }
                        _ => output.push(t),
                    }
                }
                if !found {
                    return Err(RPNError::UnexpectedComma(index));
                }
            }
            Tensor(Some(0)) => output.push(token),
            Tensor(..) => stack.push((index, token)),
            Func(_, Some(0)) => output.push(token),
            Func(..) => stack.push((index, token)),
            _ => {}
        }
    }

    while let Some((index, token)) = stack.pop() {
        match token {
            Unary(_) | Binary(_) => output.push(token),
            Func(_, None) => output.push(token),
            Tensor(None) => output.push(token),
            LParen | Func(..) => return Err(RPNError::MismatchedLParen(index)),
            _ => panic!("Unexpected token on stack."),
        }
    }

    // verify rpn
    let mut n_operands = 0isize;
    for (index, token) in output.iter().enumerate() {
        match *token {
            Var(_) | Number(_) => n_operands += 1,
            Unary(_) => (),
            Binary(_) => n_operands -= 1,
            Func(_, None) => continue,
            Func(_, Some(n_args)) => n_operands -= n_args as isize - 1,
            Tensor(None) => continue,
            Tensor(Some(size)) => n_operands -= size as isize - 1,
            _ => panic!("Nothing else should be here"),
        }
        if n_operands <= 0 {
            return Err(RPNError::NotEnoughOperands(index));
        }
    }

    if n_operands > 1 {
        return Err(RPNError::TooManyOperands);
    }

    output.shrink_to_fit();
    Ok(output)
}
