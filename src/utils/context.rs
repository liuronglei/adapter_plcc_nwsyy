use ndarray::{Array, Array2, IxDyn};
use num_complex::{Complex, Complex64};
use crate::model::south::{ContextProvider, FuncEvalError, MyCx, MyF};

impl<'a, T: ContextProvider> ContextProvider for &'a T {
    fn get_var(&self, name: &str) -> Option<f64> {
        (**self).get_var(name)
    }
    fn get_var_cx(&self, name: &str) -> Option<Complex64> {
        (**self).get_var_cx(name)
    }
    fn get_tensor(&self, name: &str) -> Option<Array<f64, IxDyn>> {
        (**self).get_tensor(name)
    }
    fn get_tensor_cx(&self, name: &str) -> Option<Array<Complex64, IxDyn>> {
        (**self).get_tensor_cx(name)
    }
    fn eval_func(&self, name: &str, args: &[f64]) -> Result<f64, FuncEvalError> {
        (**self).eval_func(name, args)
    }
    fn eval_func_cx(&self, name: &str, args: &[Complex64]) -> Result<Complex64, FuncEvalError> {
        (**self).eval_func_cx(name, args)
    }
    fn eval_func_tensor(&self, name: &str, args: &[MyF]) -> Result<MyF, FuncEvalError> {
        (**self).eval_func_tensor(name, args)
    }
    fn eval_func_tensor_cx(&self, name: &str, args: &[MyCx]) -> Result<MyCx, FuncEvalError> {
        (**self).eval_func_tensor_cx(name, args)
    }
    fn matrix_inv(&self, arg: &Array2<f64>) -> Result<Array2<f64>, FuncEvalError> {
        (**self).matrix_inv(arg)
    }
    fn matrix_inv_cx(&self, arg: &Array2<Complex64>) -> Result<Array2<Complex64>, FuncEvalError> {
        (**self).matrix_inv_cx(arg)
    }
}

impl<'a, T: ContextProvider> ContextProvider for &'a mut T {
    fn get_var(&self, name: &str) -> Option<f64> {
        (**self).get_var(name)
    }
    fn get_var_cx(&self, name: &str) -> Option<Complex64> {
        (**self).get_var_cx(name)
    }
    fn get_tensor(&self, name: &str) -> Option<Array<f64, IxDyn>> {
        (**self).get_tensor(name)
    }
    fn get_tensor_cx(&self, name: &str) -> Option<Array<Complex64, IxDyn>> {
        (**self).get_tensor_cx(name)
    }
    fn eval_func(&self, name: &str, args: &[f64]) -> Result<f64, FuncEvalError> {
        (**self).eval_func(name, args)
    }
    fn eval_func_cx(&self, name: &str, args: &[Complex64]) -> Result<Complex64, FuncEvalError> {
        (**self).eval_func_cx(name, args)
    }
    fn eval_func_tensor(&self, name: &str, args: &[MyF]) -> Result<MyF, FuncEvalError> {
        (**self).eval_func_tensor(name, args)
    }
    fn eval_func_tensor_cx(&self, name: &str, args: &[MyCx]) -> Result<MyCx, FuncEvalError> {
        (**self).eval_func_tensor_cx(name, args)
    }
    fn matrix_inv(&self, arg: &Array2<f64>) -> Result<Array2<f64>, FuncEvalError> {
        (**self).matrix_inv(arg)
    }
    fn matrix_inv_cx(&self, arg: &Array2<Complex64>) -> Result<Array2<Complex64>, FuncEvalError> {
        (**self).matrix_inv_cx(arg)
    }
}

impl<T: ContextProvider, S: ContextProvider> ContextProvider for (T, S) {
    fn get_var(&self, name: &str) -> Option<f64> {
        self.0.get_var(name).or_else(|| self.1.get_var(name))
    }
    fn get_var_cx(&self, name: &str) -> Option<Complex64> {
        self.0.get_var_cx(name).or_else(|| self.1.get_var_cx(name))
    }
    fn get_tensor(&self, name: &str) -> Option<Array<f64, IxDyn>> {
        self.0.get_tensor(name).or_else(|| self.1.get_tensor(name))
    }
    fn get_tensor_cx(&self, name: &str) -> Option<Array<Complex64, IxDyn>> {
        self.0
            .get_tensor_cx(name)
            .or_else(|| self.1.get_tensor_cx(name))
    }
    fn eval_func(&self, name: &str, args: &[f64]) -> Result<f64, FuncEvalError> {
        match self.0.eval_func(name, args) {
            Err(FuncEvalError::UnknownFunction) => self.1.eval_func(name, args),
            e => e,
        }
    }
    fn eval_func_cx(&self, name: &str, args: &[Complex64]) -> Result<Complex64, FuncEvalError> {
        match self.0.eval_func_cx(name, args) {
            Err(FuncEvalError::UnknownFunction) => self.1.eval_func_cx(name, args),
            e => e,
        }
    }
    fn eval_func_tensor(&self, name: &str, args: &[MyF]) -> Result<MyF, FuncEvalError> {
        match self.0.eval_func_tensor(name, args) {
            Err(FuncEvalError::UnknownFunction) => self.1.eval_func_tensor(name, args),
            e => e,
        }
    }
    fn eval_func_tensor_cx(&self, name: &str, args: &[MyCx]) -> Result<MyCx, FuncEvalError> {
        match self.0.eval_func_tensor_cx(name, args) {
            Err(FuncEvalError::UnknownFunction) => self.1.eval_func_tensor_cx(name, args),
            e => e,
        }
    }
    fn matrix_inv(&self, v: &Array2<f64>) -> Result<Array2<f64>, FuncEvalError> {
        match self.0.matrix_inv(v) {
            Err(FuncEvalError::UnknownFunction) => self.1.matrix_inv(v),
            e => e,
        }
    }
    fn matrix_inv_cx(&self, v: &Array2<Complex64>) -> Result<Array2<Complex64>, FuncEvalError> {
        match self.0.matrix_inv_cx(v) {
            Err(FuncEvalError::UnknownFunction) => self.1.matrix_inv_cx(v),
            e => e,
        }
    }
}

impl<S: AsRef<str>> ContextProvider for (S, f64) {
    fn get_var(&self, name: &str) -> Option<f64> {
        if self.0.as_ref() == name {
            Some(self.1)
        } else {
            None
        }
    }
}

/// `std::collections::HashMap` of variables.
impl<S> ContextProvider for std::collections::HashMap<S, f64>
where
    S: std::hash::Hash + Eq + std::borrow::Borrow<str>,
{
    fn get_var(&self, name: &str) -> Option<f64> {
        self.get(name).cloned()
    }

    fn get_var_cx(&self, name: &str) -> Option<Complex64> {
        self.get(name).map(|f| Complex::new(*f, 0.))
    }
}

/// `std::collections::HashMap` of variables.
impl<S> ContextProvider for std::collections::HashMap<S, Complex64>
where
    S: std::hash::Hash + Eq + std::borrow::Borrow<str>,
{
    fn get_var_cx(&self, name: &str) -> Option<Complex64> {
        self.get(name).cloned()
    }
}

impl<S> ContextProvider for std::collections::HashMap<S, MyF>
where
    S: std::hash::Hash + Eq + std::borrow::Borrow<str>,
{
    fn get_var(&self, name: &str) -> Option<f64> {
        if let Some(MyF::F64(f)) = self.get(name) {
            Some(*f)
        } else {
            None
        }
    }

    fn get_var_cx(&self, name: &str) -> Option<Complex64> {
        if let Some(MyF::F64(f)) = self.get(name) {
            Some(Complex64::new(*f, 0.))
        } else {
            None
        }
    }

    fn get_tensor(&self, name: &str) -> Option<Array<f64, IxDyn>> {
        if let Some(MyF::Tensor(v)) = self.get(name) {
            Some(v.clone())
        } else {
            None
        }
    }
}

impl<S> ContextProvider for std::collections::HashMap<S, MyCx>
where
    S: std::hash::Hash + Eq + std::borrow::Borrow<str>,
{
    fn get_var_cx(&self, name: &str) -> Option<Complex64> {
        if let Some(MyCx::F64(f)) = self.get(name) {
            Some(*f)
        } else {
            None
        }
    }

    fn get_tensor_cx(&self, name: &str) -> Option<Array<Complex64, IxDyn>> {
        if let Some(MyCx::Tensor(v)) = self.get(name) {
            Some(v.clone())
        } else {
            None
        }
    }
}

/// `std::collections::HashMap` of variables.
impl<S> ContextProvider for std::collections::HashMap<S, Array<f64, IxDyn>>
where
    S: std::hash::Hash + Eq + std::borrow::Borrow<str>,
{
    fn get_tensor(&self, name: &str) -> Option<Array<f64, IxDyn>> {
        self.get(name).cloned()
    }
}

/// `std::collections::BTreeMap` of variables.
impl<S> ContextProvider for std::collections::BTreeMap<S, f64>
where
    S: Ord + std::borrow::Borrow<str>,
{
    fn get_var(&self, name: &str) -> Option<f64> {
        self.get(name).cloned()
    }

    fn get_var_cx(&self, name: &str) -> Option<Complex64> {
        self.get(name).map(|f| Complex::new(*f, 0.))
    }
}

impl<S> ContextProvider for std::collections::BTreeMap<S, Complex64>
where
    S: Ord + std::borrow::Borrow<str>,
{
    fn get_var_cx(&self, name: &str) -> Option<Complex64> {
        self.get(name).cloned()
    }
}

impl<S> ContextProvider for std::collections::BTreeMap<S, Array<f64, IxDyn>>
where
    S: Ord + std::borrow::Borrow<str>,
{
    fn get_tensor(&self, name: &str) -> Option<Array<f64, IxDyn>> {
        self.get(name).cloned()
    }
}

impl<S> ContextProvider for std::collections::BTreeMap<S, MyF>
where
    S: Ord + std::borrow::Borrow<str>,
{
    fn get_var(&self, name: &str) -> Option<f64> {
        if let Some(MyF::F64(f)) = self.get(name) {
            Some(*f)
        } else {
            None
        }
    }

    fn get_var_cx(&self, name: &str) -> Option<Complex64> {
        if let Some(MyF::F64(f)) = self.get(name) {
            Some(Complex64::new(*f, 0.))
        } else {
            None
        }
    }

    fn get_tensor(&self, name: &str) -> Option<Array<f64, IxDyn>> {
        if let Some(MyF::Tensor(v)) = self.get(name) {
            Some(v.clone())
        } else {
            None
        }
    }
}

impl<S> ContextProvider for std::collections::BTreeMap<S, MyCx>
where
    S: Ord + std::borrow::Borrow<str>,
{
    fn get_var_cx(&self, name: &str) -> Option<Complex64> {
        if let Some(MyCx::F64(f)) = self.get(name) {
            Some(*f)
        } else {
            None
        }
    }

    fn get_tensor_cx(&self, name: &str) -> Option<Array<Complex64, IxDyn>> {
        if let Some(MyCx::Tensor(v)) = self.get(name) {
            Some(v.clone())
        } else {
            None
        }
    }
}

impl<S: AsRef<str>> ContextProvider for Vec<(S, f64)> {
    fn get_var(&self, name: &str) -> Option<f64> {
        for &(ref n, v) in self.iter() {
            if n.as_ref() == name {
                return Some(v);
            }
        }
        None
    }
}

// macro for implementing ContextProvider for arrays
macro_rules! array_impls {
    ($($N:expr)+) => {
        $(
            impl<S: AsRef<str>> ContextProvider for [(S, f64); $N] {
                fn get_var(&self, name: &str) -> Option<f64> {
                    for &(ref n, v) in self.iter() {
                        if n.as_ref() == name {
                            return Some(v);
                        }
                    }
                    None
                }
            }
        )+
    }
}

array_impls! {
    0 1 2 3 4 5 6 7 8
}