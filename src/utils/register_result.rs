use once_cell::sync::Lazy;
use std::sync::RwLock;

static REGISTER_RESULT: Lazy<RwLock<bool>> = Lazy::new(|| {
    RwLock::new(false)
});

pub fn get_result() -> bool {
    let result = REGISTER_RESULT.read().unwrap();
    *result
}

pub fn set_result(result: bool) {
    let mut flag = REGISTER_RESULT.write().unwrap();
    *flag = result;
}