use once_cell::sync::Lazy;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

static LAST_RESET_TIME: Lazy<RwLock<u64>> = Lazy::new(|| {
    RwLock::new(0)
});

pub fn update() {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let mut current = LAST_RESET_TIME.write().unwrap();
    *current = ts;
}

pub fn get() -> u64 {
    let current = LAST_RESET_TIME.read().unwrap();
    *current
}

pub fn time_dff() -> u64 {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    ts - get()
}