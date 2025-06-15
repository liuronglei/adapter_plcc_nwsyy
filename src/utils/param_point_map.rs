use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::RwLock;

static PARAM_MAP: Lazy<RwLock<HashMap<String, u64>>> = Lazy::new(|| {
    RwLock::new(HashMap::new())
});

pub fn insert(key: String, value: u64) {
    let mut map = PARAM_MAP.write().unwrap();
    map.insert(key, value);
}

pub fn get(key: &str) -> Option<u64> {
    let map = PARAM_MAP.read().unwrap();
    map.get(key).cloned()
}

pub fn remove(key: &str) {
    let mut map = PARAM_MAP.write().unwrap();
    map.remove(key);
}

pub fn get_all() -> HashMap<String, u64> {
    let map = PARAM_MAP.read().unwrap();
    map.clone()
}

pub fn save_all(new: HashMap<String, u64>) {
    let mut map = PARAM_MAP.write().unwrap();
    for (k, v) in new {
        map.insert(k, v);
    }
}

pub fn save_reversal(new: HashMap<u64, String>) {
    let mut map = PARAM_MAP.write().unwrap();
    for (k, v) in new {
        map.insert(v, k);
    }
}