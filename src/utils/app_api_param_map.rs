use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::RwLock;

use crate::model::north::AppApiParam;

static APP_API_PARAM_MAP: Lazy<RwLock<HashMap<u64, AppApiParam>>> = Lazy::new(|| {
    RwLock::new(HashMap::new())
});

pub fn insert(key: u64, value: AppApiParam) {
    let mut map = APP_API_PARAM_MAP.write().unwrap();
    map.insert(key, value);
}

pub fn get(key: &u64) -> Option<AppApiParam> {
    let map = APP_API_PARAM_MAP.read().unwrap();
    map.get(key).cloned()
}

pub fn remove(key: &u64) {
    let mut map = APP_API_PARAM_MAP.write().unwrap();
    map.remove(key);
}

pub fn get_all() -> HashMap<u64, AppApiParam> {
    let map = APP_API_PARAM_MAP.read().unwrap();
    map.clone()
}

pub fn save_all(new: Vec<AppApiParam>) {
    let mut map = APP_API_PARAM_MAP.write().unwrap();
    for item in new {
        map.insert(item.point_id, item);
    }
}