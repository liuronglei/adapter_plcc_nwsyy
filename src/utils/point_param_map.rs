use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::RwLock;

static POINT_MAP: Lazy<RwLock<HashMap<u64, String>>> = Lazy::new(|| {
    RwLock::new(HashMap::new())
});

/// 插入或更新键值
pub fn insert(key: u64, value: String) {
    let mut map = POINT_MAP.write().unwrap();
    map.insert(key, value);
}

/// 获取某个键的值
pub fn get(key: &u64) -> Option<String> {
    let map = POINT_MAP.read().unwrap();
    map.get(key).cloned()
}

/// 删除某个键
pub fn remove(key: &u64) {
    let mut map = POINT_MAP.write().unwrap();
    map.remove(key);
}

/// 获取整个 HashMap 的只读副本
pub fn get_all() -> HashMap<u64, String> {
    let map = POINT_MAP.read().unwrap();
    map.clone()
}

pub fn save_all(new: HashMap<u64, String>) {
    let mut map = POINT_MAP.write().unwrap();
    for (k, v) in new {
        map.insert(k, v);
    }
}

pub fn save_reversal(new: HashMap<String, u64>) {
    let mut map = POINT_MAP.write().unwrap();
    for (k, v) in new {
        map.insert(v, k);
    }
}