use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::model::north::AppApiParam;

pub static POINT_PARAM_MAP: Lazy<GlobalMap<u64, String>> = Lazy::new(|| GlobalMap::new());
pub static PARAM_POINT_MAP: Lazy<GlobalMap<String, u64>> = Lazy::new(|| GlobalMap::new());
pub static APP_API_PARAM_MAP: Lazy<GlobalMap<u64, AppApiParam>> = Lazy::new(|| GlobalMap::new());
pub static PLCC_LAST_RESET_TIME: Lazy<GlobalTime> = Lazy::new(|| GlobalTime::new());
pub static MEMS_LAST_RESET_TIME: Lazy<GlobalTime> = Lazy::new(|| GlobalTime::new());

pub struct GlobalMap<K, V> {
    map: RwLock<HashMap<K, V>>,
}

impl<K, V> GlobalMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub fn new() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
        }
    }

    pub fn insert(&self, key: K, value: V) {
        let mut map = self.map.write().unwrap();
        map.insert(key, value);
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let map = self.map.read().unwrap();
        map.get(key).cloned()
    }

    pub fn remove(&self, key: &K) {
        let mut map = self.map.write().unwrap();
        map.remove(key);
    }

    pub fn get_all(&self) -> HashMap<K, V> {
        let map = self.map.read().unwrap();
        map.clone()
    }

    pub fn save_all(&self, new: HashMap<K, V>) {
        let mut map = self.map.write().unwrap();
        *map = new;
    }
}

pub struct GlobalTime {
    value: RwLock<u64>,
}

impl GlobalTime {
    pub fn new() -> Self {
        Self {
            value: RwLock::new(0),
        }
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    /// 更新时间为当前时间
    pub fn update(&self) {
        self.set(Self::now());
    }

    /// 设置指定时间
    pub fn set(&self, ts: u64) {
        let mut v = self.value.write().unwrap();
        *v = ts;
    }

    /// 获取当前值
    pub fn get(&self) -> u64 {
        let v = self.value.read().unwrap();
        *v
    }

    /// 当前时间 - 保存时间
    pub fn time_diff(&self) -> u64 {
        Self::now() - self.get()
    }
}
