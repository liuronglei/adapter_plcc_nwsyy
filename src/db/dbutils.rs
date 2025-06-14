#![allow(warnings, unused)]

use std::convert::TryInto;
use std::{collections::HashMap, io::Read};

use byteorder::{BigEndian, ByteOrder};
use log::info;
use rocksdb::{AsColumnFamilyRef, IteratorMode, WriteBatch, DB};

use crate::*;

// 尝试统一函数，
pub fn query_values_cbor_with_tree_name<S>(inner_db: &DB, tree_name: &str) -> Vec<S>
where
    S: Clone + serde::de::DeserializeOwned,
{
    let some_tree = inner_db.cf_handle(tree_name);
    if some_tree.is_none() {
        return vec![];
    }
    let tree = some_tree.unwrap();
    let mut result = Vec::new();
    let mode = IteratorMode::Start;
    let iter = inner_db.iterator_cf(&tree, mode);
    for item in iter {
        if let Ok((_, value)) = item {
            if let Ok(m) = serde_cbor::from_slice::<S>(value.as_ref()) {
                result.push(m);
            }
        }
    }
    result.shrink_to_fit();
    result
}

pub fn query_values_cbor<S>(inner_db: &DB) -> Vec<S>
where
    S: Clone + serde::de::DeserializeOwned,
{
    let mut result = Vec::new();
    let iter = inner_db.iterator(IteratorMode::Start);
    for item in iter {
        if let Ok((_, value)) = item {
            if let Ok(m) = serde_cbor::from_slice::<S>(value.as_ref()) {
                result.push(m);
            }
        }
    }
    result.shrink_to_fit();
    result
}

pub fn query_values_cbor_by_key<S>(inner_db: &DB, key : &[u8]) -> Option<S>
    where
        S: Clone + serde::de::DeserializeOwned,
{
    if let Ok(Some(m)) = inner_db.get(key) {
        if let Ok(r) = serde_cbor::from_slice::<S>(m.as_ref()) {
            return Some(r);
        }
    }
    None
}

pub fn query_kv_cbor<S>(inner_db: &DB) -> (Vec<Vec<u8>>, Vec<S>)
    where
        S: Clone + serde::de::DeserializeOwned,
{
    let mut keys = Vec::new();
    let mut values = Vec::new();
    let mode = IteratorMode::Start;
    let iter = inner_db.iterator(mode);
    for item in iter {
        if let Ok((key, value)) = item {
            if let Ok(m) = serde_cbor::from_slice::<S>(value.as_ref()) {
                keys.push(key.to_vec());
                values.push(m);
            }
        }
    }
    keys.shrink_to_fit();
    values.shrink_to_fit();
    (keys, values)
}

pub fn query_kv_cbor_with_tree_name<S>(inner_db: &DB, tree_name : &str) -> (Vec<Vec<u8>>, Vec<S>)
    where
        S: Clone + serde::de::DeserializeOwned,
{
    let some_tree = inner_db.cf_handle(tree_name);
    if some_tree.is_none() {
        return (vec![], vec![]);
    }
    let tree = some_tree.unwrap();
    let mut keys = Vec::new();
    let mut values = Vec::new();
    let mode = IteratorMode::Start;
    let iter = inner_db.iterator_cf(&tree, mode);
    for item in iter {
        if let Ok((key, value)) = item {
            if let Ok(m) = serde_cbor::from_slice::<S>(value.as_ref()) {
                keys.push(key.to_vec());
                values.push(m);
            }
        }
    }
    keys.shrink_to_fit();
    values.shrink_to_fit();
    (keys, values)
}

pub fn query_kv_with_tree_name(inner_db: &DB, tree_name : &str) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let some_tree = inner_db.cf_handle(tree_name);
    if some_tree.is_none() {
        return (vec![], vec![]);
    }
    let tree = some_tree.unwrap();
    let mut keys = Vec::new();
    let mut values = Vec::new();
    let mode = IteratorMode::Start;
    let iter = inner_db.iterator_cf(&tree, mode);
    for item in iter {
        if let Ok((key, value)) = item {
            keys.push(key.to_vec());
            values.push(value.to_vec());
        }
    }
    keys.shrink_to_fit();
    values.shrink_to_fit();
    (keys, values)
}

pub fn query_keys_with_tree_name<F, T>(
    inner_db: &DB,
    tree_name: &str,
    key_calculate_inv: F,
) -> Vec<T>
where
    F: Fn(Vec<u8>) -> T,
{
    let some_tree = inner_db.cf_handle(tree_name);
    if some_tree.is_none() {
        return vec![];
    }
    let tree = some_tree.unwrap();
    let mut result = Vec::new();
    let mode = IteratorMode::Start;
    let iter = inner_db.iterator_cf(&tree, mode);
    for item in iter {
        if let Ok((key, _)) = item {
            let m = key_calculate_inv(key.to_vec());
            result.push(m);
        }
    }
    result.shrink_to_fit();
    result
}

pub fn query_keys_with_tree_name_as_string(inner_db: &DB, tree_name: &str) -> Vec<String> {
    query_keys_with_tree_name(inner_db, tree_name, |key| {
        String::from_utf8(key).unwrap_or_default()
    })
}

// 返回u64类型的key列表
// todo: lrl 这里需要优化，与query_keys_with_tree_name合并，支持泛型或者支持解析器传入
pub fn query_keys_with_tree_name_as_u64(inner_db: &DB, tree_name: &str) -> Vec<u64> {
    // query_keys_with_tree_name(inner_db, tree_name, |key| {
    //     u64::from_be_bytes(key.try_into().expect("slice with incorrect length"))
    // })
    query_keys_with_tree_name(inner_db, tree_name, |key| {
        BigEndian::read_u64(key.as_slice())
    })
}

pub fn query_end_key_with_tree_name<F, T>(
    inner_db: &DB,
    tree_name: &str,
    key_calculate_inv: F,
) -> Option<T>
where
    F: Fn(Vec<u8>) -> T,
{
    let some_tree = inner_db.cf_handle(tree_name);
    if some_tree.is_none() {
        return None;
    }
    let tree = some_tree.unwrap();
    let mode = IteratorMode::End;
    let mut iter = inner_db.iterator_cf(&tree, mode);
    if let Some(item) = iter.next() {
        if let Ok((key, _)) = item {
            let m = key_calculate_inv(key.to_vec());
            return Some(m);
        }
    }
    None
}

pub fn query_end_key_with_tree_name_as_string(inner_db: &DB, tree_name: &str) -> Option<String> {
    query_end_key_with_tree_name(inner_db, tree_name, |key| {
        String::from_utf8(key).unwrap_or_default()
    })
}

pub fn query_end_key_with_tree_name_as_u64(inner_db: &DB, tree_name: &str) -> Option<u64> {
    query_end_key_with_tree_name(inner_db, tree_name, |key| {
        BigEndian::read_u64(key.as_slice())
    })
}

pub fn query_hashes_cbor_with_tree_name<S>(inner_db: &DB, tree_name: &str) -> HashMap<String, S>
where
    S: Clone + serde::de::DeserializeOwned,
{
    let some_tree = inner_db.cf_handle(tree_name);
    if some_tree.is_none() {
        return HashMap::new();
    }
    let tree = some_tree.unwrap();
    let mut result = HashMap::new();
    let mode = IteratorMode::Start;
    let iter = inner_db.iterator_cf(&tree, mode);
    for item in iter {
        if let Ok((key, value)) = item {
            if let Ok(k) = serde_cbor::from_slice::<String>(key.as_ref()) {
                if let Ok(v) = serde_cbor::from_slice::<S>(value.as_ref()) {
                    result.insert(k, v);
                }
            }
        }
    }

    result.shrink_to_fit();
    result
}

pub fn query_value_cbor_by_key_with_tree_name<S, K>(
    inner_db: &DB,
    tree_name: &str,
    key: K,
) -> Option<S>
where
    S: Clone + serde::de::DeserializeOwned,
    K: AsRef<[u8]>,
{
    let some_tree = inner_db.cf_handle(tree_name);
    if some_tree.is_none() {
        return None;
    }
    let tree = some_tree.unwrap();
    if let Ok(Some(m)) = inner_db.get_cf(&tree, key) {
        if let Ok(r) = serde_cbor::from_slice::<S>(m.as_ref()) {
            return Some(r);
        }
    }
    None
}

pub fn query_value_by_key_with_tree_name<K>(
    inner_db: &DB,
    tree_name: &str,
    key: K,
) -> Option<Vec<u8>>
where
    K: AsRef<[u8]>,
{
    let some_tree = inner_db.cf_handle(tree_name);
    if some_tree.is_none() {
        return None;
    }
    let tree = some_tree.unwrap();
    inner_db.get_cf(&tree, key).ok()?
}

pub fn query_value_cbor_by_key<S, K>(inner_db: &DB, key: K) -> Option<S>
where
    S: Clone + serde::de::DeserializeOwned,
    K: AsRef<[u8]>,
{
    if let Ok(Some(m)) = inner_db.get(key) {
        if let Ok(r) = serde_cbor::from_slice::<S>(m.as_ref()) {
            return Some(r);
        }
    }
    None
}

pub fn query_values_cbor_by_keys_with_tree_name<S>(
    inner_db: &DB,
    tree_name: &str,
    keys: Vec<Vec<u8>>,
) -> Vec<S>
where
    S: serde::de::DeserializeOwned,
{
    let mut results = Vec::with_capacity(keys.len());

    let some_tree = inner_db.cf_handle(tree_name);
    if some_tree.is_none() {
        return vec![];
    }
    let tree = some_tree.unwrap();
    for key in keys {
        if let Ok(Some(m)) = inner_db.get_cf(&tree, key) {
            if let Ok(r) = serde_cbor::from_slice::<S>(m.as_ref()) {
                results.push(r);
            }
        }
    }
    results
}

pub fn query_values_cbor_by_prefix_with_tree_name<S>(
    inner_db: &DB,
    tree_name: &str,
    prefix: Vec<u8>,
) -> Vec<S>
where
    S: serde::de::DeserializeOwned,
{
    let mut results = Vec::new();

    let some_tree = inner_db.cf_handle(tree_name);
    if some_tree.is_none() {
        return vec![];
    }
    let tree = some_tree.unwrap();
    let iter = inner_db.prefix_iterator_cf(&tree, &prefix);
    for item in iter {
        if let Ok((key, value)) = item {
            if !key.starts_with(&prefix) {
                break;
            }
            if let Ok(m) = serde_cbor::from_slice::<S>(value.as_ref()) {
                results.push(m);
            }
        }
    }
    results.shrink_to_fit();
    results
}

pub fn save_item_cbor_to_db_with_tree_name<F, T>(
    inner_db: &DB,
    tree_name: &str,
    v: T,
    key_calculate: F,
) -> bool
where
    F: for<'a> Fn(&'a T) -> Vec<u8>,
    T: serde::Serialize,
{
    let some_tree = inner_db.cf_handle(tree_name);
    if some_tree.is_none() {
        return false;
    }
    let tree = some_tree.unwrap();
    let key = key_calculate(&v);
    if let Ok(value) = serde_cbor::to_vec(&v) {
        inner_db.put_cf(&tree, key.as_slice(), value).is_ok()
    } else {
        return false;
    };
    true
}

pub fn save_item_to_db_with_tree_name(
    inner_db: &DB,
    tree_name: &str,
    key: &[u8],
    value: &[u8],
) -> bool {
    let some_tree = inner_db.cf_handle(tree_name);
    if some_tree.is_none() {
        return false;
    }
    let tree = some_tree.unwrap();
    inner_db.put_cf(&tree, key, value).is_ok()
}

pub fn save_items_to_db_with_tree_name(
    inner_db: &DB,
    tree_name: &str,
    keys: &[Vec<u8>],
    values: &[Vec<u8>],
) -> bool {
    if keys.len() != values.len() {
        return false;
    }
    let some_tree = inner_db.cf_handle(tree_name);
    if some_tree.is_none() {
        return false;
    }
    let tree = some_tree.unwrap();
    let mut batch = WriteBatch::default();
    for i in 0..keys.len() {
        batch.put_cf(&tree, &keys[i], &values[i]);
    }
    inner_db.write(batch).is_ok()
}

pub fn save_item_cbor_with_key_to_db<T>(inner_db: &DB, v: &T, key: Vec<u8>) -> bool
where
    T: serde::Serialize,
{
    if let Ok(value) = serde_cbor::to_vec(v) {
        inner_db.put(key.as_slice(), value).is_ok()
    } else {
        false
    }
}

pub fn save_items_cbor_to_db_with_tree_name<F, T>(
    inner_db: &DB,
    tree_name: &str,
    values: &Vec<T>,
    key_calculate: F,
) -> bool
where
    F: for<'a> Fn(&'a T) -> Vec<u8>,
    T: serde::Serialize,
{
    let some_tree = inner_db.cf_handle(tree_name);
    if some_tree.is_none() {
        return false;
    }
    let tree = some_tree.unwrap();
    let mut batch = WriteBatch::default();
    for v in values {
        let key = key_calculate(v);
        if let Ok(value) = serde_cbor::to_vec(v) {
            batch.put_cf(&tree, key, value);
        } else {
            return false;
        };
    }
    inner_db.write(batch).is_ok()
}

pub fn save_items_cbor_to_db<F, T>(inner_db: &DB, values: Vec<T>, key_calculate: F) -> bool
where
    F: for<'a> Fn(&'a T) -> Vec<u8>,
    T: serde::Serialize,
{
    let mut batch = WriteBatch::default();
    for v in values {
        let key = key_calculate(&v);
        if let Ok(value) = serde_cbor::to_vec(&v) {
            batch.put(key, value);
        } else {
            return false;
        };
    }
    inner_db.write(batch).is_ok()
}

pub fn save_hashes_cbor_to_db_with_tree_name<T>(
    inner_db: &DB,
    tree_name: &str,
    map: HashMap<String, T>,
) -> bool
where
    T: serde::Serialize,
{
    let some_tree = inner_db.cf_handle(tree_name);
    if some_tree.is_none() {
        return false;
    }
    let tree = some_tree.unwrap();
    let mut batch = WriteBatch::default();
    for (k, v) in map {
        if let Ok(key) = serde_cbor::to_vec(&k) {
            if let Ok(value) = serde_cbor::to_vec(&v) {
                batch.put_cf(&tree, key, value);
            } else {
                return false;
            };
        } else {
            return false;
        };
    }
    inner_db.write(batch).is_ok()
}

pub fn delete_item_by_key_with_tree_name(inner_db: &DB, tree_name: &str, key: &[u8]) -> bool {
    let some_tree = inner_db.cf_handle(tree_name);
    if some_tree.is_none() {
        return false;
    }
    let tree = some_tree.unwrap();
    inner_db.delete_cf(&tree, key).is_ok()
}

pub fn delete_items_by_keys_with_tree_name(
    inner_db: &DB,
    tree_name: &str,
    keys: Vec<Vec<u8>>,
) -> bool {
    let some_tree = inner_db.cf_handle(tree_name);
    if some_tree.is_none() {
        return false;
    }
    let tree = some_tree.unwrap();
    let mut batch = WriteBatch::default();
    for key in keys {
        batch.delete_cf(&tree, key.as_slice());
    }
    inner_db.write(batch).is_ok()
}

pub fn delete_items_by_keys(inner_db: &DB, keys: Vec<Vec<u8>>) -> bool {
    let mut batch = WriteBatch::default();
    for key in keys {
        batch.delete(key.as_slice());
    }
    inner_db.write(batch).is_ok()
}

/// 解析URL路径中带逗号,的值，返回数组
pub fn parse_path_values<T: std::str::FromStr>(path: &str, pat: char) -> Vec<T> {
    let values_str: Vec<&str> = path.split(pat).collect();
    let mut vec: Vec<T> = Vec::with_capacity(values_str.len());
    for value_str in values_str {
        if let Ok(v) = value_str.trim().parse() {
            vec.push(v);
        }
    }
    vec
}

#[test]
fn test_parse_path_values() {
    let path = "100001";
    let v: Vec<u64> = parse_path_values(path, ',');
    assert_eq!(1, v.len());
    assert_eq!(100001, v[0]);
    let path = "100001,100002,100003";
    let v: Vec<u64> = parse_path_values(path, ',');
    assert_eq!(3, v.len());
    assert_eq!(100001, v[0]);
    assert_eq!(100002, v[1]);
    assert_eq!(100003, v[2]);
    let s = "100001";
    let v = format!("{:?}", s);
    println!("{v}");
    let mut ids = "\"\"".to_string();
    if ids.starts_with("\"") {
        ids = ids[1..].to_string();
    }
    if ids.ends_with("\"") {
        ids = ids[0..ids.len()-1].to_string();
    }
    println!("result: {ids}");
}


