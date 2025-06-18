use std::collections::HashMap;
pub mod tokenizer;
pub mod shuntingyard;
pub mod plccapi;
pub mod exprparser;
pub mod expr;
pub mod context;
pub mod mqttclient;
pub mod point_param_map;
pub mod param_point_map;
pub mod log_init;

use regex::Regex;

pub fn get_north_tag(north_point: &str) -> Option<String> {
    let re = Regex::new(r"\$\{([^}]+)\}").unwrap();
    if let Some(captures) = re.captures(north_point) {
        let var_name = &captures[1];
        if let Some(tag) = var_name.split(".").last() {
            return Some(tag.to_string());
        }
    }
    None
}

pub fn replace_point(input: &str, points_mapping: &HashMap<String, u64>) -> Result<String, String> {
    let re = Regex::new(r"\$\{([^}]+)\}").unwrap();
    let (mut is_success, mut err_str) = (true, "".to_string());
    let point = re.replace_all(input, |caps: &regex::Captures| {
        let key = &caps[1];
        if let Some(value) = points_mapping.get(&format!("${{{key}}}")) {
            format!("${value}")
        } else {
            // 保留原样
            is_success = false;
            err_str = caps[0].to_string();
            caps[0].to_string()
        }
    }).into_owned();
    if is_success {
        Ok(point)
    } else {
        Err(format!("测点替换失败：找不到{err_str}对应的测点"))
    }
}

pub fn get_point_tag(input: &u64, points_mapping: &HashMap<u64, String>) -> Result<String, String> {
    if let Some(tag) = points_mapping.get(input) {
        Ok(tag.to_string())
    } else {
        Err(format!("测点替换失败：找不到测点{input}对应的物模型路径"))
    }
}
