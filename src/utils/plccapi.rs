use std::sync::{LazyLock, Mutex};
use reqwest::{Client, StatusCode};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE, AUTHORIZATION};
use serde_json::{json, Value};
use base64::{Engine, engine::general_purpose::STANDARD as b64_standard};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use crate::model::south::{Measurement, Transport};
use crate::{PLCC_HOST, PLCC_USR, PLCC_PWD, URL_LOGIN, URL_POINTS, URL_TRANSPORTS, URL_AOES};

const PASSWORD_V_KEY: &[u8] = b"zju-plcc";
const HEADER_TOKEN: &str = "access-token";

type HmacSha256 = Hmac<Sha256>;

pub async fn update_data(points: Vec<Measurement>) -> Result<(), String> {
    let token = login().await?;
    // 进行测点更新操作
    let old_points = query_points(token.clone()).await?;
    let pids = old_points.iter().map(|v| v.point_id).collect::<Vec<u64>>();
    if !pids.is_empty() {
        delete_points(token.clone(), pids).await?;
    }
    save_points(token, points).await
}

async fn delete_points(token: String, ids: Vec<u64>) -> Result<(), String> {
    let url = format!("{PLCC_HOST}/{URL_POINTS}");
    let headers = get_header(token);
    let client = Client::new();
    let response = client
        .delete(&url)
        .headers(headers)
        .json(&ids)
        .send().await.unwrap();
    if response.status() == StatusCode::OK {
        Ok(())
    } else {
        Err("调用测点API删除测点失败".to_string())
    }
}

async fn query_points(token: String) -> Result<Vec<Measurement>, String> {
    let url = format!("{PLCC_HOST}/{URL_POINTS}");
    let headers = get_header(token);
    let client = Client::new();
    let response = client
        .get(&url)
        .headers(headers)
        .send().await.unwrap();
    if let Ok(points) = response.json::<Vec<Measurement>>().await {
        Ok(points)
    } else {
        Err("调用测点API获取测点失败".to_string())
    }
}

async fn save_points(token: String, points: Vec<Measurement>) -> Result<(), String> {
    let url = format!("{PLCC_HOST}/{URL_POINTS}");
    let headers = get_header(token);
    let client = Client::new();
    let response = client
        .post(&url)
        .headers(headers)
        .json(&points)
        .send().await.unwrap();
    if response.status() == StatusCode::OK {
        Ok(())
    } else {
        Err("调用测点API新增测点失败".to_string())
    }
}

async fn delete_transports(token: String, ids: Vec<u64>) -> Result<(), String> {
    let url = format!("{PLCC_HOST}/{URL_TRANSPORTS}");
    let headers = get_header(token);
    let client = Client::new();
    let response = client
        .delete(&url)
        .headers(headers)
        .json(&ids)
        .send().await.unwrap();
    if response.status() == StatusCode::OK {
        Ok(())
    } else {
        Err("调用通道API删除通道失败".to_string())
    }
}

async fn query_transports(token: String) -> Result<Vec<Transport>, String> {
    let url = format!("{PLCC_HOST}/{URL_TRANSPORTS}");
    let headers = get_header(token);
    let client = Client::new();
    let response = client
        .get(&url)
        .headers(headers)
        .send().await.unwrap();
    if let Ok(transports) = response.json::<Vec<Transport>>().await {
        Ok(transports)
    } else {
        Err("调用通道API获取通道失败".to_string())
    }
}

async fn save_transports(token: String, points: Vec<Transport>) -> Result<(), String> {
    let url = format!("{PLCC_HOST}/{URL_TRANSPORTS}");
    let headers = get_header(token);
    let client = Client::new();
    let response = client
        .post(&url)
        .headers(headers)
        .json(&points)
        .send().await.unwrap();
    if response.status() == StatusCode::OK {
        Ok(())
    } else {
        Err("调用通道API新增通道失败".to_string())
    }
}

async fn login() -> Result<String, String> {
    let login_url = format!("{PLCC_HOST}/{URL_LOGIN}");
    let body = json!((PLCC_USR, password_v_encode(PLCC_PWD.to_string())));
    let client = Client::new();
    let response = client
        .post(&login_url)
        .json(&body)
        .send().await.unwrap();
    let (token, user_id, _user_name) = response.json::<(String, u16, String)>().await.unwrap();
    if user_id == 0 {
        let error = match token.as_str() {
            "password_error" => {
                "密码错误".to_string()
            },
            "login_count_exceeded" => {
                "密码连续错误超过5次，请5分钟后再试".to_string()
            },
            "user_expired" => {
                "此用户账号已过期".to_string()
            },
            _ => {
                "未知错误".to_string()
            }
        };
        Err(error)
    } else {
        Ok(token)
    }
}

// 前端加密
fn password_v_encode(password: String) -> String {
    let mut mac = HmacSha256::new_from_slice(PASSWORD_V_KEY).expect("HMAC can take key of any size");
    mac.update(password.as_bytes());
    //加密算法
    let result = b64_standard.encode(mac.finalize().into_bytes());
    result
}

fn get_header(token: String) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(HEADER_TOKEN, HeaderValue::from_str(&token).unwrap());
    headers
}
