use std::collections::HashMap;

use reqwest::{Client, StatusCode};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde_json::json;
use base64::{Engine, engine::general_purpose::STANDARD as b64_standard};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::time::{interval, Duration};
use crate::model::aoe_action_result_to_north;
use crate::model::south::{AoeModel, Measurement, PbAoeResults, Transport};
use crate::model::north::{MyPbAoeResult, MyPbActionResult};
use crate::utils::mqttclient::{client_publish, client_publish_sync, generate_aoe_result};
use crate::utils::point_param_map;
use crate::{URL_LOGIN, URL_POINTS, URL_TRANSPORTS,
    URL_AOES, URL_AOE_RESULTS, URL_RESET, APP_NAME};
use crate::env::Env;

const PASSWORD_V_KEY: &[u8] = b"zju-plcc";
const HEADER_TOKEN: &str = "access-token";

type HmacSha256 = Hmac<Sha256>;

pub async fn update_points(points: Vec<Measurement>) -> Result<(), String> {
    let token = login().await?;
    let old_points = query_points(token.clone()).await?;
    let pids = old_points.iter().map(|v| v.point_id).collect::<Vec<u64>>();
    if !pids.is_empty() {
        delete_points(token.clone(), pids).await?;
    }
    save_points(token, points).await
}

pub async fn update_transports(transports: Vec<Transport>) -> Result<(), String> {
    let token = login().await?;
    let old_transports = query_transports(token.clone()).await?;
    let tids = old_transports.iter().map(|v| v.id()).collect::<Vec<u64>>();
    if !tids.is_empty() {
        delete_transports(token.clone(), tids).await?;
    }
    save_transports(token, transports).await
}

pub async fn update_aoes(aoes: Vec<AoeModel>) -> Result<(), String> {
    let token = login().await?;
    let old_aoes = query_aoes(token.clone()).await?;
    let aids = old_aoes.iter().map(|v| v.id).collect::<Vec<u64>>();
    if !aids.is_empty() {
        delete_aoes(token.clone(), aids).await?;
    }
    save_aoes(token, aoes).await
}

pub async fn do_reset() -> Result<(), String> {
    let token = login().await?;
    reset(token).await
}

async fn delete_points(token: String, ids: Vec<u64>) -> Result<(), String> {
    let env = Env::get_env(APP_NAME);
    let plcc_server = env.get_plcc_server();
    let url = format!("{plcc_server}/{URL_POINTS}");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .delete(&url)
        .headers(headers)
        .json(&ids)
        .send().await {
        if response.status() == StatusCode::OK {
            Ok(())
        } else {
            Err("调用测点API删除测点失败".to_string())
        }
    } else {
        Err("连接PLCC失败".to_string())
    }
}

async fn query_points(token: String) -> Result<Vec<Measurement>, String> {
    let env = Env::get_env(APP_NAME);
    let plcc_server = env.get_plcc_server();
    let url = format!("{plcc_server}/{URL_POINTS}");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .get(&url)
        .headers(headers)
        .send().await {
        if let Ok(points) = response.json::<Vec<Measurement>>().await {
            Ok(points)
        } else {
            Err("调用测点API获取测点失败".to_string())
        }
    } else {
        Err("连接PLCC失败".to_string())
    }
}

async fn save_points(token: String, points: Vec<Measurement>) -> Result<(), String> {
    let env = Env::get_env(APP_NAME);
    let plcc_server = env.get_plcc_server();
    let url = format!("{plcc_server}/{URL_POINTS}");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .post(&url)
        .headers(headers)
        .json(&points)
        .send().await {
        if response.status() == StatusCode::OK {
            Ok(())
        } else {
            Err("调用测点API新增测点失败".to_string())
        }
    } else {
        Err("连接PLCC失败".to_string())
    }
}

async fn delete_transports(token: String, ids: Vec<u64>) -> Result<(), String> {
    let env = Env::get_env(APP_NAME);
    let plcc_server = env.get_plcc_server();
    let ids = ids.iter()
        .map(|n| n.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let url = format!("{plcc_server}/{URL_TRANSPORTS}/{ids}");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .delete(&url)
        .headers(headers)
        .json(&ids)
        .send().await {
        if response.status() == StatusCode::OK {
            Ok(())
        } else {
            Err("调用通道API删除通道失败".to_string())
        }
    } else {
        Err("连接PLCC失败".to_string())
    }
}

async fn query_transports(token: String) -> Result<Vec<Transport>, String> {
    let env = Env::get_env(APP_NAME);
    let plcc_server = env.get_plcc_server();
    let url = format!("{plcc_server}/{URL_TRANSPORTS}");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .get(&url)
        .headers(headers)
        .send().await {
        if let Ok(transports) = response.json::<Vec<Transport>>().await {
            Ok(transports)
        } else {
            Err("调用通道API获取通道失败".to_string())
        }
    } else {
        Err("连接PLCC失败".to_string())
    }
}

async fn save_transports(token: String, transports: Vec<Transport>) -> Result<(), String> {
    let env = Env::get_env(APP_NAME);
    let plcc_server = env.get_plcc_server();
    let url = format!("{plcc_server}/{URL_TRANSPORTS}");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .post(&url)
        .headers(headers)
        .json(&transports)
        .send().await {
        if response.status() == StatusCode::OK {
            Ok(())
        } else {
            Err("调用通道API新增通道失败".to_string())
        }
    } else {
        Err("连接PLCC失败".to_string())
    }
}

async fn delete_aoes(token: String, ids: Vec<u64>) -> Result<(), String> {
    let env = Env::get_env(APP_NAME);
    let plcc_server = env.get_plcc_server();
    let ids = ids.iter()
        .map(|n| n.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let url = format!("{plcc_server}/{URL_AOES}/{ids}");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .delete(&url)
        .headers(headers)
        .json(&ids)
        .send().await {
        if response.status() == StatusCode::OK {
            Ok(())
        } else {
            Err("调用策略API删除策略失败".to_string())
        }
    } else {
        Err("连接PLCC失败".to_string())
    }
}

async fn query_aoes(token: String) -> Result<Vec<AoeModel>, String> {
    let env = Env::get_env(APP_NAME);
    let plcc_server = env.get_plcc_server();
    let url = format!("{plcc_server}/{URL_AOES}");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .get(&url)
        .headers(headers)
        .send().await {
        if let Ok(aoes) = response.json::<Vec<AoeModel>>().await {
            Ok(aoes)
        } else {
            Err("调用策略API获取策略失败".to_string())
        }
    } else {
        Err("连接PLCC失败".to_string())
    }
}

async fn save_aoes(token: String, aoes: Vec<AoeModel>) -> Result<(), String> {
    let env = Env::get_env(APP_NAME);
    let plcc_server = env.get_plcc_server();
    let url = format!("{plcc_server}/{URL_AOES}");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .post(&url)
        .headers(headers)
        .json(&aoes)
        .send().await {
        if response.status() == StatusCode::OK {
            Ok(())
        } else {
            Err("调用策略API新增策略失败".to_string())
        }
    } else {
        Err("连接PLCC失败".to_string())
    }
}

async fn login() -> Result<String, String> {
    let env = Env::get_env(APP_NAME);
    let plcc_user = env.get_plcc_user();
    let plcc_pwd = env.get_plcc_pwd();
    let plcc_server = env.get_plcc_server();
    let login_url = format!("{plcc_server}/{URL_LOGIN}");
    let body = json!((plcc_user, password_v_encode(plcc_pwd)));
    let client = Client::new();
    if let Ok(response) = client
        .post(&login_url)
        .json(&body)
        .send().await {
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
    } else {
        Err("连接PLCC失败".to_string())
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

async fn reset(token: String) -> Result<(), String> {
    let env = Env::get_env(APP_NAME);
    let plcc_server = env.get_plcc_server();
    let url = format!("{plcc_server}/{URL_RESET}");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .post(&url)
        .headers(headers)
        .send().await {
        if response.status() == StatusCode::OK {
            Ok(())
        } else {
            Err("调用重置API失败".to_string())
        }
    } else {
        Err("连接PLCC失败".to_string())
    }
}

pub async fn aoe_result_upload() -> Result<(), String> {
    tokio::spawn(async {
        if let Err(e) = aoe_upload_loop().await {
            log::error!("策略结果定时上传任务失败：{}", e);
        }
    });
    Ok(())
}

async fn aoe_upload_loop() -> Result<(), String> {
    let env = Env::get_env(APP_NAME);
    let mqtt_server = env.get_mqtt_server();
    let mqtt_server_port = env.get_mqtt_server_port();

    let mut ticker = interval(Duration::from_secs(5));
    let mut count = 0;
    let mut token = login().await?;
    let mut last_time: HashMap<u64, u64> = HashMap::new();

    let mut mqttoptions = rumqttc::MqttOptions::new("plcc_aoe_result", &mqtt_server, mqtt_server_port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    // mqttoptions.set_credentials("username", "password");
    let topic_request_upload = format!("/sys.brd/{APP_NAME}/S-dataservice/F-UpdateSOE");
    let (client, _) = rumqttc::Client::new(mqttoptions, 10);
    loop {
        if count >= 86400 {
            token = login().await?;
            count = 0;
        }
        count += 1;
        ticker.tick().await;
        let aoe_results = query_aoe_result(token.clone()).await?;
        let points_mapping = point_param_map::get_all();
        let my_aoe_result = aoe_results.results.iter()
            .filter(|a| {
                let aoe_id = a.aoe_id.unwrap();
                let end_time = a.end_time.unwrap();
                if let Some(v) = last_time.get(&aoe_id) {
                    *v != end_time
                } else {
                    last_time.insert(aoe_id, end_time);
                    true
                }
            })
            .map(|a|{
                let action_results = a.action_results.iter().filter_map(|action_result|{
                    aoe_action_result_to_north(action_result.clone(), &points_mapping).ok()
                }).collect::<Vec<MyPbActionResult>>();
                MyPbAoeResult {
                    aoe_id: a.aoe_id,
                    start_time: a.start_time,
                    end_time: a.end_time,
                    event_results: a.event_results.clone(),
                    action_results,
                }
            }).collect::<Vec<MyPbAoeResult>>();
        if !my_aoe_result.is_empty() {
            let body = generate_aoe_result(my_aoe_result);
            let payload = serde_json::to_string(&body).unwrap();
            client_publish_sync(&client, &topic_request_upload, &payload).await?;
        }

    }
}

async fn query_aoe_result(token: String) -> Result<PbAoeResults, String> {
    let env = Env::get_env(APP_NAME);
    let plcc_server = env.get_plcc_server();
    let url = format!("{plcc_server}/{URL_AOE_RESULTS}?last_only=true");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .get(&url)
        .headers(headers)
        .send().await {
        if let Ok(aoe_results) = response.json::<PbAoeResults>().await {
            Ok(aoe_results)
        } else {
            Err("调用策略执行结果API获取测点失败".to_string())
        }
    } else {
        Err("连接PLCC失败".to_string())
    }
}
