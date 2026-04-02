use reqwest::{Client, StatusCode};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde_json::json;
use base64::{Engine, engine::general_purpose::STANDARD as b64_standard};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::time::Duration;
use crate::model::south::{Measurement, PointControl, Transport};
use crate::utils::global::PLCC_LAST_RESET_TIME;
use crate::{ADAPTER_NAME, AdapterErr, ErrCode, URL_LOGIN, URL_POINTS,
    URL_PLCC_RESET, URL_TRANSPORTS, URL_POINT_CONTROL};
use crate::env::Env;

const PASSWORD_V_KEY: &[u8] = b"zju-plcc";
const HEADER_TOKEN: &str = "access-token";

type HmacSha256 = Hmac<Sha256>;

pub async fn update_points(points: Vec<Measurement>) -> Result<(), AdapterErr> {
    let token = login().await?;
    let old_points = query_points(token.clone()).await?;
    let pids = old_points.iter().map(|v| v.point_id).collect::<Vec<u64>>();
    if !pids.is_empty() {
        delete_points(token.clone(), pids).await?;
    }
    save_points(token, points).await
}

pub async fn update_transports(transports: Vec<Transport>) -> Result<(), AdapterErr> {
    let token = login().await?;
    let old_transports = query_transports(token.clone()).await?;
    let tids = old_transports.iter().map(|v| v.id()).collect::<Vec<u64>>();
    if !tids.is_empty() {
        delete_transports(token.clone(), tids).await?;
    }
    save_transports(token, transports).await
}

pub async fn do_reset_plcc() -> Result<(), AdapterErr> {
    loop {
        // 8秒内不允许重复reset
        if PLCC_LAST_RESET_TIME.time_diff() < 8000 {
            log::warn!("do reset PLCC within 8 seconds, waiting...");
            actix_rt::time::sleep(Duration::from_millis(1000)).await;
        } else {
            PLCC_LAST_RESET_TIME.update();
            break;
        }
    }
    let token = login().await?;
    reset(token).await
}

async fn delete_points(token: String, ids: Vec<u64>) -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
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
            Err(AdapterErr {
                code: ErrCode::PlccConnectErr,
                msg: "调用测点API删除测点失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::PlccConnectErr,
            msg: "failed to connect PLCC".to_string(),
        })
    }
}

async fn query_points(token: String) -> Result<Vec<Measurement>, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
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
            Err(AdapterErr {
                code: ErrCode::PlccConnectErr,
                msg: "调用测点API获取测点失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::PlccConnectErr,
            msg: "failed to connect PLCC".to_string(),
        })
    }
}

async fn save_points(token: String, points: Vec<Measurement>) -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
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
            Err(AdapterErr {
                code: ErrCode::PlccConnectErr,
                msg: "调用测点API新增测点失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::PlccConnectErr,
            msg: "failed to connect PLCC".to_string(),
        })
    }
}

async fn delete_transports(token: String, ids: Vec<u64>) -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
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
            Err(AdapterErr {
                code: ErrCode::PlccConnectErr,
                msg: "调用通道API删除通道失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::PlccConnectErr,
            msg: "failed to connect PLCC".to_string(),
        })
    }
}

async fn query_transports(token: String) -> Result<Vec<Transport>, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
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
            Err(AdapterErr {
                code: ErrCode::PlccConnectErr,
                msg: "调用通道API获取通道失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::PlccConnectErr,
            msg: "failed to connect PLCC".to_string(),
        })
    }
}

async fn save_transports(token: String, transports: Vec<Transport>) -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
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
            Err(AdapterErr {
                code: ErrCode::PlccConnectErr,
                msg: "调用通道API新增通道失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::PlccConnectErr,
            msg: "failed to connect PLCC".to_string(),
        })
    }
}

async fn login() -> Result<String, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
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
            Err(AdapterErr {
                code: ErrCode::PlccConnectErr,
                msg: error,
            })
        } else {
            Ok(token)
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::PlccConnectErr,
            msg: "PLCC登录失败".to_string(),
        })
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

async fn reset(token: String) -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let plcc_server = env.get_plcc_server();
    let url = format!("{plcc_server}/{URL_PLCC_RESET}");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .post(&url)
        .headers(headers)
        .send().await {
        if response.status() == StatusCode::OK {
            Ok(())
        } else {
            Err(AdapterErr {
                code: ErrCode::PlccConnectErr,
                msg: "调用重置API失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::PlccConnectErr,
            msg: "failed to connect PLCC".to_string(),
        })
    }
}

pub async fn do_point_action(point_control: PointControl) -> Result<(), AdapterErr> {
    let token = login().await?;
    point_action(token, point_control).await
}

async fn point_action(token: String, point_control: PointControl) -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let plcc_server = env.get_plcc_server();
    let url = format!("{plcc_server}/{URL_POINT_CONTROL}");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .post(&url)
        .headers(headers)
        .json(&point_control)
        .send().await {
        if response.status() == StatusCode::OK {
            Ok(())
        } else {
            Err(AdapterErr {
                code: ErrCode::PlccConnectErr,
                msg: "调用测点指令API失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::PlccConnectErr,
            msg: "failed to connect PLCC".to_string(),
        })
    }
}
