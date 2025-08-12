use std::collections::HashMap;

use rumqttc::{AsyncClient, MqttOptions};
use reqwest::{Client, StatusCode};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde_json::json;
use base64::{Engine, engine::general_purpose::STANDARD as b64_standard};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::time::{interval, Duration};
use crate::model::aoe_action_result_to_north;
use crate::model::datacenter::CloudEventAoeStatus;
use crate::model::south::{AoeModel, Measurement, PbAoeResults, Transport, AoeControl, AoeAction};
use crate::model::north::{MyPbAoeResult, MyPbActionResult};
use crate::utils::mqttclient::{get_mqttoptions, client_publish, generate_aoe_update, generate_aoe_set, query_register_dev};
use crate::utils::point_param_map;
use crate::utils::localapi::query_aoe_mapping;
use crate::{AdapterErr, ErrCode, ADAPTER_NAME, URL_AOES, URL_AOE_RESULTS, URL_LOGIN, URL_POINTS, URL_RESET,
    URL_TRANSPORTS, URL_UNRUN_AOES, URL_RUNNING_AOES, URL_AOE_CONTROL};
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

pub async fn update_aoes(aoes: Vec<AoeModel>) -> Result<(), AdapterErr> {
    let token = login().await?;
    let old_aoes = query_aoes(token.clone()).await?;
    let aids = old_aoes.iter().map(|v| v.id).collect::<Vec<u64>>();
    if !aids.is_empty() {
        delete_aoes(token.clone(), aids).await?;
    }
    save_aoes(token, aoes).await
}

pub async fn do_reset() -> Result<(), AdapterErr> {
    let token = login().await?;
    // 记录重置前的AOE状态
    let aoes_status = do_query_aoe_status().await?;
    reset(token.clone()).await?;
    // 重置后恢复AOE状态
    let my_aoe_action = aoes_status.iter().map(|status| {
        match status.aoe_status {
            0 => {
                AoeAction::StopAoe(status.aoe_id)
            },
            _ => {
                AoeAction::StartAoe(status.aoe_id)
            }
        }
    }).collect::<Vec<AoeAction>>();
    aoe_action(token, AoeControl { AoeActions: my_aoe_action }).await
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
            msg: "连接PLCC失败".to_string(),
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
            msg: "连接PLCC失败".to_string(),
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
            msg: "连接PLCC失败".to_string(),
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
            msg: "连接PLCC失败".to_string(),
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
            msg: "连接PLCC失败".to_string(),
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
            msg: "连接PLCC失败".to_string(),
        })
    }
}

async fn delete_aoes(token: String, ids: Vec<u64>) -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
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
            Err(AdapterErr {
                code: ErrCode::PlccConnectErr,
                msg: "调用策略API删除策略失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::PlccConnectErr,
            msg: "连接PLCC失败".to_string(),
        })
    }
}

async fn query_aoes(token: String) -> Result<Vec<AoeModel>, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
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
            Err(AdapterErr {
                code: ErrCode::PlccConnectErr,
                msg: "调用策略API获取策略失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::PlccConnectErr,
            msg: "连接PLCC失败".to_string(),
        })
    }
}

async fn save_aoes(token: String, aoes: Vec<AoeModel>) -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
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
            Err(AdapterErr {
                code: ErrCode::PlccConnectErr,
                msg: "调用策略API新增策略失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::PlccConnectErr,
            msg: "连接PLCC失败".to_string(),
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
            Err(AdapterErr {
                code: ErrCode::PlccConnectErr,
                msg: "调用重置API失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::PlccConnectErr,
            msg: "连接PLCC失败".to_string(),
        })
    }
}

pub async fn aoe_result_upload() -> Result<(), AdapterErr> {
    tokio::spawn(async {
        if let Err(e) = aoe_upload_loop().await {
            log::error!("do aoe_result_upload error: {}", e.msg);
        }
    });
    Ok(())
}

async fn aoe_upload_loop() -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mqtt_server = env.get_mqtt_server();
    let mqtt_server_port = env.get_mqtt_server_port();
    let app_name = env.get_app_name();
    let app_model = env.get_app_model();

    let mut ticker = interval(Duration::from_secs(5));
    let mut count = 0;
    let mut token = "".to_string();
    let mut last_time: HashMap<u64, u64> = HashMap::new();

    let mqttoptions = get_mqttoptions("plcc_aoe_result", &mqtt_server, mqtt_server_port);
    let topic_request_update = format!("/sys.brd/{app_name}/S-dataservice/F-UpdateSOE");
    let topic_request_set = format!("/sys.dbc/{app_name}/S-dataservice/F-SetSOE");
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 100);
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(_) => {}
                Err(e) => {
                    log::error!("do aoe_result_upload error: {:?}", e);
                    break;
                }
            }
        }
    });
    loop {
        ticker.tick().await;
        if count >= 86400 {
            count = 0;
        }
        if count == 0 {
            match login().await {
                Ok(v) => token = v,
                Err(_) => continue
            }
        }
        count += 1;
        if let Err(e) = do_aoe_upload(&client, &topic_request_update, &topic_request_set, &token, &mut last_time, &app_model).await {
            log::error!("do aoe_result_upload error: {}", e.msg);
        }
    }
}

async fn do_aoe_upload(client: &AsyncClient, topic_request_update: &str, topic_request_set: &str, token: &str, last_time: &mut HashMap<u64, u64>, app_model: &str) -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let app_name = env.get_app_name();
    let my_aoes = query_aoes(token.to_string()).await?;
    let aids = my_aoes.iter().map(|v| v.id).collect::<Vec<u64>>();
    let aoe_results = query_aoe_result(token.to_string(), aids).await?;
    let points_mapping = point_param_map::get_all();
    let aoe_mapping = query_aoe_mapping().await?;
    let my_aoe_result = aoe_results.results.iter()
        .filter(|a| {
            let aoe_id = a.aoe_id.unwrap();
            let end_time = a.end_time.unwrap();
            if let Some(v) = last_time.get_mut(&aoe_id) {
                let old = v.clone();
                *v = end_time;
                old != end_time
            } else {
                last_time.insert(aoe_id, end_time);
                true
            }
        })
        .map(|a|{
            let action_results = a.action_results.iter().filter_map(|action_result|{
                aoe_action_result_to_north(action_result.clone(), &points_mapping).ok()
            }).collect::<Vec<MyPbActionResult>>();
            let aoe_id = if let Some(sid) = a.aoe_id {
                if let Some(nid) = aoe_mapping.get(&sid) {
                    Some(nid.to_string())
                } else {
                    None
                }
            } else {
                None
            };
            MyPbAoeResult {
                aoe_id,
                start_time: a.start_time,
                end_time: a.end_time,
                event_results: a.event_results.clone(),
                action_results,
            }
        }).collect::<Vec<MyPbAoeResult>>();
    if !my_aoe_result.is_empty() {
        let dev = query_register_dev().await?;
        let body = generate_aoe_update(my_aoe_result.clone(), app_model.to_string(), dev.clone(), app_name.clone());
        let payload = serde_json::to_string(&body).unwrap();
        client_publish(client, topic_request_update, &payload).await?;
        let body = generate_aoe_set(my_aoe_result, app_model.to_string(), dev, app_name);
        let payload = serde_json::to_string(&body).unwrap();
        client_publish(client, topic_request_set, &payload).await?;
    }
    Ok(())
}

async fn query_aoe_result(token: String, ids: Vec<u64>) -> Result<PbAoeResults, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let plcc_server = env.get_plcc_server();
    let ids = ids.iter()
        .map(|n| n.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let today = chrono::Local::now().format("%Y-%m-%d").to_string();
    let url = format!("{plcc_server}/{URL_AOE_RESULTS}?id={ids}&date={today}&last_only=true");
    let headers = get_header(token);
    let client = Client::new();
    match client
        .get(&url)
        .headers(headers)
        .send().await {
        Ok(response) => {
            if let Ok(aoe_results) = response.json::<PbAoeResults>().await {
                Ok(aoe_results)
            } else {
                Err(AdapterErr {
                    code: ErrCode::PlccConnectErr,
                    msg: "调用策略执行结果API获取测点失败".to_string(),
                })
            }
        },
        Err(ee) => {
            log::error!("link to plcc error: {:?}", ee);
            Err(AdapterErr {
                code: ErrCode::PlccConnectErr,
                msg: format!("连接PLCC失败：{:?}", ee),
            })
        }
    }
}

pub async fn do_query_aoe_status() -> Result<Vec<CloudEventAoeStatus>, AdapterErr> {
    let token = login().await?;
    let mut aoe_status = vec![];
    match query_unrun_aoes(token.clone()).await {
        Ok(unrun_aoes) => {
            for aoe_id in unrun_aoes {
                aoe_status.push(CloudEventAoeStatus {
                    aoe_id,
                    aoe_status: 0,
                });
            }
        },
        Err(e) => {
            return Err(e);
        }
    }
    match query_running_aoes(token.clone()).await {
        Ok(running_aoes) => {
            for aoe_id in running_aoes {
                aoe_status.push(CloudEventAoeStatus {
                    aoe_id,
                    aoe_status: 1,
                });
            }
        },
        Err(e) => {
            return Err(e);
        }
    }
    aoe_status.sort_by_key(|x| x.aoe_id);
    Ok(aoe_status)
}

async fn query_unrun_aoes(token: String) -> Result<Vec<u64>, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let plcc_server = env.get_plcc_server();
    let url = format!("{plcc_server}/{URL_UNRUN_AOES}");
    let headers = get_header(token);
    let client = Client::new();
    match client
        .get(&url)
        .headers(headers)
        .send().await {
        Ok(response) => {
            if let Ok(aoe_ids) = response.json::<Vec<u64>>().await {
                Ok(aoe_ids)
            } else {
                Err(AdapterErr {
                    code: ErrCode::PlccConnectErr,
                    msg: "调用查询未运行策略API失败".to_string(),
                })
            }
        },
        Err(ee) => {
            log::error!("link to plcc error: {:?}", ee);
            Err(AdapterErr {
                code: ErrCode::PlccConnectErr,
                msg: format!("连接PLCC失败：{:?}", ee),
            })
        }
    }
}

async fn query_running_aoes(token: String) -> Result<Vec<u64>, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let plcc_server = env.get_plcc_server();
    let url = format!("{plcc_server}/{URL_RUNNING_AOES}");
    let headers = get_header(token);
    let client = Client::new();
    match client
        .get(&url)
        .headers(headers)
        .send().await {
        Ok(response) => {
            if let Ok(aoe_ids) = response.json::<Vec<u64>>().await {
                Ok(aoe_ids)
            } else {
                Err(AdapterErr {
                    code: ErrCode::PlccConnectErr,
                    msg: "调用查询运行中策略API失败".to_string(),
                })
            }
        },
        Err(ee) => {
            log::error!("link to plcc error: {:?}", ee);
            Err(AdapterErr {
                code: ErrCode::PlccConnectErr,
                msg: format!("连接PLCC失败：{:?}", ee),
            })
        }
    }
}

pub async fn do_aoe_action(aoe_control: AoeControl) -> Result<(), AdapterErr> {
    let token = login().await?;
    aoe_action(token, aoe_control).await
}

async fn aoe_action(token: String, aoe_control: AoeControl) -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let plcc_server = env.get_plcc_server();
    let url = format!("{plcc_server}/{URL_AOE_CONTROL}");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .post(&url)
        .headers(headers)
        .json(&aoe_control)
        .send().await {
        if response.status() == StatusCode::OK {
            Ok(())
        } else {
            Err(AdapterErr {
                code: ErrCode::PlccConnectErr,
                msg: "调用启停策略API失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::PlccConnectErr,
            msg: "连接PLCC失败".to_string(),
        })
    }
}
