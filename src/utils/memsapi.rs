use std::collections::{HashMap, HashSet};

use rumqttc::AsyncClient;
use reqwest::{Client, StatusCode};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde_json::json;
use base64::{Engine, engine::general_purpose::STANDARD as b64_standard};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::time::{interval, Duration};
use crate::model::datacenter::MemsEventDffStatus;
use crate::model::south::{DffModel, DffResult, FlowOperation};
use crate::model::north::MyDffResult;
use crate::utils::jsonmodel::from_serde_value_to_dff_model;
use crate::utils::mqttclient::{get_mqttoptions, client_publish};
use crate::utils::memsmqtt::{generate_dff_update, generate_dff_set};
use crate::utils::plccmqtt::query_register_dev;
use crate::utils::localapi::query_dff_mapping;
use crate::{ADAPTER_NAME, AdapterErr, ErrCode, MODEL_FROZEN, URL_RUNNING_DFFS, URL_DFF_RESULTS, URL_DFFS, URL_LOGIN,
    URL_DFF_RESET, URL_DFF_START, URL_DFF_CONTROL, URL_UNRUN_DFFS};
use crate::env::Env;

const PASSWORD_V_KEY: &[u8] = b"zju-plcc";
const HEADER_TOKEN: &str = "access-token";

type HmacSha256 = Hmac<Sha256>;

pub async fn update_dffs(dffs: Vec<DffModel>) -> Result<(), AdapterErr> {
    let token = login().await?;
    let old_dffs = query_dffs(token.clone()).await?;
    let aids = old_dffs.iter().map(|v| v.id).collect::<Vec<u64>>();
    if !aids.is_empty() {
        delete_dffs(token.clone(), aids).await?;
    }
    save_dffs(token, dffs).await
}

pub async fn do_start_dff() -> Result<(), AdapterErr> {
    let token = login().await?;
    let dffs = query_dffs(token.clone()).await?;
    let running_dffs = dffs.iter().map(|k| k.id).collect::<Vec<u64>>();
    match start_all(token).await {
        Ok(_) => {
            actix_rt::time::sleep(std::time::Duration::from_millis(2000)).await;
            do_dff_action(FlowOperation::StartFlows(running_dffs)).await
        },
        Err(e) => Err(e),
    }
}

pub async fn do_reset_dff(unrun_dffs_north: Vec<u64>, dff_mapping: &HashMap<u64, u64>) -> Result<(), AdapterErr> {
    let token = login().await?;
    // 记录重置前的dff状态
    let running_dffs = dff_mapping
        .iter()
        .filter_map(|(k, v)| {
            if unrun_dffs_north.contains(v) {
                None
            } else {
                Some(*k)
            }
        }).collect::<Vec<u64>>();
    match reset(token.clone()).await {
        Ok(_) => {
            actix_rt::time::sleep(std::time::Duration::from_millis(2000)).await;
            do_dff_action(FlowOperation::StartFlows(running_dffs)).await
        },
        Err(e) => Err(e),
    }
}

async fn delete_dffs(token: String, ids: Vec<u64>) -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_server = env.get_mems_server();
    let ids = ids.iter()
        .map(|n| n.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let url = format!("{mems_server}/{URL_DFFS}/{ids}");
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
                code: ErrCode::MemsConnectErr,
                msg: "调用报表API删除报表失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::MemsConnectErr,
            msg: "连接MEMS失败".to_string(),
        })
    }
}

async fn query_dffs(token: String) -> Result<Vec<DffModel>, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_server = env.get_mems_server();
    let url = format!("{mems_server}/{URL_DFFS}");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .get(&url)
        .headers(headers)
        .send().await {
        if let Ok(dffs) = response.json::<Vec<DffModel>>().await {
            Ok(dffs)
        } else {
            Err(AdapterErr {
                code: ErrCode::MemsConnectErr,
                msg: "调用报表API获取报表失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::MemsConnectErr,
            msg: "连接MEMS失败".to_string(),
        })
    }
}

async fn save_dffs(token: String, dffs: Vec<DffModel>) -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_server = env.get_mems_server();
    let url = format!("{mems_server}/{URL_DFFS}");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .post(&url)
        .headers(headers)
        .json(&dffs)
        .send().await {
        if response.status() == StatusCode::OK {
            Ok(())
        } else {
            Err(AdapterErr {
                code: ErrCode::MemsConnectErr,
                msg: "调用报表API新增报表失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::MemsConnectErr,
            msg: "连接MEMS失败".to_string(),
        })
    }
}

async fn login() -> Result<String, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_user = env.get_mems_user();
    let mems_pwd = env.get_mems_pwd();
    let mems_server = env.get_mems_server();
    let login_url = format!("{mems_server}/{URL_LOGIN}");
    let body = json!((mems_user, password_v_encode(mems_pwd)));
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
                code: ErrCode::MemsConnectErr,
                msg: error,
            })
        } else {
            Ok(token)
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::MemsConnectErr,
            msg: "MEMS登录失败".to_string(),
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
    let mems_server = env.get_mems_server();
    let url = format!("{mems_server}/{URL_DFF_RESET}");
    let headers = get_header(token.clone());
    let client = Client::new();
    if let Ok(response) = client
        .post(&url)
        .headers(headers)
        .send().await {
        if response.status() == StatusCode::OK {
            start_all(token).await
        } else {
            Err(AdapterErr {
                code: ErrCode::MemsConnectErr,
                msg: "调用重置报表API失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::MemsConnectErr,
            msg: "连接MEMS失败".to_string(),
        })
    }
}

async fn start_all(token: String) -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_server = env.get_mems_server();
    let url = format!("{mems_server}/{URL_DFF_START}");
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
                code: ErrCode::MemsConnectErr,
                msg: "调用启用报表API失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::MemsConnectErr,
            msg: "连接MEMS失败".to_string(),
        })
    }
}

pub async fn dff_result_upload() -> Result<(), AdapterErr> {
    tokio::spawn(async {
        if let Err(e) = dff_upload_loop().await {
            log::error!("do dff_result_upload error: {}", e.msg);
        }
    });
    Ok(())
}

async fn dff_upload_loop() -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mqtt_server = env.get_mqtt_server();
    let mqtt_server_port = env.get_mqtt_server_port();
    let app_name = env.get_app_name();
    let frozen_model = MODEL_FROZEN.to_string();

    let mut ticker = interval(Duration::from_secs(5));
    let mut count = 0;
    let mut token = "".to_string();
    let mut last_time: HashMap<u64, u64> = HashMap::new();

    let mqttoptions = get_mqttoptions("mems_dff_result", &mqtt_server, mqtt_server_port);
    let topic_request_update = format!("/sys.brd/{app_name}/S-dataservice/F-UpdateSOE");
    let topic_request_set = format!("/sys.dbc/{app_name}/S-dataservice/F-SetSOE");
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 100);
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(_) => {}
                Err(e) => {
                    log::error!("do dff_result_upload error: {:?}", e);
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
        if let Err(e) = do_dff_upload(&client, &topic_request_update, &topic_request_set, &token, &mut last_time, &frozen_model).await {
            log::error!("do dff_result_upload error: {}", e.msg);
        }
    }
}

async fn do_dff_upload(client: &AsyncClient, topic_request_update: &str, topic_request_set: &str, token: &str, last_time: &mut HashMap<u64, u64>, frozen_model: &str) -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let app_name = env.get_app_name();
    let my_dffs = query_dffs(token.to_string()).await?;
    let aids = my_dffs.iter().map(|v| v.id).collect::<Vec<u64>>();
    let dff_results = query_dff_results(token.to_string(), aids).await?;
    let dff_mapping = query_dff_mapping().await?;
    let my_dff_result = dff_results.iter()
        .filter(|a| {
            let dff_id = a.flow_id;
            let end_time = a.end_time;
            if let Some(v) = last_time.get_mut(&dff_id) {
                let old = v.clone();
                *v = end_time;
                old != end_time
            } else {
                last_time.insert(dff_id, end_time);
                true
            }
        }).map(|a|{
            let flow_id = if let Some(nid) = dff_mapping.get(&a.flow_id) {
                Some(nid.to_string())
            } else {
                None
            };
            MyDffResult {
                flow_id,
                start_time: Some(a.start_time),
                end_time: Some(a.end_time),
                result: a.result.clone(),
            }
        }).collect::<Vec<MyDffResult>>();
    if !my_dff_result.is_empty() {
        let dev = query_register_dev().await?;
        let body = generate_dff_update(my_dff_result.clone(), frozen_model.to_string(), dev.clone(), app_name.clone());
        let payload = serde_json::to_string(&body).unwrap();
        client_publish(client, topic_request_update, &payload).await?;
        let body = generate_dff_set(my_dff_result, frozen_model.to_string(), dev, app_name);
        let payload = serde_json::to_string(&body).unwrap();
        client_publish(client, topic_request_set, &payload).await?;
    }
    Ok(())
}

async fn query_dff_results(token: String, ids: Vec<u64>) -> Result<Vec<DffResult>, AdapterErr> {
    let mut results = vec![];
    for id in ids {
        if let Ok(Some(dff_result)) = query_dff_result(token.clone(), id).await {
            results.push(dff_result);
        }
    }
    Ok(results)
}

async fn query_dff_result(token: String, id: u64) -> Result<Option<DffResult>, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_server = env.get_mems_server();
    let url = format!("{mems_server}/{URL_DFF_RESULTS}?id={id}");
    let headers = get_header(token);
    let client = Client::new();
    match client
        .get(&url)
        .headers(headers)
        .send().await {
        Ok(response) => {
            if let Ok(dff_result) = response.json::<Option<DffResult>>().await {
                Ok(dff_result)
            } else {
                Err(AdapterErr {
                    code: ErrCode::MemsConnectErr,
                    msg: "调用API获取报表执行结果失败".to_string(),
                })
            }
        },
        Err(ee) => {
            log::error!("link to plcc error: {:?}", ee);
            Err(AdapterErr {
                code: ErrCode::MemsConnectErr,
                msg: format!("连接MEMS失败：{:?}", ee),
            })
        }
    }
}

pub async fn do_query_dff_status() -> Result<Vec<MemsEventDffStatus>, AdapterErr> {
    let token = login().await?;
    let mut dff_status = vec![];
    match query_unrun_dffs(token.clone()).await {
        Ok(unrun_dffs) => {
            for dff_id in unrun_dffs {
                dff_status.push(MemsEventDffStatus {
                    dff_id,
                    dff_status: 0,
                });
            }
        },
        Err(e) => {
            return Err(e);
        }
    }
    match query_running_dffs(token.clone()).await {
        Ok(running_dffs) => {
            for dff_id in running_dffs {
                dff_status.push(MemsEventDffStatus {
                    dff_id,
                    dff_status: 1,
                });
            }
        },
        Err(e) => {
            return Err(e);
        }
    }
    dff_status.sort_by_key(|x| x.dff_id);
    Ok(dff_status)
}

pub async fn do_query_unrun_dffs() -> Result<Vec<u64>, AdapterErr> {
    let token = login().await?;
    query_unrun_dffs(token).await
}

async fn query_unrun_dffs(token: String) -> Result<Vec<u64>, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_server = env.get_mems_server();
    let url = format!("{mems_server}/{URL_UNRUN_DFFS}");
    let headers = get_header(token);
    let client = Client::new();
    match client
        .get(&url)
        .headers(headers)
        .send().await {
        Ok(response) => {
            if let Ok(dff_ids) = response.json::<Vec<u64>>().await {
                Ok(dff_ids)
            } else {
                Err(AdapterErr {
                    code: ErrCode::MemsConnectErr,
                    msg: "调用查询未运行报表API失败".to_string(),
                })
            }
        },
        Err(ee) => {
            log::error!("link to plcc error: {:?}", ee);
            Err(AdapterErr {
                code: ErrCode::MemsConnectErr,
                msg: format!("连接MEMS失败：{:?}", ee),
            })
        }
    }
}

async fn query_running_dffs(token: String) -> Result<Vec<u64>, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_server = env.get_mems_server();
    let url = format!("{mems_server}/{URL_RUNNING_DFFS}");
    let headers = get_header(token);
    let client = Client::new();
    match client
        .get(&url)
        .headers(headers)
        .send().await {
        Ok(response) => {
            if let Ok(dff_ids) = response.json::<Vec<u64>>().await {
                Ok(dff_ids)
            } else {
                Err(AdapterErr {
                    code: ErrCode::MemsConnectErr,
                    msg: "调用查询运行中报表API失败".to_string(),
                })
            }
        },
        Err(ee) => {
            log::error!("link to plcc error: {:?}", ee);
            Err(AdapterErr {
                code: ErrCode::MemsConnectErr,
                msg: format!("连接MEMS失败：{:?}", ee),
            })
        }
    }
}

pub async fn do_dff_action(dff_control: FlowOperation) -> Result<(), AdapterErr> {
    let token = login().await?;
    dff_action(token, dff_control).await
}

async fn dff_action(token: String, dff_control: FlowOperation) -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_server = env.get_mems_server();
    let url = format!("{mems_server}/{URL_DFF_CONTROL}");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .post(&url)
        .headers(headers)
        .json(&dff_control)
        .send().await {
        if response.status() == StatusCode::OK {
            Ok(())
        } else {
            Err(AdapterErr {
                code: ErrCode::MemsConnectErr,
                msg: "调用执行报表动作API失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::MemsConnectErr,
            msg: "连接MEMS失败".to_string(),
        })
    }
}
