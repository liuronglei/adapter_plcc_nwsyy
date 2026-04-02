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
use crate::model::polars_to_json_df;
use crate::model::south::{CommitNote, DffModel, DffResult, FlowOperation, SysAoes, SysPoints};
use crate::model::north::{MyDffResult, MyPbActionResult, MyPbEventResult};
use crate::utils::jsonmodel::from_serde_value_to_dff_model;
use crate::utils::mqttclient::{get_mqttoptions, client_publish};
use crate::utils::memsmqtt::{generate_dff_update, generate_dff_set};
use crate::utils::plccmqtt::query_register_dev;
use crate::utils::localapi::query_dff_mapping;

use crate::model::{aoe_event_result_to_north, aoe_action_result_to_north};
use crate::model::datacenter::CloudEventAoeStatus;
use crate::model::south::{AoeControl, AoeModel, PbAoeResults};
use crate::model::north::MyPbAoeResult;
use crate::utils::global::{PARAM_POINT_MAP, POINT_PARAM_MAP, MEMS_LAST_RESET_TIME};
use crate::utils::plccmqtt::{generate_aoe_update, generate_aoe_set};
use crate::utils::localapi::{query_aoe_mapping, query_point_mapping};

use crate::{ADAPTER_NAME, AdapterErr, ErrCode, MODEL_FROZEN, URL_RUNNING_DFFS, URL_DFF_RESULTS, URL_DFFS, URL_LOGIN,
    URL_MEMS_RESET, URL_DFF_START, URL_DFF_CONTROL, URL_UNRUN_DFFS, URL_IMPORT_POINTS, URL_POINTS_VERSION,
    URL_POINTS_APPLY, URL_POINTS, URL_AOES_VERSION, URL_AOES, URL_AOE_CONTROL, URL_AOE_RESULTS, URL_RUNNING_AOES,
    URL_UNRUN_AOES, URL_AOES_APPLY};
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

pub async fn do_reset_mems(
    unrun_dffs_north: Vec<u64>,
    dff_mapping: &HashMap<u64, u64>,
    old_aoe_mapping: &HashMap<u64, u64>,
    new_aoe_mapping: &HashMap<u64, u64>
) -> Result<(), AdapterErr> {
    loop {
        // 8秒内不允许重复reset
        if MEMS_LAST_RESET_TIME.time_diff() < 8000 {
            log::warn!("do reset MEMS within 8 seconds, waiting...");
            actix_rt::time::sleep(Duration::from_millis(1000)).await;
        } else {
            MEMS_LAST_RESET_TIME.update();
            break;
        }
    }
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
    // 记录重置前的aoe状态
    let unrun_aoes = query_unrun_aoes(token.clone()).await?;
    let new_aoe_values = new_aoe_mapping.values().copied().collect::<HashSet<u64>>();
    let unrun_aoes = unrun_aoes
        .iter()
        .filter_map(|k| {
            old_aoe_mapping.get(k).and_then(|v| {
                if new_aoe_values.contains(v) {
                    Some(*k)
                } else {
                    None
                }
            })
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
            msg: "failed to connect MEMS".to_string(),
        })
    }
}

async fn query_dffs(token: String) -> Result<Vec<DffModel>, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_server = env.get_mems_server();
    let url = format!("{mems_server}/{URL_DFFS}_json");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .get(&url)
        .headers(headers)
        .send().await {
        if let Ok(dffs_value) = response.json::<Vec<serde_json::Value>>().await {
            let mut dffs = vec![];
            for dff_value in dffs_value {
                match from_serde_value_to_dff_model(&dff_value) {
                    Ok(dff) => {
                        dffs.push(dff);
                    }
                    Err(e) => {
                        log::error!("from_serde_value_to_dff_model: {e}");
                    },
                }
            }
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
            msg: "failed to connect MEMS".to_string(),
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
            msg: "failed to connect MEMS".to_string(),
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
    let url = format!("{mems_server}/{URL_MEMS_RESET}");
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
            msg: "failed to connect MEMS".to_string(),
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
                result: polars_to_json_df(&a.result),
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
                msg: format!("failed to connect MEMS：{:?}", ee),
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
                msg: format!("failed to connect MEMS：{:?}", ee),
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
                msg: format!("failed to connect MEMS：{:?}", ee),
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
            msg: "failed to connect MEMS".to_string(),
        })
    }
}

pub async fn do_query_aoes() -> Result<Vec<AoeModel>, AdapterErr> {
    let token = login().await?;
    query_aoes(token).await
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

async fn delete_aoes(token: String, ids: Vec<u64>) -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_server = env.get_mems_server();
    let ids = ids.iter()
        .map(|n| n.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let url = format!("{mems_server}/{URL_AOES}/{ids}");
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
                msg: "调用策略API删除策略失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::MemsConnectErr,
            msg: "failed to connect MEMS".to_string(),
        })
    }
}

async fn query_aoes(token: String) -> Result<Vec<AoeModel>, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_server = env.get_mems_server();
    let url = format!("{mems_server}/{URL_AOES}");
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
                code: ErrCode::MemsConnectErr,
                msg: "调用策略API获取策略失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::MemsConnectErr,
            msg: "failed to connect MEMS".to_string(),
        })
    }
}

async fn save_aoes(token: String, aoes: Vec<AoeModel>) -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_server = env.get_mems_server();
    let url = format!("{mems_server}/{URL_AOES}");
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
                code: ErrCode::MemsConnectErr,
                msg: "调用策略API新增策略失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::MemsConnectErr,
            msg: "failed to connect MEMS".to_string(),
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
    let frozen_model = MODEL_FROZEN.to_string();

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
        if let Err(e) = do_aoe_upload(&client, &topic_request_update, &topic_request_set, &token, &mut last_time, &frozen_model).await {
            log::error!("do aoe_result_upload error: {}", e.msg);
        }
    }
}

async fn do_aoe_upload(client: &AsyncClient, topic_request_update: &str, topic_request_set: &str, token: &str, last_time: &mut HashMap<u64, u64>, frozen_model: &str) -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let app_name = env.get_app_name();
    let my_aoes = query_aoes(token.to_string()).await?;
    let aids = my_aoes.iter().map(|v| v.id).collect::<Vec<u64>>();
    let aoe_results = query_aoe_result(token.to_string(), aids).await?;
    // 查询映射，如果映射为空，则从数据库填充
    let mut points_mapping = POINT_PARAM_MAP.get_all();
    if points_mapping.is_empty() {
        let param_point_map = query_point_mapping().await?;
        let point_param_map = param_point_map.iter().map(|(k, v)| (*v, k.clone())).collect::<HashMap<u64, String>>();
        PARAM_POINT_MAP.save_all(param_point_map);
        POINT_PARAM_MAP.save_all(point_param_map.clone());
        points_mapping = point_param_map;
    }
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
            let event_results = a.event_results.iter().filter_map(|event_result|{
                aoe_event_result_to_north(event_result.clone()).ok()
            }).collect::<Vec<MyPbEventResult>>();
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
                event_results,
                action_results,
            }
        }).collect::<Vec<MyPbAoeResult>>();
    if !my_aoe_result.is_empty() {
        let dev = query_register_dev().await?;
        let body = generate_aoe_update(my_aoe_result.clone(), frozen_model.to_string(), dev.clone(), app_name.clone());
        let payload = serde_json::to_string(&body).unwrap();
        client_publish(client, topic_request_update, &payload).await?;
        let body = generate_aoe_set(my_aoe_result, frozen_model.to_string(), dev, app_name);
        let payload = serde_json::to_string(&body).unwrap();
        client_publish(client, topic_request_set, &payload).await?;
    }
    Ok(())
}

async fn query_aoe_result(token: String, ids: Vec<u64>) -> Result<PbAoeResults, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_server = env.get_mems_server();
    let ids = ids.iter()
        .map(|n| n.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let today = chrono::Local::now().format("%Y-%m-%d").to_string();
    let url = format!("{mems_server}/{URL_AOE_RESULTS}?id={ids}&date={today}&last_only=true");
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
                    code: ErrCode::MemsConnectErr,
                    msg: "调用API获取策略执行结果失败".to_string(),
                })
            }
        },
        Err(ee) => {
            log::error!("link to plcc error: {:?}", ee);
            Err(AdapterErr {
                code: ErrCode::MemsConnectErr,
                msg: format!("failed to connect MEMS：{:?}", ee),
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
    let mems_server = env.get_mems_server();
    let url = format!("{mems_server}/{URL_UNRUN_AOES}");
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
                    code: ErrCode::MemsConnectErr,
                    msg: "调用查询未运行策略API失败".to_string(),
                })
            }
        },
        Err(ee) => {
            log::error!("link to plcc error: {:?}", ee);
            Err(AdapterErr {
                code: ErrCode::MemsConnectErr,
                msg: format!("failed to connect MEMS：{:?}", ee),
            })
        }
    }
}

async fn query_running_aoes(token: String) -> Result<Vec<u64>, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_server = env.get_mems_server();
    let url = format!("{mems_server}/{URL_RUNNING_AOES}");
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
                    code: ErrCode::MemsConnectErr,
                    msg: "调用查询运行中策略API失败".to_string(),
                })
            }
        },
        Err(ee) => {
            log::error!("link to plcc error: {:?}", ee);
            Err(AdapterErr {
                code: ErrCode::MemsConnectErr,
                msg: format!("failed to connect MEMS：{:?}", ee),
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
    let mems_server = env.get_mems_server();
    let url = format!("{mems_server}/{URL_AOE_CONTROL}");
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
                code: ErrCode::MemsConnectErr,
                msg: "调用启停策略API失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::MemsConnectErr,
            msg: "failed to connect MEMS".to_string(),
        })
    }
}

pub async fn do_import_points(point_ids: Vec<u64>) -> Result<(), AdapterErr> {
    let token = login().await?;
    import_points(token, point_ids).await
}

async fn import_points(token: String, point_ids: Vec<u64>) -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_server = env.get_mems_server();
    let plcc_bee_id = env.get_plcc_beeid();
    let url = format!("{mems_server}/{URL_IMPORT_POINTS}/{plcc_bee_id}");
    let headers = get_header(token);
    let client = Client::new();
    let data = point_ids
        .iter()
        .map(|x| x.to_string())
        .collect::<Vec<String>>()
        .join(",");
    let data = format!("{:?}", data);
    let body = data.as_bytes().to_vec();
    if let Ok(response) = client
        .post(&url)
        .headers(headers)
        .body(body)
        .send().await {
        if response.status() == StatusCode::OK {
            Ok(())
        } else {
            Err(AdapterErr {
                code: ErrCode::MemsConnectErr,
                msg: "调用同步测点API失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::MemsConnectErr,
            msg: "failed to connect MEMS".to_string(),
        })
    }
}

pub async fn do_apply_current_aoes() -> Result<(), AdapterErr> {
    let token = login().await?;
    let notes = query_aoes_versions(token.clone()).await?;
    let version = generate_new_id(&notes.iter().map(|v| v.version).collect::<Vec<u32>>());
    let note = CommitNote {
        version,
        note: "auto apply".to_string(),
        tree_id: "memsAoeSettingTreeId".to_string(),
    };
    add_aoes_version(token.clone(), note).await?;
    let aoes = query_apply_aoes(token.clone(), version).await?;
    apply_aoes_version(token, aoes).await?;
    Ok(())
}

async fn query_aoes_versions(token: String) -> Result<Vec<CommitNote>, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_server = env.get_mems_server();
    let url = format!("{mems_server}/{URL_AOES_VERSION}");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .get(&url)
        .headers(headers)
        .send().await {
        if let Ok(notes) = response.json::<Vec<CommitNote>>().await {
            Ok(notes)
        } else {
            Err(AdapterErr {
                code: ErrCode::MemsConnectErr,
                msg: "调用查询所有策略版本API失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::MemsConnectErr,
            msg: "failed to connect MEMS".to_string(),
        })
    }
}

async fn add_aoes_version(token: String, note: CommitNote) -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_server = env.get_mems_server();
    let url = format!("{mems_server}/{URL_AOES_VERSION}");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .post(&url)
        .headers(headers)
        .json(&note)
        .send().await {
        if response.status() == StatusCode::OK {
            Ok(())
        } else {
            Err(AdapterErr {
                code: ErrCode::MemsConnectErr,
                msg: "调用策略版本添加API失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::MemsConnectErr,
            msg: "failed to connect MEMS".to_string(),
        })
    }
}

async fn query_apply_aoes(token: String, version: u32) -> Result<SysAoes, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_server = env.get_mems_server();
    let url = format!("{mems_server}/{URL_AOES}/for_apply?version={version}");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .get(&url)
        .headers(headers)
        .send().await {
        if let Ok(aoes) = response.json::<SysAoes>().await {
            Ok(aoes)
        } else {
            Err(AdapterErr {
                code: ErrCode::MemsConnectErr,
                msg: "调用根据版本查询策略API失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::MemsConnectErr,
            msg: "failed to connect MEMS".to_string(),
        })
    }
}

async fn apply_aoes_version(token: String, aoes: SysAoes) -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_server = env.get_mems_server();
    let url = format!("{mems_server}/{URL_AOES_APPLY}");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .post(&url)
        .json(&aoes)
        .headers(headers)
        .send().await {
        if response.status() == StatusCode::OK {
            Ok(())
        } else {
            Err(AdapterErr {
                code: ErrCode::MemsConnectErr,
                msg: "调用策略应用API失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::MemsConnectErr,
            msg: "failed to connect MEMS".to_string(),
        })
    }
}

pub async fn do_apply_current_points() -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_bee_id = env.get_mems_beeid();
    let token = login().await?;
    let notes = query_points_versions(token.clone()).await?;
    let version = generate_new_id(&notes.iter().map(|v| v.version).collect::<Vec<u32>>());
    let note = CommitNote {
        version,
        note: "auto apply".to_string(),
        tree_id: format!("{mems_bee_id}_point_version"),
    };
    add_points_version(token.clone(), note).await?;
    let points = query_apply_points(token.clone(), version).await?;
    apply_points_version(token, points).await?;
    Ok(())
}

async fn query_points_versions(token: String) -> Result<Vec<CommitNote>, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_server = env.get_mems_server();
    let url = format!("{mems_server}/{URL_POINTS_VERSION}");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .get(&url)
        .headers(headers)
        .send().await {
        if let Ok(notes) = response.json::<Vec<CommitNote>>().await {
            Ok(notes)
        } else {
            Err(AdapterErr {
                code: ErrCode::MemsConnectErr,
                msg: "调用查询所有测点版本API失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::MemsConnectErr,
            msg: "failed to connect MEMS".to_string(),
        })
    }
}

async fn add_points_version(token: String, note: CommitNote) -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_server = env.get_mems_server();
    let url = format!("{mems_server}/{URL_POINTS_VERSION}");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .post(&url)
        .headers(headers)
        .json(&note)
        .send().await {
        if response.status() == StatusCode::OK {
            Ok(())
        } else {
            Err(AdapterErr {
                code: ErrCode::MemsConnectErr,
                msg: "调用测点版本添加API失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::MemsConnectErr,
            msg: "failed to connect MEMS".to_string(),
        })
    }
}

async fn query_apply_points(token: String, version: u32) -> Result<SysPoints, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_server = env.get_mems_server();
    let url = format!("{mems_server}/{URL_POINTS}/for_apply?version={version}");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .get(&url)
        .headers(headers)
        .send().await {
        if let Ok(points) = response.json::<SysPoints>().await {
            Ok(points)
        } else {
            Err(AdapterErr {
                code: ErrCode::MemsConnectErr,
                msg: "调用根据版本查询测点API失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::MemsConnectErr,
            msg: "failed to connect MEMS".to_string(),
        })
    }
}

async fn apply_points_version(token: String, points: SysPoints) -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mems_server = env.get_mems_server();
    let url = format!("{mems_server}/{URL_POINTS_APPLY}");
    let headers = get_header(token);
    let client = Client::new();
    if let Ok(response) = client
        .post(&url)
        .json(&points)
        .headers(headers)
        .send().await {
        if response.status() == StatusCode::OK {
            Ok(())
        } else {
            Err(AdapterErr {
                code: ErrCode::MemsConnectErr,
                msg: "调用测点应用API失败".to_string(),
            })
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::MemsConnectErr,
            msg: "failed to connect MEMS".to_string(),
        })
    }
}

/// 生成新的ID
fn generate_new_id(current_keys: &[u32]) -> u32 {
    // 先取最大的ID
    if let Some(max_id) = current_keys.iter().max() {
        // 如果ID满了，则从1开始找未被使用的ID
        if *max_id == u32::max_value() {
            for (i, v) in current_keys.iter().enumerate() {
                if *v > i as u32 + 1 {
                    return i as u32 + 1;
                }
            }
            0
        } else {
            *max_id + 1
        }
    } else {
        1
    }
}
