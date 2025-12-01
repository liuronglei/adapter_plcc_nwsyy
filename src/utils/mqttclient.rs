use std::fs::File;
use std::io::{BufReader, Write};
use std::collections::{HashSet, HashMap};
use std::pin::Pin;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::time::{timeout, Duration};
use tokio::sync::oneshot;
use chrono::{Local, TimeZone, FixedOffset, Duration as ChronoDuration};
use tokio_cron_scheduler::{Job, JobScheduler};

use crate::utils::meter_data::export_meter_csv;
use crate::{ADAPTER_NAME, AdapterErr, ErrCode, MODEL_FROZEN, MODEL_FROZEN_METER};
use crate::env::Env;
use crate::model::datacenter::*;
use crate::model::north::{MyAoes, MyPbAoeResult, MyPoints, MyTransports, MyTransport};
use crate::model::south::{AoeControl, AoeAction};
use crate::utils::{register_result, get_point_attr};
use crate::utils::localapi::{query_dev_mapping, query_aoe_mapping};
use crate::utils::plccapi::{do_query_aoe_status, do_aoe_action};

pub fn get_mqttoptions(user_name: &str, mqtt_server: &str, mqtt_server_port: u16) -> MqttOptions {
    let time = Local::now().timestamp_millis();
    let mut mqttoptions = MqttOptions::new(format!("{user_name}_{time}"), mqtt_server, mqtt_server_port);
    mqttoptions.set_max_packet_size(1024 * 1024 * 100, 1024 * 1024 * 100);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    mqttoptions
}

pub async fn client_subscribe(client: &AsyncClient, topic: &str) -> Result<(), AdapterErr> {
    match client.subscribe(topic, QoS::AtMostOnce).await {
        Ok(_) => Ok(()),
        Err(v) => {
            Err(AdapterErr {
                code: ErrCode::MqttConnectErr,
                msg: v.to_string(),
            })
        }
    }
}

pub async fn client_publish(client: &AsyncClient, topic: &str, payload: &str) -> Result<(), AdapterErr> {
    match client.publish(topic, QoS::AtMostOnce, false, payload).await {
        Ok(_) => Ok(()),
        Err(v) => {
            Err(AdapterErr {
                code: ErrCode::MqttConnectErr,
                msg: v.to_string(),
            })
        }
    }
}

pub async fn do_query_dev(transports: &Vec<MyTransport>) -> Result<Vec<QueryDevResponseBody>, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let app_name = env.get_app_name();
    let query_dev_bodys = build_query_dev_bodys(transports);
    let body = generate_query_dev(query_dev_bodys);
    match mqtt_acquirer::<_, QueryDevResponse>(
        "plcc_query_dev".to_string(),
        format!("/sys.iot/{app_name}/S-otaservice/F-GetDCAttr"),
        format!("/{app_name}/sys.iot/S-otaservice/F-GetDCAttr"),
        body,
    ).await {
        Ok(msg) => {
            let mut has_error = false;
            let mut result = vec![];
            for data in msg.devices.clone() {
                for dev in &data.devs {
                    if data.service_id.as_str() == "serviceNotExist" || dev.not_found.clone().is_some_and(|x|!x.is_empty()) {
                        has_error = true;
                        break;
                    }
                }
                result.push(data);
            }
            if !has_error {
                Ok(result)
            } else {
                Err(AdapterErr {
                    code: ErrCode::QueryDevAttrNotFound,
                    msg: format!("查询设备信息报错，有部分属性在数据中心未找到：{:?}", msg),
                })
            }
        }
        Err(e) => Err(AdapterErr {
            code: e.code,
            msg: format!("查询南向设备信息失败，{}", e.msg),
        }),
    }
}

pub async fn do_register() -> Result<(), AdapterErr> {
    tokio::spawn(async {
        let mut result = true;
        if let Err(err) = do_register_model().await {
            result = false;
            log::error!("{}", err.msg);
        }
        if let Err(err) = do_register_app().await {
            result = false;
            log::error!("{}", err.msg);
        }
        register_result::set_result(result);
    });
    Ok(())
}

pub async fn do_register_sync() -> Result<(), AdapterErr> {
    let _ = do_register_model().await?;
    let _ = do_register_app().await?;
    register_result::set_result(true);
    Ok(())
}

async fn do_register_model() -> Result<(), AdapterErr> {
    if !get_has_model_registered().await? {
        start_register_model().await
    } else {
        log::info!("model已注册，跳过本次注册");
        Ok(())
    }
}

async fn start_register_model() -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let app_name = env.get_app_name();
    let app_model = env.get_app_model();
    let body = generate_register_model(app_model);
    match mqtt_acquirer::<_, RegisterResponse>(
        "plcc_model_register".to_string(),
        format!("/sys.dbc/{app_name}/S-dataservice/F-SetModel"),
        format!("/{app_name}/sys.dbc/S-dataservice/F-SetModel"),
        body,
    ).await {
        Ok(msg) => {
            let result = msg.ack.to_lowercase() == "true".to_string();
            if !result {
                return Err(AdapterErr {
                    code: ErrCode::ModelRegisterErr,
                    msg: "注册model失败，响应结果为false".to_string(),
                });
            } else {
                Ok(())
            }
        }
        Err(e) => Err(AdapterErr {
            code: e.code,
            msg: format!("注册model失败，{}", e.msg),
        }),
    }
}

async fn get_has_model_registered() -> Result<bool, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let app_name = env.get_app_name();
    let app_model = env.get_app_model();
    let body = generate_get_model_register(app_model.clone());
    match mqtt_acquirer::<_, GetModelResponse>(
        "get_model_register".to_string(),
        format!("/sys.dbc/{app_name}/S-dataservice/F-GetModel"),
        format!("/{app_name}/sys.dbc/S-dataservice/F-GetModel"),
        body,
    ).await {
        Ok(msg) => {
            let mut has_registered = false;
            for msg_body in msg.body {
                if msg_body.model == app_model {
                    has_registered = true;
                    break;
                }
            }
            Ok(has_registered)
        }
        Err(e) => Err(AdapterErr {
            code: e.code,
            msg: format!("获取model注册信息失败，{}", e.msg),
        }),
    }
}

async fn do_register_app() -> Result<(), AdapterErr> {
    if !get_has_app_registered().await? {
        start_register_app().await
    } else {
        log::info!("app已注册，跳过本次注册");
        Ok(())
    }
}

async fn start_register_app() -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let app_name = env.get_app_name();
    let app_model = env.get_app_model();
    let body = generate_register_app(app_model);
    match mqtt_acquirer::<_, RegisterResponse>(
        "plcc_app_register".to_string(),
        format!("/sys.dbc/{app_name}/S-dataservice/F-Register"),
        format!("/{app_name}/sys.dbc/S-dataservice/F-Register"),
        body,
    ).await {
        Ok(msg) => {
            let result = msg.ack.to_lowercase() == "true".to_string();
            if !result {
                return Err(AdapterErr {
                    code: ErrCode::AppRegisterErr,
                    msg: "注册APP失败，响应结果为false".to_string(),
                });
            } else {
                Ok(())
            }
        }
        Err(e) => Err(AdapterErr {
            code: e.code,
            msg: format!("注册APP失败，{}", e.msg),
        }),
    }
}

async fn get_has_app_registered() -> Result<bool, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let app_name = env.get_app_name();
    let app_model = env.get_app_model();
    let body = generate_get_app_register(app_model.clone());
    match mqtt_acquirer::<_, RegisterDevResult>(
        "get_app_register".to_string(),
        format!("/sys.dbc/{app_name}/S-dataservice/F-GetRegister"),
        format!("/{app_name}/sys.dbc/S-dataservice/F-GetRegister"),
        body,
    ).await {
        Ok(msg) => {
            let mut has_registered = false;
            for msg_body in msg.body {
                if msg_body.model == app_model {
                    has_registered = true;
                    break;
                }
            }
            Ok(has_registered)
        }
        Err(e) => Err(AdapterErr {
            code: e.code,
            msg: format!("获取app注册信息失败，{}", e.msg),
        }),
    }
}

pub async fn do_data_query() -> Result<(), AdapterErr> {
    tokio::spawn(async {
        // 等待5秒，避免因为API还没启动或者数据还未写入完成，查询失败
        actix_rt::time::sleep(Duration::from_millis(5000)).await;
        if let Err(e) = data_query().await {
            log::error!("do data_query error: {}", e.msg);
        }
    });
    Ok(())
}

pub async fn data_query() -> Result<(), AdapterErr> {
    let devs = query_dev_mapping().await?;
    if !devs.is_empty() {
        let env = Env::get_env(ADAPTER_NAME);
        let app_name = env.get_app_name();
        let body = generate_query_data(&devs);
        mqtt_push_only(
            "plcc_data_query".to_string(),
            format!("/sys.dbc/{app_name}/S-dataservice/F-GetRealData"),
            format!("/{app_name}/sys.dbc/S-dataservice/F-GetRealData"),
            body,
        ).await
    } else {
        Ok(())
    }
}

pub async fn do_keep_alive() -> Result<(), AdapterErr> {
    tokio::spawn(async {
        if let Err(e) = keep_alive().await {
            log::error!("do keep_alive error: {}", e.msg);
        }
    });
    Ok(())
}

pub async fn keep_alive() -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let app_name = env.get_app_name();
    mqtt_provider(
        "plcc_keep_alive".to_string(),
        format!("/sys.appman/{app_name}/S-appmanager/F-KeepAlive"),
        format!("/{app_name}/sys.appman/S-appmanager/F-KeepAlive"),
        move |payload| {
            Box::pin(async move {
                if let Ok(msg) = serde_json::from_slice::<KeepAliveRequest>(&payload) {
                    generate_keep_alive_response(msg)
                } else {
                    log::error!("do keep_alive 序列化错误: {payload:?}");
                    let time = generate_current_time();
                    KeepAliveResponse {
                        token: time.clone(),
                        time,
                        ack: "true".to_string(),
                        errmsg: "success".to_string(),
                    }
                }
            })
        },
    ).await
}

pub async fn query_register_dev() -> Result<String, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let app_name = env.get_app_name();
    let body = generate_query_register_dev();
    match mqtt_acquirer::<_, RegisterDevResult>(
        "plcc_register_dev".to_string(),
        format!("/sys.dbc/{app_name}/S-dataservice/F-GetRegister"),
        format!("/{app_name}/sys.dbc/S-dataservice/F-GetRegister"),
        body,
    ).await {
        Ok(msg) => {
            let mut dev = "".to_string();
            if !msg.body.is_empty() {
                let first_body = msg.body.first().unwrap();
                if !first_body.body.is_empty() {
                    dev = first_body.body.first().unwrap().dev.clone();
                }
            }
            Ok(dev)
        }
        Err(e) => Err(AdapterErr {
            code: e.code,
            msg: format!("查询注册dev失败，{}", e.msg),
        }),
    }
}

pub async fn do_cloud_event() -> Result<(), AdapterErr> {
    tokio::spawn(async {
        if let Err(e) = cloud_event().await {
            log::error!("do cloud_event error: {}", e.msg);
        }
    });
    Ok(())
}

pub async fn cloud_event() -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let app_name = env.get_app_name();
    mqtt_provider(
        "plcc_event".to_string(),
        format!("/ext.syy.phSmc/{app_name}/S-smclink/F-PlccEvent"),
        format!("/{app_name}/ext.syy.phSmc/S-smclink/F-PlccEvent"),
        move |payload| {
            Box::pin(async move {
                if let Ok(msg) = serde_json::from_slice::<CloudEventRequest>(&payload) {
                    match msg.cmd {
                        CloudEventCmd::GetTgPLCCConfig => {
                            do_get_plcc_config(msg)
                        },
                        CloudEventCmd::TgAOEControl => {
                            do_aoe_control(msg).await
                        },
                        CloudEventCmd::GetTgAOEStatus => {
                            do_get_aoe_status(msg).await
                        }
                    }
                } else {
                    log::error!("do cloud_event 序列化错误: {payload:?}");
                    let time = Local::now().timestamp_millis();
                    let data = get_aoe_status_body(None, ErrCode::DataJsonDeserializeErr, "Json格式错误".to_string());
                    CloudEventResponse {
                        token: time.to_string(),
                        request_id: time.to_string(),
                        time: generate_current_time(),
                        msg_info: "".to_string(),
                        data,
                    }
                }
            })
        },
    ).await
}

fn do_get_plcc_config(cloud_event: CloudEventRequest) -> CloudEventResponse {
    let env = Env::get_env(ADAPTER_NAME);
    let result_dir = env.get_result_dir();
    let point_dir = env.get_point_dir();
    let transport_dir = env.get_transport_dir();
    let aoe_dir = env.get_aoe_dir();
    let file_name_points = format!("{result_dir}/{point_dir}");
    let file_name_transports = format!("{result_dir}/{transport_dir}");
    let file_name_aoes = format!("{result_dir}/{aoe_dir}");
    let mut points = None;
    let mut transports = None;
    let mut aoes = None;
    if let Ok(file) = File::open(file_name_points) {
        let reader = BufReader::new(file);
        match serde_json::from_reader::<_, MyPoints>(reader) {
            Ok(my_points) => {
                points = my_points.points;
            },
            Err(_) => {}
        }
    }
    if let Ok(file) = File::open(file_name_transports) {
        let reader = BufReader::new(file);
        match serde_json::from_reader::<_, MyTransports>(reader) {
            Ok(my_transports) => {
                transports = my_transports.transports;
            },
            Err(_) => {}
        }
    }
    if let Ok(file) = File::open(file_name_aoes) {
        let reader = BufReader::new(file);
        match serde_json::from_reader::<_, MyAoes>(reader) {
            Ok(my_aoes) => {
                aoes = my_aoes.aoes;
            },
            Err(_) => {}
        }
    }
    CloudEventResponse {
        token: cloud_event.token,
        request_id: cloud_event.request_id,
        time: generate_current_time(),
        msg_info: "".to_string(),
        data: CloudEventResponseBody {
            points,
            transports,
            aoes,
            aoes_status: None,
            code: ErrCode::Success,
            msg: "".to_string(),
        }
    }
}

async fn do_aoe_control(cloud_event: CloudEventRequest) -> CloudEventResponse {
    let data = 'result: {
        if let Some(body) = cloud_event.body {
            if let Some(aoes_status) = body.aoes_status {
                let mut south_aoes_status = aoes_status.clone();
                match query_aoe_mapping().await {
                    Ok(aoe_mapping) => {
                        for status in south_aoes_status.iter_mut() {
                            if let Some(south_aoe_id) = aoe_mapping.iter().find_map(|(k, v)| if *v == status.aoe_id { Some(*k) } else { None }) {
                                status.aoe_id = south_aoe_id;
                            } else {
                                break 'result get_aoe_status_body(None, ErrCode::AoeIdNotFound, "未找到北向aoe_id".to_string());
                            }
                        }
                    },
                    Err(e) => {
                        break 'result get_aoe_status_body(None, ErrCode::InternalErr, e.msg);
                    }
                }
                let aoe_action = south_aoes_status.iter().map(|status| {
                    match status.aoe_status {
                        0 => {
                            AoeAction::StopAoe(status.aoe_id)
                        },
                        _ => {
                            AoeAction::StartAoe(status.aoe_id)
                        }
                    }
                }).collect::<Vec<AoeAction>>();
                match do_aoe_action(AoeControl { AoeActions: aoe_action }).await {
                    Ok(_) => {
                        get_aoe_status_body(Some(aoes_status), ErrCode::Success, "".to_string())
                    }
                    Err(e) => get_aoe_status_body(None, ErrCode::PlccActionErr, e.msg)
                }
            } else {
                get_aoe_status_body(None, ErrCode::DataJsonDeserializeErr, "body.aoes_status不能为空".to_string())
            }
        } else {
            get_aoe_status_body(None, ErrCode::DataJsonDeserializeErr, "body不能为空".to_string())
        }
    };
    CloudEventResponse {
        token: cloud_event.token,
        request_id: cloud_event.request_id,
        time: generate_current_time(),
        msg_info: "".to_string(),
        data,
    }
}

async fn do_get_aoe_status(cloud_event: CloudEventRequest) -> CloudEventResponse {
    let (aoes_status, code, msg) = 'result: {
        match do_query_aoe_status().await {
            Ok(mut aoes_status) => {
                match query_aoe_mapping().await {
                    Ok(aoe_mapping) => {
                        for status in aoes_status.iter_mut() {
                            if let Some(north_aoe_id) = aoe_mapping.get(&status.aoe_id) {
                                status.aoe_id = *north_aoe_id;
                            } else {
                                break 'result (None, ErrCode::AoeIdNotFound, "未找到北向aoe_id".to_string());
                            }
                        }
                        if let Some(body) = cloud_event.body {
                            if let Some(aoes_id) = body.aoes_id {
                                let b_set: HashSet<u64> = aoes_id.into_iter().collect();
                                aoes_status.retain(|x| b_set.contains(&x.aoe_id));
                            }
                        };
                        (Some(aoes_status), ErrCode::Success, "".to_string())
                    },
                    Err(e) => {
                        (None, ErrCode::InternalErr, e.msg)
                    }
                }
            },
            Err(e) => {
                (None, ErrCode::PlccActionErr, e.msg)
            }
        }
    };
    CloudEventResponse {
        token: cloud_event.token,
        request_id: cloud_event.request_id,
        time: generate_current_time(),
        msg_info: "".to_string(),
        data: CloudEventResponseBody {
            points: None,
            transports: None,
            aoes: None,
            aoes_status,
            code,
            msg,
        },
    }
}

fn get_aoe_status_body(aoes_status: Option<Vec<CloudEventAoeStatus>>, code: ErrCode, msg: String) -> CloudEventResponseBody {
    CloudEventResponseBody {
        points: None,
        transports: None,
        aoes: None,
        aoes_status,
        code,
        msg,
    }
}

fn generate_get_app_register(model: String) -> QueryRegisterDev {
    let time = Local::now().timestamp_millis();
    QueryRegisterDev {
        token: time.to_string(),
        time: generate_current_time(),
        body: vec![model],
    }
}

fn generate_get_model_register(model: String) -> GetModel {
    let time = Local::now().timestamp_millis();
    GetModel {
        token: time.to_string(),
        time: generate_current_time(),
        body: vec![model],
    }
}

fn generate_register_model(model: String) -> RegisterModel {
    let body = RegisterModelBody {
        name: "tgPowerCutAlarm".to_string(),
        mtype: "int".to_string(),
        unit: "".to_string(),
        deadzone: "".to_string(),
        ratio: "".to_string(),
        isReport: "0".to_string(),
        userdefine: "".to_string(),
    };
    let time = Local::now().timestamp_millis();
    RegisterModel {
        token: time.to_string(),
        time: generate_current_time(),
        model,
        body: vec![body],
    }
}

fn generate_register_app(model: String) -> RegisterApp {
    let body = RegisterAPPBody {
        model,
        port: "NULL".to_string(),
        addr: "000000".to_string(),
        desc: "terminal".to_string(),
        manuID: "".to_string(),
        manuName: "".to_string(),
        proType: "".to_string(),
        deviceType: "".to_string(),
        isReport: "0".to_string(),
        nodeID: "".to_string(),
        productID: "".to_string(),
    };
    let time = Local::now().timestamp_millis();
    RegisterApp {
        token: time.to_string(),
        time: generate_current_time(),
        body: vec![body],
    }
}

fn generate_current_time() -> String {
    let now = Local::now();
    now.format("%Y-%m-%dT%H:%M:%S%.3f%z").to_string()
}

fn generate_history_data_time() -> (String, String) {
    let tz = FixedOffset::east_opt(8 * 3600).unwrap();
    // 今天 0 点
    let today_0 = Local::now().date_naive().and_hms_milli_opt(0, 0, 0, 0).unwrap();
    // 前一天 0 点
    let yesterday_start = tz.from_local_datetime(&(today_0 - ChronoDuration::days(1))).unwrap();
    let yesterday_end = tz.from_local_datetime(&(today_0 - ChronoDuration::seconds(1))).unwrap();
    let start_time = yesterday_start.format("%Y-%m-%dT%H:%M:%S%.3f%z").to_string();
    let end_time = yesterday_end.format("%Y-%m-%dT%H:%M:%S%.3f%z").to_string();
    (start_time, end_time)
}

fn generate_query_data(devs: &Vec<QueryDevResponseBody>) -> DataQuery {
    let time = Local::now().timestamp_millis();
    let body = devs.iter().flat_map(|dev|
        dev.devs.iter().map(|v|{
            DataQueryBody {
                dev: v.dev_guid.clone(),
                totalcall: "1".to_string(),
                body: vec![],
            }
        }).collect::<Vec<DataQueryBody>>()
    ).collect::<Vec<DataQueryBody>>();
    DataQuery {
        token: time.to_string(),
        time: generate_current_time(),
        body,
    }
}

fn generate_query_register_dev() -> QueryRegisterDev {
    let time = Local::now().timestamp_millis();
    QueryRegisterDev {
        token: time.to_string(),
        time: generate_current_time(),
        body: vec![MODEL_FROZEN.to_string()],
    }
}

fn generate_query_meter_dev() -> QueryRegisterDev {
    let time = Local::now().timestamp_millis();
    QueryRegisterDev {
        token: time.to_string(),
        time: generate_current_time(),
        body: vec![MODEL_FROZEN_METER.to_string()],
    }
}

fn generate_query_dev(query_dev_bodys: Vec<QueryDevBody>) -> QueryDev {
    let time = Local::now().timestamp_millis();
    QueryDev {
        token: time.to_string(),
        time: generate_current_time(),
        devices: query_dev_bodys,
    }
}

pub fn generate_aoe_update(aoe_result: Vec<MyPbAoeResult>, model: String, dev: String, app_name: String) -> AoeUpdate {
    let time = Local::now().timestamp_millis();
    let (min_start, max_end) = find_min_start_max_end(&aoe_result);
    let body = AoeUpdateBody {
        model,
        dev,
        event: "tgAOEResult".to_string(),
        starttime: generate_time(min_start),
        endtime: generate_time(max_end),
        happen_src: app_name,
        is_need_rpt: "Yes".to_string(),
        extdata: aoe_result,
    };
    AoeUpdate {
        token: time.to_string(),
        time: generate_current_time(),
        body: vec![body],
    }
}

pub fn generate_aoe_set(aoe_result: Vec<MyPbAoeResult>, model: String, dev: String, app_name: String) -> AoeSet {
    let time = Local::now().timestamp_millis();
    let timestamp = generate_current_time();
    let (min_start, max_end) = find_min_start_max_end(&aoe_result);
    let start_time = generate_time(min_start);
    let end_time = generate_time(max_end);
    let body = AoeSetBody {
        model,
        dev,
        event: "tgAOEResult".to_string(),
        timestamp: timestamp.clone(),
        timestartgather: start_time.clone(),
        timeendgather: end_time.clone(),
        starttimestamp: start_time.clone(),
        endtimestamp: end_time.clone(),
        happen_src: app_name,
        is_need_rpt: "Yes".to_string(),
        occurnum: "1".to_string(),
        event_level: "common".to_string(),
        rpt_status: vec![RptStatusItem { net_1: "00".to_string() }],
        data: "".to_string(),
        extdata: aoe_result,
    };
    AoeSet {
        token: time.to_string(),
        time: timestamp,
        body: vec![body],
        sour_type: "104".to_string(),
    }
}

fn generate_keep_alive_response(request: KeepAliveRequest) -> KeepAliveResponse {
    KeepAliveResponse {
        token: request.token,
        time: request.time,
        ack: "true".to_string(),
        errmsg: "success".to_string(),
    }
}

fn generate_query_meter_history(devs: Vec<String>) -> RequestHistory {
    let time = Local::now().timestamp_millis();
    let timestamp = generate_current_time();
    let (start_time, end_time) = generate_history_data_time();
    let body = devs.iter().map(|dev| RequestHistoryBody {
        dev: dev.clone(),
        body: vec!["tgSupWh".to_string()],
    }).collect();
    RequestHistory {
        token: time.to_string(),
        time: timestamp,
        choice: "1".to_string(),
        time_type: "timestartgather".to_string(),
        start_time,
        end_time,
        time_span: "5".to_string(),
        frozentype: "min".to_string(),
        body,
    }
}

// fn generate_query_meter_guid(addrs: Vec<String>) -> RequestGuid {
//     let time = Local::now().timestamp_millis();
//     let timestamp = generate_current_time();
//     let body = addrs.iter().map(|addr| RequestGuidBody {
//         model: MODEL_FROZEN_METER.to_string(),
//         port: MODEL_PORT_METER.to_string(),
//         addr: addr.clone(),
//         desc: MODEL_DESC_METER.to_string(),
//     }).collect();
//     RequestGuid {
//         token: time.to_string(),
//         time: timestamp,
//         body,
//     }
// }

fn find_min_start_max_end(aoe_result: &Vec<MyPbAoeResult>) -> (Option<u64>, Option<u64>) {
    let min_start = aoe_result
        .iter()
        .filter_map(|r| r.start_time)
        .min();

    let max_end = aoe_result
        .iter()
        .filter_map(|r| r.end_time)
        .max();

    (min_start, max_end)
}

fn generate_time(ts_millis: Option<u64>) -> String {
    let time = if let Some(time) = ts_millis {
        time as i64
    } else {
        Local::now().timestamp_millis()
    };
    if let chrono::LocalResult::Single(dt) = Local.timestamp_millis_opt(time) {
        dt.format("%Y-%m-%dT%H:%M:%S%.3f%z").to_string()
    } else {
        generate_current_time()
    }
}

fn build_query_dev_bodys(transports: &Vec<MyTransport>) -> Vec<QueryDevBody> {
    let mut result = vec![];
    let mut ycyx_map: HashMap<(String, String), Vec<String>> = HashMap::new();
    let mut yt_map: HashMap<(String, String), Vec<String>> = HashMap::new();
    let mut yk_map: HashMap<(String, String), Vec<String>> = HashMap::new();
    for transport in transports {
        for point in transport.point_ycyx_ids() {
            if let Some((dev_id, service_id, attr)) = get_point_attr(&point) {
                ycyx_map.entry((dev_id, service_id)).or_default().push(attr);
            }
        }
        for point in transport.point_yt_ids() {
            if let Some((dev_id, service_id, attr)) = get_point_attr(&point) {
                yt_map.entry((dev_id, service_id)).or_default().push(attr);
            }
        }
        for point in transport.point_yk_ids() {
            if let Some((dev_id, service_id, attr)) = get_point_attr(&point) {
                yk_map.entry((dev_id, service_id)).or_default().push(attr);
            }
        }
    }
    result.extend(ycyx_map.into_iter()
        .map(|((dev_id, service_id), mut attrs)| {
            attrs.sort();
            attrs.dedup();
            QueryDevBody {
                dev_id,
                service_id,
                attrs: Some(attrs),
                setting_cmds: None,
                yk_cmds: None,
            }
        }).collect::<Vec<QueryDevBody>>());
    result.extend(yt_map.into_iter()
        .map(|((dev_id, service_id), mut attrs)| {
            attrs.sort();
            attrs.dedup();
            let mut attr_map: HashMap<String, Vec<String>> = HashMap::new();
            for attr in attrs {
                let mut parts = attr.splitn(2, ':');
                let (name, param) = match (parts.next(), parts.next()) {
                    (Some(k), Some(v)) => (k.to_string(), v.to_string()),
                    _ => {
                        ("".to_string(), "".to_string())
                    }
                };
                attr_map.entry(name).or_default().push(param);
            }
            let setting_cmds = attr_map.into_iter()
                .map(|(name, mut params)| {
                    params.sort();
                    params.dedup();
                    QueryDevBodyCmdMap {
                        name,
                        params,
                    }
            }).collect::<Vec<QueryDevBodyCmdMap>>();
            QueryDevBody {
                dev_id,
                service_id,
                attrs: None,
                setting_cmds: Some(setting_cmds),
                yk_cmds: None,
            }
        }).collect::<Vec<QueryDevBody>>());
    result.extend(yk_map.into_iter()
        .map(|((dev_id, service_id), mut attrs)| {
            attrs.sort();
            attrs.dedup();
            let yk_cmds = attrs.iter().map(|attr|{
                let mut parts = attr.splitn(2, ':');
                let (name, _) = match (parts.next(), parts.next()) {
                    (Some(k), Some(v)) => (k.to_string(), v.to_string()),
                    _ => {
                        ("".to_string(), "".to_string())
                    }
                };
                name
            }).collect::<Vec<String>>();
            QueryDevBody {
                dev_id,
                service_id,
                attrs: None,
                setting_cmds: None,
                yk_cmds: Some(yk_cmds),
            }
        }).collect::<Vec<QueryDevBody>>());
    result
}

pub fn build_dev_mapping(devs: &Vec<QueryDevResponseBody>) -> HashMap<(String, String, String), (String, String, String)> {
    let mut result = HashMap::new();
    for data in devs {
        for dev in &data.devs {
            if let Some(attrs) = &dev.attrs {
                for attr in attrs {
                    result.insert(
                        (data.dev_id.clone(), data.service_id.clone(), attr.iot.clone()),
                        (dev.dev_guid.clone(), dev.model.clone(), attr.dc.clone())
                    );
                }
            }
            if let Some(setting_cmds) = &dev.setting_cmds {
                for setting_cmd in setting_cmds {
                    for param in &setting_cmd.params {
                        result.insert(
                            (data.dev_id.clone(), data.service_id.clone(), format!("{}:{}", setting_cmd.name.clone(), param.iot.clone())),
                            (dev.dev_guid.clone(), dev.model.clone(), param.dc.clone())
                        );
                    }
                }
            }
            if let Some(yk_cmds) = &dev.yk_cmds {
                for yk_cmd in yk_cmds {
                    result.insert(
                        (data.dev_id.clone(), data.service_id.clone(), format!("{}:cmd", yk_cmd.iot.clone())),
                        (dev.dev_guid.clone(), dev.model.clone(), yk_cmd.dc.clone())
                    );
                }
            }
        }
    }
    result
}

pub async fn do_meter_data_query() -> Result<(), AdapterErr> {
    let sched = JobScheduler::new().await.unwrap();
    let job = Job::new_async("0 0 1 * * *", |_uuid, _l| {
        Box::pin(async move {
            let env = Env::get_env(ADAPTER_NAME);
            let app_name = env.get_app_name();
            let meter_sum_no = env.get_meter_sum_no();
            let meter_dir = env.get_meter_dir();
            if let Ok(meter_dev_addr) = query_meter_dev().await {
                let dev_guids = meter_dev_addr.keys().cloned().collect::<Vec<_>>();
                let body = generate_query_meter_history(dev_guids);
                match mqtt_acquirer::<_, ResponseHistory>(
                    "mems_query_history_data".to_string(),
                    format!("/sys.dbc/{app_name}/S-dataservice/F-GetFrozenData"),
                    format!("/{app_name}/sys.dbc/S-dataservice/F-GetFrozenData"),
                    body,
                ).await {
                    Ok(msg) => {
                        // let mut meter_nos = vec![];
                        let mut timestamps = vec![];
                        let mut meter_data = HashMap::new();
                        for data_body in msg.body {
                            if let Some(addr) = meter_dev_addr.get(&data_body.dev) {
                                for measures in data_body.body {
                                    // meter_nos.push(addr.clone());
                                    if !timestamps.contains(&measures.timestamp) {
                                        timestamps.push(measures.timestamp.clone());
                                    }
                                    if let Some(measure) = measures.body.first() {
                                        meter_data.insert((addr.clone(), measures.timestamp), measure.val.clone());
                                    }
                                }
                            }
                        }
                        let csv = export_meter_csv(
                            &meter_sum_no,
                            timestamps,
                            meter_data,
                        );
                        let mut meter_data_file = File::create(&format!("{meter_dir}/meter_data.csv")).unwrap();
                        meter_data_file.write_all(csv.as_bytes()).unwrap();
                    }
                    Err(e) => {
                        log::error!("do mems_query_history_data error: {}", e.msg);
                    }
                }
            }
        })
    }).unwrap();
    sched.add(job).await.unwrap();
    sched.start().await.unwrap();
    Ok(())

    // let id = job.guid();
    // sched.remove(&id).await.unwrap();
}

pub async fn query_meter_dev() -> Result<HashMap<String, String>, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let app_name = env.get_app_name();
    let body = generate_query_meter_dev();
    match mqtt_acquirer::<_, RegisterDevResult>(
        "mems_meter_dev".to_string(),
        format!("/sys.dbc/{app_name}/S-dataservice/F-GetRegister"),
        format!("/{app_name}/sys.dbc/S-dataservice/F-GetRegister"),
        body,
    ).await {
        Ok(msg) => {
            let mut addr_guid_map = HashMap::new();
            for dev_body in msg.body {
                if dev_body.model == MODEL_FROZEN_METER {
                    for dev in dev_body.body {
                        addr_guid_map.insert(dev.dev, dev.addr);
                    }
                }
            }
            Ok(addr_guid_map)
        }
        Err(e) => Err(AdapterErr {
            code: e.code,
            msg: format!("查询表计dev失败，{}", e.msg),
        }),
    }
}

// pub async fn do_meter_guid_query() -> Result<ResponseGuid, AdapterErr> {
//     let env = Env::get_env(ADAPTER_NAME);
//     let app_name = env.get_app_name();
//     let meter_addrs = vec!["0601020005622971".to_string(), "0601020005622972".to_string()];
//     let body = generate_query_meter_guid(meter_addrs);
//     match mqtt_acquirer::<_, ResponseGuid>(
//         "get_guid_register".to_string(),
//         format!("/sys.dbc/{app_name}/S-dataservice/F-GetGUID"),
//         format!("/{app_name}/sys.dbc/S-dataservice/F-GetGUID"),
//         body,
//     ).await {
//         Ok(msg) => {
//             Ok(msg)
//         }
//         Err(e) => Err(AdapterErr {
//             code: e.code,
//             msg: format!("获取表计guid失败，{}", e.msg),
//         }),
//     }
// }

pub async fn mqtt_acquirer<T, R>(
    mqtt_usr_name: String,
    topic_request: String,
    topic_response: String,
    body: T,
) -> Result<R, AdapterErr>
where
    T: Serialize + HasToken,
    R: DeserializeOwned + HasToken + Send + 'static,
{
    let env = Env::get_env(ADAPTER_NAME);
    let mqtt_server = env.get_mqtt_server();
    let mqtt_server_port = env.get_mqtt_server_port();
    let mqtt_timeout = env.get_mqtt_timeout();
    let mqttoptions = get_mqttoptions(&mqtt_usr_name, &mqtt_server, mqtt_server_port);
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    let payload = serde_json::to_string(&body).unwrap();
    client_subscribe(&client, &topic_response).await?;
    client_publish(&client, &topic_request, &payload).await?;
    let (tx, rx) = oneshot::channel::<Result<R, AdapterErr>>();
    let token = body.token();
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Incoming::Publish(p))) => {
                    if p.topic == topic_response {
                        let result = match serde_json::from_slice::<R>(&p.payload) {
                            Ok(r) => {
                                if r.token() == token {
                                    // todo 只有相同token才能认定是当前消息响应
                                }
                                tx.send(Ok(r))
                            }
                            Err(e) => tx.send(Err(AdapterErr {
                                code: ErrCode::DataJsonDeserializeErr,
                                msg: format!("解析MQTT返回字符串失败: {e:?}"),
                            })),
                        };
                        if result.is_err() {
                            log::error!("MQTT响应发送失败");
                        }
                        break;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    let _ = tx.send(Err(AdapterErr {
                        code: ErrCode::MqttConnectErr,
                        msg: format!("MQTT连接发生错误: {e:?}"),
                    }));
                    break;
                }
            }
        }
    });
    match timeout(Duration::from_secs(mqtt_timeout), rx).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err(AdapterErr {
            code: ErrCode::MqttTimeoutErr,
            msg: "等待响应时发生错误".to_string(),
        }),
        Err(_) => Err(AdapterErr {
            code: ErrCode::MqttTimeoutErr,
            msg: "MQTT响应超时".to_string(),
        }),
    }
}

pub async fn mqtt_provider<F, Resp>(
    mqtt_usr_name: String,
    topic_request: String,
    topic_response: String,
    callback: F,
) -> Result<(), AdapterErr>
where
    Resp: Serialize + HasToken + Sync + Send + 'static,
    F: Fn(actix_web::web::Bytes)
            -> Pin<Box<dyn Future<Output = Resp> + Send + 'static>>
        + Send
        + Sync
        + 'static,
{
    let env = Env::get_env(ADAPTER_NAME);
    let mqtt_server = env.get_mqtt_server();
    let mqtt_server_port = env.get_mqtt_server_port();
    let mqttoptions = get_mqttoptions(&mqtt_usr_name, &mqtt_server, mqtt_server_port);
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    client_subscribe(&client, &topic_response).await?;
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Incoming::Publish(p))) => {
                    if p.topic == topic_response {
                        let data = callback(p.payload).await;
                        let response = serde_json::to_string(&data).unwrap();
                        let _ = client_publish(&client, &topic_request, &response).await;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    log::error!("do mqtt_provider error: {e:?}");
                    break;
                }
            }
        }
    });
    Ok(())
}

pub async fn mqtt_push_only<T>(
    mqtt_usr_name: String,
    topic_request: String,
    topic_response: String,
    body: T,
) -> Result<(), AdapterErr>
where
    T: Serialize + HasToken,
{
    let env = Env::get_env(ADAPTER_NAME);
    let mqtt_server = env.get_mqtt_server();
    let mqtt_server_port = env.get_mqtt_server_port();
    let mqttoptions = get_mqttoptions(&mqtt_usr_name, &mqtt_server, mqtt_server_port);
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    let payload = serde_json::to_string(&body).unwrap();
    client_subscribe(&client, &topic_response).await?;
    client_publish(&client, &topic_request, &payload).await?;
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Incoming::Publish(p))) => {
                    if p.topic == topic_response {
                        break;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    log::error!("do mqtt_push_only error: {e:?}");
                    break;
                }
            }
        }
    });
    Ok(())
}

#[tokio::test]
async fn test_mqtt_response() {
    Env::init(ADAPTER_NAME);
    let env = Env::get_env(ADAPTER_NAME);
    let app_name = env.get_app_name();
    let mqtt_server = env.get_mqtt_server();
    let mqtt_server_port = env.get_mqtt_server_port();
    let mqttoptions = get_mqttoptions("my_test", &mqtt_server, mqtt_server_port);
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    // 启动 event loop 的异步任务（用于保持连接和接收消息）
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(_) => {}
                Err(e) => eprintln!("MQTT 事件循环错误: {:?}", e),
            }
        }
    });
    // 循环发送消息
    loop {
        actix_rt::time::sleep(Duration::from_millis(1000)).await;
        let tp = format!("/{app_name}/sys.dbc/S-dataservice/F-SetModel");
        let body = serde_json::to_string(&RegisterResponse {
            token: "".to_string(),
            time: "".to_string(),
            ack: "true".to_string(),
        }).unwrap();
        let _ = client_publish(&client, &tp, &body).await;

        let tp = format!("/{app_name}/sys.dbc/S-dataservice/F-Register");
        let body = serde_json::to_string(&RegisterResponse {
            token: "".to_string(),
            time: "".to_string(),
            ack: "true".to_string(),
        }).unwrap();
        let _ = client_publish(&client, &tp, &body).await;

        let tp = format!("/{app_name}/sys.iot/S-otaservice/F-GetDCAttr");
        let body = serde_json::to_string(&QueryDevResponse {
            token: "".to_string(),
            time: "".to_string(),
            devices: vec![
                QueryDevResponseBody {
                    dev_id: "FE80-3728-DF9B-6076-3872-7525-9B31-F77F".to_string(),
                    service_id: "serviceSettings".to_string(),
                    devs: vec![
                        QueryDevResponseBodyDev { dev_guid: "guid1".to_string(), addr: "".to_string(), model: "model1".to_string(), desc: "".to_string(), port: "".to_string(), 
                            setting_cmds: Some(vec![
                                QueryDevResponseBodySettingCmd{ name: "tgLimitPower".to_string(), params: vec![QueryDevResponseBodyMap { iot: "tgGun1Power".to_string(), dc: "Gun1Power".to_string() }] }
                            ]),
                            not_found: Some(vec![]), reason: Some("".to_string()),
                            attrs: None, yk_cmds: None }
                    ],
                },
                QueryDevResponseBody {
                    dev_id: "FE80-4D2F-E64F-B696-5206-26A0-4B37-DD6E".to_string(),
                    service_id: "serviceYC2".to_string(),
                    devs: vec![
                        QueryDevResponseBodyDev { dev_guid: "guid2".to_string(), addr: "".to_string(), model: "model2".to_string(), desc: "".to_string(), port: "".to_string(),
                            attrs: Some(vec![QueryDevResponseBodyMap { iot: "tgP".to_string(), dc: "P".to_string() }]), not_found: Some(vec![]), reason: Some("".to_string()),
                            setting_cmds: None, yk_cmds: None }
                    ],
                },
                QueryDevResponseBody {
                    dev_id: "FE80-4D2F-E64F-B696-5206-26A0-4B37-DD6E".to_string(),
                    service_id: "serviceYK".to_string(),
                    devs: vec![
                        QueryDevResponseBodyDev { dev_guid: "guid2".to_string(), addr: "".to_string(), model: "model2".to_string(), desc: "".to_string(), port: "".to_string(),
                            yk_cmds: Some(vec![QueryDevResponseBodyMap { iot: "tgYKChannel1".to_string(), dc: "YKChannel1".to_string() }]), not_found: Some(vec![]), reason: Some("".to_string()),
                            setting_cmds: None, attrs: None }
                    ],
                },
                QueryDevResponseBody {
                    dev_id: "FE80-90D1-B2E2-5A07-B94D-FDA2-80E0-939A".to_string(),
                    service_id: "serviceSettings".to_string(),
                    devs: vec![
                        QueryDevResponseBodyDev { dev_guid: "guid3".to_string(), addr: "".to_string(), model: "model1".to_string(), desc: "".to_string(), port: "".to_string(),
                            attrs: Some(vec![QueryDevResponseBodyMap { iot: "tgPlimit".to_string(), dc: "Plimit".to_string() }]), not_found: Some(vec![]), reason: Some("".to_string()),
                            setting_cmds: None, yk_cmds: None }
                    ],
                },
                QueryDevResponseBody {
                    dev_id: "FE80-90D1-B2E2-5A07-B94D-FDA2-80E0-939A".to_string(),
                    service_id: "servicePVInput".to_string(),
                    devs: vec![
                        QueryDevResponseBodyDev { dev_guid: "guid3".to_string(), addr: "".to_string(), model: "model1".to_string(), desc: "".to_string(), port: "".to_string(),
                            attrs: Some(vec![QueryDevResponseBodyMap { iot: "tgInP".to_string(), dc: "InP".to_string() }]), not_found: Some(vec![]), reason: Some("".to_string()),
                            setting_cmds: None, yk_cmds: None }
                    ],
                },
            ],
        }).unwrap();
        let _ = client_publish(&client, &tp, &body).await;

        let tp = format!("/{app_name}/sys.dbc/S-dataservice/F-GetRealData");
        let body = serde_json::to_string(&RegisterResponse {
            token: "".to_string(),
            time: "".to_string(),
            ack: "true".to_string(),
        }).unwrap();
        let _ = client_publish(&client, &tp, &body).await;

        let tp = format!("/{app_name}/sys.appman/S-appmanager/F-KeepAlive");
        let body = serde_json::to_string(&KeepAliveResponse {
            token: "".to_string(),
            time: "".to_string(),
            ack: "true".to_string(),
            errmsg: "".to_string(),
        }).unwrap();
        let _ = client_publish(&client, &tp, &body).await;

        let tp = format!("/{app_name}/sys.dbc/S-dataservice/F-GetRegister");
        let body = serde_json::to_string(&RegisterDevResult {
            token: "".to_string(),
            time: "".to_string(),
            body: vec![
                RegisterDevResultBody {
                    model: "DC_PLCC".to_string(),
                    port: "".to_string(),
                    body: vec![RegisterDevEntry { addr: "".to_string(), appname: "".to_string(), desc: "".to_string(), dev: "DC_SDTTU_frozen_1".to_string(), 
                        device_type: "".to_string(), guid: "".to_string(), is_report: "".to_string(), manu_id: "".to_string(), 
                        manu_name: "".to_string(), node_id: "".to_string(), pro_type: "".to_string(), product_id: "".to_string() }
                    ]
                },
                RegisterDevResultBody {
                    model: MODEL_FROZEN_METER.to_string(),
                    port: "".to_string(),
                    body: vec![
                        RegisterDevEntry { addr: "0000000000000000".to_string(), appname: "".to_string(), desc: "".to_string(), dev: "DC_Meter_frozen_176".to_string(), 
                            device_type: "".to_string(), guid: "176".to_string(), is_report: "".to_string(), manu_id: "".to_string(), 
                            manu_name: "".to_string(), node_id: "".to_string(), pro_type: "".to_string(), product_id: "".to_string()
                        },
                        RegisterDevEntry { addr: "0601020021015579".to_string(), appname: "".to_string(), desc: "".to_string(), dev: "DC_Meter_frozen_173".to_string(), 
                            device_type: "".to_string(), guid: "173".to_string(), is_report: "".to_string(), manu_id: "".to_string(), 
                            manu_name: "".to_string(), node_id: "".to_string(), pro_type: "".to_string(), product_id: "".to_string()
                        },
                        RegisterDevEntry { addr: "0601196044360269".to_string(), appname: "".to_string(), desc: "".to_string(), dev: "DC_Meter_frozen_174".to_string(), 
                            device_type: "".to_string(), guid: "174".to_string(), is_report: "".to_string(), manu_id: "".to_string(), 
                            manu_name: "".to_string(), node_id: "".to_string(), pro_type: "".to_string(), product_id: "".to_string()
                        },
                    ]
                },
            ],
        }).unwrap();
        let _ = client_publish(&client, &tp, &body).await;

        let tp = format!("/ext.syy.phSmc/{app_name}/S-smclink/F-PlccEvent");
        let body = serde_json::to_string(&CloudEventResponse {
            token: "123456".to_string(),
            request_id: "cac99431-94ee-40e2-a0e8-fbdd6ceacabe".to_string(),
            time: "2025-08-25T10:49:24+8:00".to_string(),
            msg_info: "".to_string(),
            data: CloudEventResponseBody {
                points: None,
                transports: None,
                aoes: None,
                aoes_status: Some(vec![CloudEventAoeStatus {
                    aoe_id: 1955881650638458880,
                    aoe_status: 0,
                },CloudEventAoeStatus {
                    aoe_id: 1955881650631516321,
                    aoe_status: 0,
                }]),
                code: ErrCode::Success,
                msg: "".to_string(),
            },
        }).unwrap();
        let _ = client_publish(&client, &tp, &body).await;

        let tp = format!("/{app_name}/sys.dbc/S-dataservice/F-GetModel");
        let body = serde_json::to_string(&GetModelResponse {
            token: "".to_string(),
            time: "".to_string(),
            body: vec![GetModelResponseBody {
                model: "DC_PLCC".to_string(),
                body: vec![]
            }],
        }).unwrap();
        let _ = client_publish(&client, &tp, &body).await;

        let tp = format!("/{app_name}/sys.dbc/S-dataservice/F-GetFrozenData");
        let body = serde_json::to_string(&ResponseHistory {
            token: "".to_string(),
            time: "".to_string(),
            body: vec![
                ResponseHistoryBody {
                    dev: "DC_Meter_frozen_176".to_string(),
                    body: vec![
                        ResponseHistoryData {
                            timestamp: "2025-11-23T00:00:00.000+0800".to_string(),
                            timestartgather: "2025-11-23T00:00:00.000+0800".to_string(),
                            timeendgather: "2025-11-23T00:00:00.000+0800".to_string(),
                            additionalcheck: "".to_string(),
                            body: vec![ResponseHistoryMeasure {
                                name: "".to_string(),
                                val: "50".to_string(),
                            }],
                        },
                        ResponseHistoryData {
                            timestamp: "2025-11-24T00:00:00.000+0800".to_string(),
                            timestartgather: "2025-11-24T00:00:00.000+0800".to_string(),
                            timeendgather: "2025-11-24T00:00:00.000+0800".to_string(),
                            additionalcheck: "".to_string(),
                            body: vec![ResponseHistoryMeasure {
                                name: "".to_string(),
                                val: "60".to_string(),
                            }]
                        },
                        ResponseHistoryData {
                            timestamp: "2025-11-25T00:00:00.000+0800".to_string(),
                            timestartgather: "2025-11-25T00:00:00.000+0800".to_string(),
                            timeendgather: "2025-11-25T00:00:00.000+0800".to_string(),
                            additionalcheck: "".to_string(),
                            body: vec![ResponseHistoryMeasure {
                                name: "".to_string(),
                                val: "50".to_string(),
                            }],
                        },
                        ResponseHistoryData {
                            timestamp: "2025-11-26T00:00:00.000+0800".to_string(),
                            timestartgather: "2025-11-26T00:00:00.000+0800".to_string(),
                            timeendgather: "2025-11-26T00:00:00.000+0800".to_string(),
                            additionalcheck: "".to_string(),
                            body: vec![ResponseHistoryMeasure {
                                name: "".to_string(),
                                val: "60".to_string(),
                            }]
                        },
                    ]
                },
                ResponseHistoryBody {
                    dev: "DC_Meter_frozen_173".to_string(),
                    body: vec![
                        ResponseHistoryData {
                            timestamp: "2025-11-23T00:00:00.000+0800".to_string(),
                            timestartgather: "2025-11-23T00:00:00.000+0800".to_string(),
                            timeendgather: "2025-11-23T00:00:00.000+0800".to_string(),
                            additionalcheck: "".to_string(),
                            body: vec![ResponseHistoryMeasure {
                                name: "".to_string(),
                                val: "15.5".to_string(),
                            }],
                        },
                        ResponseHistoryData {
                            timestamp: "2025-11-24T00:00:00.000+0800".to_string(),
                            timestartgather: "2025-11-24T00:00:00.000+0800".to_string(),
                            timeendgather: "2025-11-24T00:00:00.000+0800".to_string(),
                            additionalcheck: "".to_string(),
                            body: vec![ResponseHistoryMeasure {
                                name: "".to_string(),
                                val: "16.6".to_string(),
                            }]
                        },
                        ResponseHistoryData {
                            timestamp: "2025-11-25T00:00:00.000+0800".to_string(),
                            timestartgather: "2025-11-25T00:00:00.000+0800".to_string(),
                            timeendgather: "2025-11-25T00:00:00.000+0800".to_string(),
                            additionalcheck: "".to_string(),
                            body: vec![ResponseHistoryMeasure {
                                name: "".to_string(),
                                val: "15.5".to_string(),
                            }],
                        },
                        ResponseHistoryData {
                            timestamp: "2025-11-26T00:00:00.000+0800".to_string(),
                            timestartgather: "2025-11-26T00:00:00.000+0800".to_string(),
                            timeendgather: "2025-11-26T00:00:00.000+0800".to_string(),
                            additionalcheck: "".to_string(),
                            body: vec![ResponseHistoryMeasure {
                                name: "".to_string(),
                                val: "16.6".to_string(),
                            }]
                        },
                    ]
                },
                ResponseHistoryBody {
                    dev: "DC_Meter_frozen_174".to_string(),
                    body: vec![
                        ResponseHistoryData {
                            timestamp: "2025-11-23T00:00:00.000+0800".to_string(),
                            timestartgather: "2025-11-23T00:00:00.000+0800".to_string(),
                            timeendgather: "2025-11-23T00:00:00.000+0800".to_string(),
                            additionalcheck: "".to_string(),
                            body: vec![ResponseHistoryMeasure {
                                name: "".to_string(),
                                val: "25.5".to_string(),
                            }],
                        },
                        ResponseHistoryData {
                            timestamp: "2025-11-24T00:00:00.000+0800".to_string(),
                            timestartgather: "2025-11-24T00:00:00.000+0800".to_string(),
                            timeendgather: "2025-11-24T00:00:00.000+0800".to_string(),
                            additionalcheck: "".to_string(),
                            body: vec![ResponseHistoryMeasure {
                                name: "".to_string(),
                                val: "26.6".to_string(),
                            }]
                        },
                        ResponseHistoryData {
                            timestamp: "2025-11-25T00:00:00.000+0800".to_string(),
                            timestartgather: "2025-11-25T00:00:00.000+0800".to_string(),
                            timeendgather: "2025-11-25T00:00:00.000+0800".to_string(),
                            additionalcheck: "".to_string(),
                            body: vec![ResponseHistoryMeasure {
                                name: "".to_string(),
                                val: "25.5".to_string(),
                            }],
                        },
                        ResponseHistoryData {
                            timestamp: "2025-11-26T00:00:00.000+0800".to_string(),
                            timestartgather: "2025-11-26T00:00:00.000+0800".to_string(),
                            timeendgather: "2025-11-26T00:00:00.000+0800".to_string(),
                            additionalcheck: "".to_string(),
                            body: vec![ResponseHistoryMeasure {
                                name: "".to_string(),
                                val: "26.6".to_string(),
                            }]
                        },
                    ]
                },
            ],
        }).unwrap();
        let _ = client_publish(&client, &tp, &body).await;


        
    }
}
