use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use tokio::time::{timeout, Duration};
use tokio::sync::oneshot;
use chrono::{Local, TimeZone};

use crate::model::north::MyPbAoeResult;
use crate::utils::register_result;
use crate::{AdapterErr, ErrCode, ADAPTER_NAME};
use crate::model::datacenter::*;
use crate::env::Env;
use crate::utils::localapi::query_dev_mapping;

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

pub async fn do_query_dev(dev_ids: Vec<String>) -> Result<Vec<QueryDevResponseBody>, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mqtt_server = env.get_mqtt_server();
    let mqtt_server_port = env.get_mqtt_server_port();
    let mqtt_timeout = env.get_mqtt_timeout();
    let app_name = env.get_app_name();
    let mut mqttoptions = MqttOptions::new("plcc_query_dev", &mqtt_server, mqtt_server_port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    // mqttoptions.set_credentials("username", "password");
    let topic_request_query_dev = format!("/ext.syy.subota/{app_name}/S-otaservice/F-GetNodeInfo");
    let topic_response_query_dev = format!("/{app_name}/ext.syy.subota/S-otaservice/F-GetNodeInfo");
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    // 订阅查询消息返回
    client_subscribe(&client, &topic_response_query_dev).await?;
    // 发布查询消息
    let body = generate_query_dev(dev_ids);
    let payload = serde_json::to_string(&body).unwrap();
    client_publish(&client, &topic_request_query_dev, &payload).await?;
    // 处理订阅消息
    let (tx, rx) = oneshot::channel::<Result<Vec<QueryDevResponseBody>, AdapterErr>>();
    tokio::spawn(async move {
        loop {
            let event = eventloop.poll().await;
            match event {
                Ok(Event::Incoming(Incoming::Publish(p))) => {
                    if p.topic == topic_response_query_dev {
                        let send_result = match serde_json::from_slice::<QueryDevResponse>(&p.payload) {
                            Ok(msg) => {
                                let mut has_error = false;
                                let mut result = vec![];
                                for data in msg.devices.clone() {
                                    let data_clone = data.clone();
                                    if data.status.to_lowercase() == "true".to_string() && data.guid.is_some() {
                                        result.push(data_clone);
                                    } else {
                                        has_error = true;
                                    }
                                }
                                if has_error {
                                    tx.send(Err(AdapterErr {
                                        code: ErrCode::QueryDevStatusErr,
                                        msg: format!("查询设备GUID，返回status为false：: {:?}", msg),
                                    }))
                                } else {
                                    tx.send(Ok(result))
                                }
                            }
                            Err(e) => tx.send(
                                Err(AdapterErr {
                                    code: ErrCode::QueryDevDeserializeErr,
                                    msg: format!("查询设备GUID，返回字符串反序列化失败: {:?}", e),
                                }))
                        };
                        if send_result.is_err() {
                            log::error!("do dev_guid error: receive mqtt massage failed");
                        }
                        break;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    let _ = tx.send(Err(AdapterErr {
                        code: ErrCode::MqttConnectErr,
                        msg: format!("执行dev_guid时mqtt连接失败: {:?}", e),
                    }));
                    break;
                }
            }
        }
    });
    match timeout(Duration::from_secs(mqtt_timeout), rx).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err(AdapterErr {
            code: ErrCode::QueryDevTimeout,
            msg: "查询设备GUID，等待响应失败".to_string(),
        }),
        Err(_) => Err(AdapterErr {
            code: ErrCode::QueryDevTimeout,
            msg: "查询设备GUID超时，未收到MQTT响应".to_string(),
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
    let env = Env::get_env(ADAPTER_NAME);
    let mqtt_server = env.get_mqtt_server();
    let mqtt_server_port = env.get_mqtt_server_port();
    let mqtt_timeout = env.get_mqtt_timeout();
    let app_name = env.get_app_name();
    let app_model = env.get_app_model();
    let mut mqttoptions = MqttOptions::new("plcc_model_register", &mqtt_server, mqtt_server_port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    // mqttoptions.set_credentials("username", "password");
    let topic_request_register = format!("/sys.dbc/{app_name}/S-dataservice/F-SetModel");
    let topic_response_register = format!("/{app_name}/sys.dbc/S-dataservice/F-SetModel");
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    // 订阅注册消息返回
    client_subscribe(&client, &topic_response_register).await?;
    // 发布注册消息
    let body = generate_register_model(app_model);
    let payload = serde_json::to_string(&body).unwrap();
    client_publish(&client, &topic_request_register, &payload).await?;
    // 处理订阅消息
    let (tx, rx) = oneshot::channel::<Result<bool, AdapterErr>>();
    tokio::spawn(async move {
        loop {
            let event = eventloop.poll().await;
            match event {
                Ok(Event::Incoming(Incoming::Publish(p))) => {
                    if p.topic == topic_response_register {
                        let send_result = match serde_json::from_slice::<RegisterResponse>(&p.payload) {
                            Ok(msg) => {
                                let result = msg.ack.to_lowercase() == "true".to_string();
                                tx.send(Ok(result))
                            }
                            Err(e) => tx.send(Err(AdapterErr {
                                code: ErrCode::ModelRegisterErr,
                                msg: format!("注册model失败，解析返回字符串错误: {:?}", e),
                            })),
                        };
                        if send_result.is_err() {
                            log::error!("do register_model error: receive mqtt massage failed");
                        }
                        break;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    let _ = tx.send(Err(AdapterErr {
                        code: ErrCode::ModelRegisterErr,
                        msg: format!("注册model失败，发生错误: {:?}", e),
                    }));
                    break;
                }
            }
        }
    });
    match timeout(Duration::from_secs(mqtt_timeout), rx).await {
        Ok(Ok(result)) => {
            match result {
                Ok(b) => {
                    if !b {
                        return Err(AdapterErr {
                            code: ErrCode::ModelRegisterErr,
                            msg: "注册model失败，响应结果为false".to_string(),
                        });
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Ok(Err(_)) => return Err(AdapterErr {
            code: ErrCode::ModelRegisterErr,
            msg: "注册model，等待响应失败".to_string(),
        }),
        Err(_) => return Err(AdapterErr {
            code: ErrCode::ModelRegisterErr,
            msg: "注册model超时，未收到MQTT响应".to_string(),
        }),
    }
    Ok(())
}

async fn do_register_app() -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mqtt_server = env.get_mqtt_server();
    let mqtt_server_port = env.get_mqtt_server_port();
    let mqtt_timeout = env.get_mqtt_timeout();
    let app_name = env.get_app_name();
    let app_model = env.get_app_model();
    let mut mqttoptions = MqttOptions::new("plcc_app_register", &mqtt_server, mqtt_server_port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    // mqttoptions.set_credentials("username", "password");
    let topic_request_register = format!("/sys.dbc/{app_name}/S-dataservice/F-Register");
    let topic_response_register = format!("/{app_name}/sys.dbc/S-dataservice/F-Register");
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    // 订阅注册消息返回
    client_subscribe(&client, &topic_response_register).await?;
    // 发布注册消息
    let body = generate_register_app(app_model);
    let payload = serde_json::to_string(&body).unwrap();
    client_publish(&client, &topic_request_register, &payload).await?;
    // 处理订阅消息
    let (tx, rx) = oneshot::channel::<Result<bool, AdapterErr>>();
    tokio::spawn(async move {
        loop {
            let event = eventloop.poll().await;
            match event {
                Ok(Event::Incoming(Incoming::Publish(p))) => {
                    if p.topic == topic_response_register {
                        let send_result = match serde_json::from_slice::<RegisterResponse>(&p.payload) {
                            Ok(msg) => {
                                let result = msg.ack.to_lowercase() == "true".to_string();
                                tx.send(Ok(result))
                            }
                            Err(e) => tx.send(Err(AdapterErr {
                                code: ErrCode::AppRegisterErr,
                                msg: format!("注册APP，解析返回字符串失败: {:?}", e),
                            })),
                        };
                        if send_result.is_err() {
                            log::error!("do register_app error: receive mqtt massage failed");
                        }
                        break;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    let _ = tx.send(Err(AdapterErr {
                        code: ErrCode::AppRegisterErr,
                        msg: format!("注册APP，发生错误: {:?}", e),
                    }));
                    break;
                }
            }
        }
    });
    match timeout(Duration::from_secs(mqtt_timeout), rx).await {
        Ok(Ok(result)) => {
            match result {
                Ok(b) => {
                    if !b {
                        return Err(AdapterErr {
                            code: ErrCode::AppRegisterErr,
                            msg: "注册APP失败，响应结果为false".to_string(),
                        });
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Ok(Err(_)) => return Err(AdapterErr {
            code: ErrCode::AppRegisterErr,
            msg: "注册APP，等待响应失败".to_string(),
        }),
        Err(_) => return Err(AdapterErr {
            code: ErrCode::AppRegisterErr,
            msg: "注册APP超时，未收到MQTT响应".to_string(),
        }),
    }
    Ok(())
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
    let env = Env::get_env(ADAPTER_NAME);
    let mqtt_server = env.get_mqtt_server();
    let mqtt_server_port = env.get_mqtt_server_port();
    let app_name = env.get_app_name();
    let mut mqttoptions = MqttOptions::new("plcc_data_query", &mqtt_server, mqtt_server_port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    // mqttoptions.set_credentials("username", "password");
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    let topic_request_query = format!("/sys.dbc/{app_name}/S-dataservice/F-GetRealData");
    let topic_response_query = format!("/{app_name}/sys.dbc/S-dataservice/F-GetRealData");
    let devs = query_dev_mapping().await?;
    if !devs.is_empty() {
        let payload = serde_json::to_string(&generate_query_data(&devs)).unwrap();// 订阅注册消息返回
        client_subscribe(&client, &topic_response_query).await?;
        client_publish(&client, &topic_request_query, &payload).await?;
        tokio::spawn(async move {
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Incoming::Publish(p))) => {
                        if p.topic == topic_response_query {
                            break;
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        log::error!("do data_query error: {:?}", e);
                        break;
                    }
                }
            }
        });
    }
    Ok(())
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
    let mqtt_server = env.get_mqtt_server();
    let mqtt_server_port = env.get_mqtt_server_port();
    let mut mqttoptions = MqttOptions::new("plcc_keep_alive", &mqtt_server, mqtt_server_port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    // mqttoptions.set_credentials("username", "password");
    let topic_request = format!("/sys.appman/{app_name}/S-appmanager/F-KeepAlive");
    let topic_response = format!("/{app_name}/sys.appman/S-appmanager/F-KeepAlive");
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    // 订阅保活主题
    client_subscribe(&client, &topic_response).await?;
    tokio::spawn(async move {
        loop {
            let event = eventloop.poll().await;
            match event {
                Ok(Event::Incoming(Incoming::Publish(p))) => {
                    if let Ok(msg) = serde_json::from_slice::<KeepAliveRequest>(&p.payload) {
                        let response = serde_json::to_string(&generate_keep_alive_response(msg)).unwrap();
                        let _ = client_publish(&client, &topic_request, &response).await;
                    };
                }
                Ok(_) => {}
                Err(e) => {
                    log::error!("do keep_alive error: {:?}", e);
                    break;
                }
            }
        }
    });
    Ok(())
}

pub async fn query_register_dev() -> Result<String, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mqtt_server = env.get_mqtt_server();
    let mqtt_server_port = env.get_mqtt_server_port();
    let mqtt_timeout = env.get_mqtt_timeout();
    let app_name = env.get_app_name();
    let mut mqttoptions = MqttOptions::new("plcc_register_dev", &mqtt_server, mqtt_server_port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    // mqttoptions.set_credentials("username", "password");
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    let topic_request_query = format!("/sys.dbc/{app_name}/S-dataservice/F-GetRegister");
    let topic_response_query = format!("/{app_name}/sys.dbc/S-dataservice/F-GetRegister");
    let payload = serde_json::to_string(&generate_query_register_dev()).unwrap();// 订阅注册消息返回
    client_subscribe(&client, &topic_response_query).await?;
    client_publish(&client, &topic_request_query, &payload).await?;
    // 处理订阅消息
    let (tx, rx) = oneshot::channel::<Result<String, AdapterErr>>();
    tokio::spawn(async move {
        loop {
            let event = eventloop.poll().await;
            match event {
                Ok(Event::Incoming(Incoming::Publish(p))) => {
                    if p.topic == topic_response_query {
                        let send_result = match serde_json::from_slice::<RegisterDevResult>(&p.payload) {
                            Ok(msg) => {
                                let mut dev = "".to_string();
                                if !msg.body.is_empty() {
                                    let first_body = msg.body.first().unwrap();
                                    if !first_body.body.is_empty() {
                                        dev = first_body.body.first().unwrap().dev.clone();
                                    }
                                }
                                tx.send(Ok(dev))
                            }
                            Err(e) => tx.send(Err(AdapterErr {
                                code: ErrCode::QueryRegisterDevErr,
                                msg: format!("查询注册dev，解析返回字符串失败: {:?}", e),
                            })),
                        };
                        if send_result.is_err() {
                            log::error!("do query_register_dev error: receive mqtt massage failed");
                        }
                        break;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    let _ = tx.send(Err(AdapterErr {
                        code: ErrCode::QueryRegisterDevErr,
                        msg: format!("查询注册dev，发生错误: {:?}", e),
                    }));
                    break;
                }
            }
        }
    });
    match timeout(Duration::from_secs(mqtt_timeout), rx).await {
        Ok(Ok(result)) => {
            result
        }
        Ok(Err(_)) => Err(AdapterErr {
            code: ErrCode::QueryRegisterDevErr,
            msg: "查询注册dev，等待响应失败".to_string(),
        }),
        Err(_) => Err(AdapterErr {
            code: ErrCode::QueryRegisterDevErr,
            msg: "查询注册dev超时，未收到MQTT响应".to_string(),
        }),
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

fn generate_query_data(devs: &Vec<QueryDevResponseBody>) -> DataQuery {
    let time = Local::now().timestamp_millis();
    let body = devs.iter().map(|v|
        DataQueryBody {
            dev: format!("{}_{}", v.model.clone().unwrap_or("".to_string()), v.devID),
            totalcall: "1".to_string(),
            body: vec![],
        }
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
        body: vec!["DC_SDTTU_frozen".to_string()],
    }
}

fn generate_query_dev(dev_ids: Vec<String>) -> QueryDev {
    let time = Local::now().timestamp_millis();
    QueryDev {
        token: time.to_string(),
        time: generate_current_time(),
        devices: dev_ids,
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

#[tokio::test]
async fn test_mqtt_response() {
    Env::init(ADAPTER_NAME);
    let env = Env::get_env(ADAPTER_NAME);
    let app_name = env.get_app_name();
    let mqtt_server = env.get_mqtt_server();
    let mqtt_server_port = env.get_mqtt_server_port();
    let mut mqttoptions = MqttOptions::new("my_test", &mqtt_server, mqtt_server_port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
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

        let tp = format!("/{app_name}/ext.syy.subota/S-otaservice/F-GetNodeInfo");
        let body = serde_json::to_string(&QueryDevResponse {
            token: "".to_string(),
            time: "".to_string(),
            devices: vec![
                QueryDevResponseBody { devID: "FE80-3728-DF9B-6076-3872-7525-9B31-F77F".to_string(), status: "true".to_string(),
                    addr: Some("".to_string()), model: Some("".to_string()), desc: Some("".to_string()), port: Some("".to_string()),
                    guid: Some("guid1".to_string()), reason: Some("".to_string()) },
                QueryDevResponseBody { devID: "FE80-4D2F-E64F-B696-5206-26A0-4B37-DD6E".to_string(), status: "true".to_string(),
                    addr: Some("".to_string()), model: Some("".to_string()), desc: Some("".to_string()), port: Some("".to_string()),
                    guid: Some("guid2".to_string()), reason: Some("".to_string()) },
                QueryDevResponseBody { devID: "FE80-90D1-B2E2-5A07-B94D-FDA2-80E0-939A".to_string(), status: "true".to_string(),
                    addr: Some("".to_string()), model: Some("".to_string()), desc: Some("".to_string()), port: Some("".to_string()),
                    guid: Some("guid3".to_string()), reason: Some("".to_string()) }
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
            body: vec![RegisterDevResultBody {
                model: "".to_string(),
                port: "".to_string(),
                body: vec![DeviceEntry { addr: "".to_string(), appname: "".to_string(), desc: "".to_string(), dev: "app_dev".to_string(), 
                    device_type: "".to_string(), guid: "".to_string(), isReport: "".to_string(), manu_id: "".to_string(), 
                    manu_name: "".to_string(), node_id: "".to_string(), pro_type: "".to_string(), product_id: "".to_string() }
                ]
            }],
        }).unwrap();
        let _ = client_publish(&client, &tp, &body).await;
    }
}
