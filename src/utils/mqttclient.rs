use std::fs::File;
use std::io::BufReader;
use std::collections::{HashSet, HashMap};
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use tokio::time::{timeout, Duration};
use tokio::sync::oneshot;
use chrono::{Local, TimeZone};

use crate::{AdapterErr, ErrCode, ADAPTER_NAME};
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
    let mqtt_server = env.get_mqtt_server();
    let mqtt_server_port = env.get_mqtt_server_port();
    let mqtt_timeout = env.get_mqtt_timeout();
    let app_name = env.get_app_name();
    let mqttoptions = get_mqttoptions("plcc_query_dev", &mqtt_server, mqtt_server_port);
    let topic_request_query_dev = format!("/sys.iot/{app_name}/S-otaservice/F-GetDCAttr");
    let topic_response_query_dev = format!("/{app_name}/sys.iot/S-otaservice/F-GetDCAttr");
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    // 订阅查询消息返回
    client_subscribe(&client, &topic_response_query_dev).await?;
    // 发布查询消息
    let query_dev_bodys = build_query_dev_bodys(transports);
    let body = generate_query_dev(query_dev_bodys);
    let payload = serde_json::to_string(&body).unwrap();
    log::info!("do dev_guid mqtt send: {}", payload);
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
                                    for dev in &data.devs {
                                        if data.service_id.as_str() == "serviceNotExist" || dev.not_found.clone().is_some_and(|x|!x.is_empty()) {
                                            has_error = true;
                                            break;
                                        }
                                    }
                                    result.push(data);
                                }
                                if has_error {
                                    tx.send(Err(AdapterErr {
                                        code: ErrCode::QueryDevAttrNotFound,
                                        msg: format!("查询设备信息报错，有部分属性在数据中心未找到：{:?}", msg),
                                    }))
                                } else {
                                    tx.send(Ok(result))
                                }
                            }
                            Err(e) => tx.send(
                                Err(AdapterErr {
                                    code: ErrCode::QueryDevDeserializeErr,
                                    msg: format!("查询设备信息报错，返回字符串反序列化失败: {:?}", e),
                                }))
                        };
                        if send_result.is_err() {
                            log::error!("do query_dev_info error: receive mqtt massage failed");
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
            msg: "查询南向设备信息，等待响应失败".to_string(),
        }),
        Err(_) => Err(AdapterErr {
            code: ErrCode::QueryDevTimeout,
            msg: "查询南向设备信息超时，未收到MQTT响应".to_string(),
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
    let mqtt_server = env.get_mqtt_server();
    let mqtt_server_port = env.get_mqtt_server_port();
    let mqtt_timeout = env.get_mqtt_timeout();
    let app_name = env.get_app_name();
    let app_model = env.get_app_model();
    let mqttoptions = get_mqttoptions("plcc_model_register", &mqtt_server, mqtt_server_port);
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

async fn get_has_model_registered() -> Result<bool, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mqtt_server = env.get_mqtt_server();
    let mqtt_server_port = env.get_mqtt_server_port();
    let mqtt_timeout = env.get_mqtt_timeout();
    let app_name = env.get_app_name();
    let app_model = env.get_app_model();
    let mqttoptions = get_mqttoptions("get_model_register", &mqtt_server, mqtt_server_port);
    let topic_request_register = format!("/sys.dbc/{app_name}/S-dataservice/F-GetModel");
    let topic_response_register = format!("/{app_name}/sys.dbc/S-dataservice/F-GetModel");
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    // 订阅注册消息返回
    client_subscribe(&client, &topic_response_register).await?;
    // 发布注册消息
    let body = generate_get_model_register(app_model.clone());
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
                        let send_result = match serde_json::from_slice::<GetModelResponse>(&p.payload) {
                            Ok(msg) => {
                                let mut has_registered = false;
                                for msg_body in msg.body {
                                    if msg_body.model == app_model {
                                        has_registered = true;
                                        break;
                                    }
                                }
                                tx.send(Ok(has_registered))
                            }
                            Err(e) => tx.send(Err(AdapterErr {
                                code: ErrCode::ModelRegisterErr,
                                msg: format!("获取model注册信息失败，解析返回字符串错误: {:?}", e),
                            })),
                        };
                        if send_result.is_err() {
                            log::error!("do get_has_model_registered error: receive mqtt massage failed");
                        }
                        break;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    let _ = tx.send(Err(AdapterErr {
                        code: ErrCode::ModelRegisterErr,
                        msg: format!("获取model注册信息失败，发生错误: {:?}", e),
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
                    return Ok(b);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(Err(_)) => return Err(AdapterErr {
            code: ErrCode::ModelRegisterErr,
            msg: "获取model注册信息，等待响应失败".to_string(),
        }),
        Err(_) => return Err(AdapterErr {
            code: ErrCode::ModelRegisterErr,
            msg: "获取model注册信息超时，未收到MQTT响应".to_string(),
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
    let mqtt_server = env.get_mqtt_server();
    let mqtt_server_port = env.get_mqtt_server_port();
    let mqtt_timeout = env.get_mqtt_timeout();
    let app_name = env.get_app_name();
    let app_model = env.get_app_model();
    let mqttoptions = get_mqttoptions("plcc_app_register", &mqtt_server, mqtt_server_port);
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

async fn get_has_app_registered() -> Result<bool, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mqtt_server = env.get_mqtt_server();
    let mqtt_server_port = env.get_mqtt_server_port();
    let mqtt_timeout = env.get_mqtt_timeout();
    let app_name = env.get_app_name();
    let app_model = env.get_app_model();
    let mqttoptions = get_mqttoptions("get_app_register", &mqtt_server, mqtt_server_port);
    let topic_request_register = format!("/sys.dbc/{app_name}/S-dataservice/F-GetRegister");
    let topic_response_register = format!("/{app_name}/sys.dbc/S-dataservice/F-GetRegister");
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    // 订阅注册消息返回
    client_subscribe(&client, &topic_response_register).await?;
    // 发布注册消息
    let body = generate_get_app_register(app_model.clone());
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
                        let send_result = match serde_json::from_slice::<RegisterDevResult>(&p.payload) {
                            Ok(msg) => {
                                let mut has_registered = false;
                                for msg_body in msg.body {
                                    if msg_body.model == app_model {
                                        has_registered = true;
                                        break;
                                    }
                                }
                                tx.send(Ok(has_registered))
                            }
                            Err(e) => tx.send(Err(AdapterErr {
                                code: ErrCode::ModelRegisterErr,
                                msg: format!("获取app注册信息失败，解析返回字符串错误: {:?}", e),
                            })),
                        };
                        if send_result.is_err() {
                            log::error!("do get_has_app_registered error: receive mqtt massage failed");
                        }
                        break;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    let _ = tx.send(Err(AdapterErr {
                        code: ErrCode::ModelRegisterErr,
                        msg: format!("获取app注册信息失败，发生错误: {:?}", e),
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
                    return Ok(b);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(Err(_)) => return Err(AdapterErr {
            code: ErrCode::ModelRegisterErr,
            msg: "获取app注册信息，等待响应失败".to_string(),
        }),
        Err(_) => return Err(AdapterErr {
            code: ErrCode::ModelRegisterErr,
            msg: "获取app注册信息超时，未收到MQTT响应".to_string(),
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
    let env = Env::get_env(ADAPTER_NAME);
    let mqtt_server = env.get_mqtt_server();
    let mqtt_server_port = env.get_mqtt_server_port();
    let app_name = env.get_app_name();
    let mqttoptions = get_mqttoptions("plcc_data_query", &mqtt_server, mqtt_server_port);
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
    let mqttoptions = get_mqttoptions("plcc_keep_alive", &mqtt_server, mqtt_server_port);
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
    let mqttoptions = get_mqttoptions("plcc_register_dev", &mqtt_server, mqtt_server_port);
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
    let mqtt_server = env.get_mqtt_server();
    let mqtt_server_port = env.get_mqtt_server_port();
    let mqttoptions = get_mqttoptions("plcc_event", &mqtt_server, mqtt_server_port);
    // mqttoptions.set_credentials("username", "password");
    let topic_request = format!("/ext.syy.phSmc/{app_name}/S-smclink/F-PlccEvent");
    let topic_response = format!("/{app_name}/ext.syy.phSmc/S-smclink/F-PlccEvent");
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    // 订阅魔方消息主题
    client_subscribe(&client, &topic_response).await?;
    tokio::spawn(async move {
        loop {
            let event = eventloop.poll().await;
            match event {
                Ok(Event::Incoming(Incoming::Publish(p))) => {
                    if let Ok(msg) = serde_json::from_slice::<CloudEventRequest>(&p.payload) {
                        match msg.cmd {
                            CloudEventCmd::GetTgPLCCConfig => {
                                let response = serde_json::to_string(&do_get_plcc_config(msg)).unwrap();
                                let _ = client_publish(&client, &topic_request, &response).await;
                            },
                            CloudEventCmd::TgAOEControl => {
                                let response = serde_json::to_string(&do_aoe_control(msg).await).unwrap();
                                let _ = client_publish(&client, &topic_request, &response).await;
                            },
                            CloudEventCmd::GetTgAOEStatus => {
                                let response = serde_json::to_string(&do_get_aoe_status(msg).await).unwrap();
                                let _ = client_publish(&client, &topic_request, &response).await;
                            }
                        }
                    } else {
                        let time = Local::now().timestamp_millis();
                        let data = get_aoe_status_body(None, ErrCode::DataJsonDeserializeErr, "Json格式错误".to_string());
                        let result = CloudEventResponse {
                            token: time.to_string(),
                            request_id: time.to_string(),
                            time: generate_current_time(),
                            msg_info: "".to_string(),
                            data,
                        };
                        let response = serde_json::to_string(&result).unwrap();
                        let _ = client_publish(&client, &topic_request, &response).await;
                        log::error!("do cloud_event 序列化错误: {:?}", p.payload);
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    log::error!("do cloud_event error: {:?}", e);
                    break;
                }
            }
        }
    });
    Ok(())
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

fn generate_query_data(devs: &Vec<QueryDevResponseBody>) -> DataQuery {
    let time = Local::now().timestamp_millis();
    let body = devs.iter().flat_map(|dev|
        dev.devs.iter().map(|v|{
            DataQueryBody {
                dev: format!("{}_{}", v.model.clone(), v.dev_guid),
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
        body: vec!["DC_SDTTU_frozen".to_string()],
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
            body: vec![RegisterDevResultBody {
                model: "DC_PLCC".to_string(),
                port: "".to_string(),
                body: vec![DeviceEntry { addr: "".to_string(), appname: "".to_string(), desc: "".to_string(), dev: "app_dev".to_string(), 
                    device_type: "".to_string(), guid: "".to_string(), isReport: "".to_string(), manu_id: "".to_string(), 
                    manu_name: "".to_string(), node_id: "".to_string(), pro_type: "".to_string(), product_id: "".to_string() }
                ]
            }],
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
    }
}
