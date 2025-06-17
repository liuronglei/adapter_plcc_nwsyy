use std::collections::HashMap;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use tokio::time::{timeout, Duration};
use tokio::sync::oneshot;
use chrono::Local;

use crate::model::north::MyPbAoeResult;
use crate::ADAPTER_NAME;
use crate::model::datacenter::{DataQuery, DataQueryBody, KeepAliveRequest, KeepAliveResponse, QueryDev, QueryDevResponse, Register, RegisterBody, RegisterResponse};
use crate::model::datacenter::{AoeResult, AoeResultBody};
use crate::env::Env;

pub async fn client_subscribe(client: &AsyncClient, topic: &str) -> Result<(), String> {
    match client.subscribe(topic, QoS::AtMostOnce).await {
        Ok(_) => Ok(()),
        Err(v) => {
            Err(v.to_string())
        }
    }
}

pub async fn client_publish(client: &AsyncClient, topic: &str, payload: &str) -> Result<(), String> {
    match client.publish(topic, QoS::AtMostOnce, false, payload).await {
        Ok(_) => Ok(()),
        Err(v) => {
            Err(v.to_string())
        }
    }
}

pub async fn do_query_dev(name: &str, host: &str, port: u16, dev_ids: Vec<String>) -> Result<HashMap<String, String>, String> {
    let env = Env::get_env(ADAPTER_NAME);
    let mqtt_timeout = env.get_mqtt_timeout();
    let app_name = env.get_app_name();
    let mut mqttoptions = MqttOptions::new(name, host, port);
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
    let (tx, rx) = oneshot::channel::<Result<HashMap<String, String>, String>>();
    tokio::spawn(async move {
        loop {
            let event = eventloop.poll().await;
            match event {
                Ok(Event::Incoming(Incoming::Publish(p))) => {
                    if p.topic == topic_response_query_dev {
                        let send_result = match serde_json::from_slice::<QueryDevResponse>(&p.payload) {
                            Ok(msg) => {
                                let mut result = HashMap::new();
                                for data in msg.devices {
                                    if data.status.to_lowercase() == "true".to_string() && data.guid.is_some() {
                                        result.insert(data.devID, data.guid.unwrap());
                                    }
                                }
                                tx.send(Ok(result))
                            }
                            Err(e) => tx.send(Err(format!("查询设备GUID，解析返回字符串失败: {:?}", e))),
                        };
                        if send_result.is_err() {
                            log::error!("查询设备GUID，接收mqtt消息发生错误");
                        }
                        break;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    let _ = tx.send(Err(format!("查询设备GUID，发生错误: {:?}", e)));
                    break;
                }
            }
        }
    });
    match timeout(Duration::from_secs(mqtt_timeout), rx).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err("查询设备GUID，等待响应失败".to_string()),
        Err(_) => Err("查询设备GUID超时，未收到MQTT响应".to_string()),
    }
}

pub async fn do_register(name: &str, host: &str, port: u16) -> Result<(), String> {
    let env = Env::get_env(ADAPTER_NAME);
    let mqtt_timeout = env.get_mqtt_timeout();
    let app_name = env.get_app_name();
    let mut mqttoptions = MqttOptions::new(name, host, port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    // mqttoptions.set_credentials("username", "password");
    let topic_request_register = format!("/sys.dbc/{app_name}/S-dataservice/F-Register");
    let topic_response_register = format!("/{app_name}/sys.dbc/S-dataservice/F-Register");
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    // 订阅注册消息返回
    client_subscribe(&client, &topic_response_register).await?;
    // 发布注册消息
    let body = generate_register();
    let payload = serde_json::to_string(&body).unwrap();
    client_publish(&client, &topic_request_register, &payload).await?;
    // 处理订阅消息
    let (tx, rx) = oneshot::channel::<Result<bool, String>>();
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
                            Err(e) => tx.send(Err(format!("注册APP，解析返回字符串失败: {:?}", e))),
                        };
                        if send_result.is_err() {
                            log::error!("注册APP，接收mqtt消息发生错误");
                        }
                        break;
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    let _ = tx.send(Err(format!("注册APP，发生错误: {:?}", e)));
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
                        return Err("注册APP失败，响应结果为false".to_string());
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Ok(Err(_)) => return Err("注册APP，等待响应失败".to_string()),
        Err(_) => return Err("注册APP超时，未收到MQTT响应".to_string()),
    }
    Ok(())
}

pub async fn do_data_query(name: &str, host: &str, port: u16) -> Result<(), String> {
    let env = Env::get_env(ADAPTER_NAME);
    let app_name = env.get_app_name();
    let mut mqttoptions = MqttOptions::new(name, host, port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    // mqttoptions.set_credentials("username", "password");
    let (client, e) = AsyncClient::new(mqttoptions, 10);
    let topic_request_query = format!("/sys.dbc/{app_name}/S-dataservice/F-GetRealData");
    let payload = serde_json::to_string(&generate_query_data()).unwrap();
    client_publish(&client, &topic_request_query, &payload).await?;
    Ok(())
}

pub async fn do_keep_alive() -> Result<(), String> {
    tokio::spawn(async {
        if let Err(e) = keep_alive().await {
            log::error!("保活监听任务失败：{}", e);
        }
    });
    Ok(())
}

pub async fn keep_alive() -> Result<(), String> {
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
                    eprintln!("发生错误: {:?}", e);
                    break;
                }
            }
        }
    });
    Ok(())
}

fn generate_register() -> Register {
    let body = RegisterBody {
        model: "ADC".to_string(),
        port: "1".to_string(),
        addr: "1".to_string(),
        desc: "jiaoliucaiji".to_string(),
        manuID: "1234".to_string(),
        manuName: "xxx".to_string(),
        proType: "xxx".to_string(),
        deviceType: "1234".to_string(),
        isReport: "0".to_string(),
        nodeID: "XXXX".to_string(),
        productID: "XXXX".to_string(),
    };
    let time = Local::now().timestamp_millis();
    Register {
        token: format!("register_{time}"),
        time: generate_current_time(),
        body: vec![body],
    }
}

fn generate_current_time() -> String {
    let now = Local::now();
    now.format("%Y-%m-%dT%H:%M:%S%.3f%z").to_string()
}

fn generate_query_data() -> DataQuery {
    let time = Local::now().timestamp_millis();
    let body = DataQueryBody {
        dev: "plcc_1".to_string(),
        totalcall: "1".to_string(),
        body: vec![],
    };
    DataQuery {
        token: format!("register_{time}"),
        time: generate_current_time(),
        body: vec![body],
    }
}

fn generate_query_dev(dev_ids: Vec<String>) -> QueryDev {
    let time = Local::now().timestamp_millis();
    QueryDev {
        token: format!("query_dev_{time}"),
        time: generate_current_time(),
        devices: dev_ids,
    }
}

pub fn generate_aoe_result(aoe_result: Vec<MyPbAoeResult>) -> AoeResult {
    let time = Local::now().timestamp_millis();
    let body = AoeResultBody {
        model: "plcc".to_string(),
        dev: "plcc_1".to_string(),
        event: "tgAOEResult".to_string(),
        starttime: generate_current_time(),
        endtime: generate_current_time(),
        happen_src: "00".to_string(),
        is_need_rpt: "Yes".to_string(),
        extdata: aoe_result.clone(),
    };
    AoeResult {
        token: format!("aoe_result_{time}"),
        time: generate_current_time(),
        body: vec![body],
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
