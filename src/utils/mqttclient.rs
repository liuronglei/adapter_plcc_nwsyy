use std::pin::Pin;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::time::{timeout, Duration};
use chrono::Local;
use tokio::sync::oneshot;

use crate::{ADAPTER_NAME, AdapterErr, ErrCode};
use crate::env::Env;
use crate::model::datacenter::*;

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
                    port: "NULL".to_string(),
                    body: vec![RegisterDevEntry { addr: "000000".to_string(), appname: "".to_string(), desc: "terminal".to_string(), dev: "DC_SDTTU_frozen_1".to_string(), 
                        device_type: "".to_string(), guid: "".to_string(), is_report: "".to_string(), manu_id: "".to_string(), 
                        manu_name: "".to_string(), node_id: "".to_string(), pro_type: "".to_string(), product_id: "".to_string() }
                    ]
                },
                RegisterDevResultBody {
                    model: crate::MODEL_FROZEN_METER.to_string(),
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
                body: vec![RegisterModelBody {
                    name: "tgPowerCutAlarm".to_string(),
                    mtype: "int".to_string(),
                    unit: "".to_string(),
                    deadzone: "".to_string(),
                    ratio: "".to_string(),
                    isReport: "0".to_string(),
                    userdefine: "".to_string(),
                }]
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
