use log::{debug, error, info, trace, warn};

use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use tokio::time::Duration;
use crate::APP_NAME;
use crate::model::datacenter::{Register, RegisterBody};
use chrono::Local;

pub async fn start_mqtt() -> std::io::Result<()> {
    let mut mqttoptions = MqttOptions::new("rust-client", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    // mqttoptions.set_credentials("username", "password");
    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
    // 订阅注册消息返回
    client.subscribe(format!("/{APP_NAME}/svc.dbc/S-dataservice/F-Register"), QoS::AtMostOnce).await.unwrap();
    // 发布注册消息
    let payload = serde_json::to_string(&generate_register()).unwrap();
    client
        .publish(format!("/svc.dbc/{APP_NAME}/S-dataservice/F-Register"), QoS::AtMostOnce, false, payload)
        .await
        .unwrap();
    // 处理订阅消息
    tokio::spawn(async move {
        loop {
            let event = eventloop.poll().await;
            match event {
                Ok(Event::Incoming(Incoming::Publish(p))) => {
                    println!("接收到消息: {}, {}", p.topic, String::from_utf8_lossy(&p.payload));
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
    Register {
        token: "234".to_string(),
        time: generate_current_time(),
        body: vec![body],
    }
}

fn generate_current_time() -> String {
    let now = Local::now();
    now.format("%Y-%m-%dT%H:%M:%S%.3f%z").to_string()
}
