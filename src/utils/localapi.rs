use std::collections::HashMap;
use reqwest::Client;
use crate::model::datacenter::QueryDevResponseBody;
use crate::ADAPTER_NAME;
use crate::env::Env;

pub async fn query_aoe_mapping() -> Result<HashMap<u64, u64>, String> {
    let env = Env::get_env(ADAPTER_NAME);
    let http_server_port = env.get_http_server_port();
    let url = format!("http://localhost:{http_server_port}/api/v1/parser/aoe_mapping");
    let client = Client::new();
    match client
        .get(&url)
        .send().await {
        Ok(response) => {
            if let Ok(aoes_mapping) = response.json::<HashMap<u64, u64>>().await {
                Ok(aoes_mapping)
            } else {
                Err("调用AOE映射API获取AOE映射失败".to_string())
            }
        },
        Err(ee) => {
            log::error!("link to local api error: {:?}", ee);
            Err(format!("连接本地API失败：{:?}", ee))
        }
    }
}

pub async fn query_dev_mapping() -> Result<Vec<QueryDevResponseBody>, String> {
    let env = Env::get_env(ADAPTER_NAME);
    let http_server_port = env.get_http_server_port();
    let url = format!("http://localhost:{http_server_port}/api/v1/parser/dev_mapping");
    let client = Client::new();
    match client
        .get(&url)
        .send().await {
        Ok(response) => {
            if let Ok(devs) = response.json::<Vec<QueryDevResponseBody>>().await {
                Ok(devs)
            } else {
                Err("调用设备列表API获取设备映射失败".to_string())
            }
        },
        Err(ee) => {
            log::error!("link to local api error: {:?}", ee);
            Err(format!("连接本地API失败：{:?}", ee))
        }
    }
}
