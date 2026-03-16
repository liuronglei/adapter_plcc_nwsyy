use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use crate::{AdapterErr, ErrCode};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NumberArrayResult {
    code: u16,
    message: Option<String>,
    data: Option<Vec<f64>>,
}

pub async fn do_get_number_array(url: &str) -> Result<Vec<f64>, AdapterErr> {
    let client = Client::new();
    if let Ok(response) = client
        .get(url)
        .send().await {
        match response.text().await {
            Ok(text) => {
                log::info!("do app_api {url} return text: {text}");
                match serde_json::from_str::<NumberArrayResult>(&text) {
                    Ok(result) => {
                        if result.code == StatusCode::OK {
                            Ok(result.data.unwrap_or_default())
                        } else {
                            Err(AdapterErr {
                                code: ErrCode::Other,
                                msg: format!("do app_api {url} success, but code is {}, message is: {}", result.code, result.message.unwrap_or_default()),
                            })
                        }
                    }
                    Err(e) => {
                        Err(AdapterErr {
                            code: ErrCode::Other,
                            msg: format!("do app_api {url} parse json error: {e:?}"),
                        })
                    }
                }
            }
            Err(e) => {
                Err(AdapterErr {
                    code: ErrCode::Other,
                    msg: format!("do app_api {url} get text error: {e:?}"),
                })
            }
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::Other,
            msg: format!("do app_api error: {url}"),
        })
    }
}
