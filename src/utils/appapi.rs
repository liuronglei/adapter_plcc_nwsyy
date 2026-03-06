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
        match response.json::<NumberArrayResult>().await {
            Ok(result) => {
                if result.code == StatusCode::OK {
                    Ok(result.data.unwrap_or_default())
                } else {
                    Err(AdapterErr {
                        code: ErrCode::PlccConnectErr,
                        msg: format!("调用第三方{url}获取结果成功，但code为:{}，message为:{}", result.code, result.message.unwrap_or_default()),
                    })
                }
            }
            Err(e) => {
                Err(AdapterErr {
                    code: ErrCode::PlccConnectErr,
                    msg: format!("调用第三方{url}获取结果JSON解析失败：{e:?}"),
                })
            }
        }
    } else {
        Err(AdapterErr {
            code: ErrCode::PlccConnectErr,
            msg: format!("调用第三方{url}失败"),
        })
    }
}
