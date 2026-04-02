use reqwest::{
    Client, StatusCode,
    header::{HeaderMap, HeaderValue, ACCEPT, USER_AGENT},
};
use serde::{Deserialize, Serialize};
use crate::{AdapterErr, ErrCode};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct NumberArrayResult {
    code: u16,
    message: Option<String>,
    data: Option<Vec<f64>>,
}

pub async fn do_get_number_array(url: &str) -> Result<Vec<f64>, AdapterErr> {
    let client = create_client();
    if let Ok(response) = client
        .get(url)
        .send().await {
        match response.json::<NumberArrayResult>().await {
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
    } else {
        Err(AdapterErr {
            code: ErrCode::Other,
            msg: format!("do app_api error: {url}"),
        })
    }
}

fn create_client() -> Client {
    let mut headers = HeaderMap::new();
    headers.insert(
        ACCEPT,
        HeaderValue::from_static("*/*")
    );
    headers.insert(
        USER_AGENT,
        HeaderValue::from_static("plcc")
    );
    Client::builder()
        .default_headers(headers)
        .build()
        .unwrap()
}
