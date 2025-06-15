use serde::{Deserialize, Serialize};

use crate::model::north::MyPbAoeResult;

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct Register {
    pub token: String,
    pub time: String,
    pub body: Vec<RegisterBody>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct RegisterBody {
    pub model: String,
    pub port: String,
    pub addr: String,
    pub desc: String,
    pub manuID: String,
    pub manuName: String,
    pub proType: String,
    pub deviceType: String,
    pub isReport: String,
    pub nodeID: String,
    pub productID: String,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct RegisterResponse {
    pub token: String,
    pub time: String,
    pub ack: String,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct DataQuery {
    pub token: String,
    pub time: String,
    pub body: Vec<DataQueryBody>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct DataQueryBody {
    pub dev: String,
    pub totalcall: String,
    pub body: Vec<String>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct QueryDev {
    pub token: String,
    pub time: String,
    pub devices: Vec<String>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct QueryDevResponse {
    pub token: String,
    pub time: String,
    pub devices: Vec<QueryDevResponseBody>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct QueryDevResponseBody {
    pub devID: String,
    pub status: String,
    // 以下字段是可选的（不是所有设备都有这些字段）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub addr: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desc: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AoeResult {
    pub token: String,
    pub time: String,
    pub body: Vec<AoeResultBody>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AoeResultBody {
    pub model: String,
    pub dev: String,
    pub event: String,
    pub starttime: String,
    pub endtime: String,
    #[serde(rename = "HappenSrc")]
    pub happen_src: String,
    #[serde(rename = "IsNeedRpt")]
    pub is_need_rpt: String,
    pub extdata: Vec<MyPbAoeResult>,
}