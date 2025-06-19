use serde::{Deserialize, Serialize};

use crate::model::north::MyPbAoeResult;

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct RegisterApp {
    pub token: String,
    pub time: String,
    pub body: Vec<RegisterAPPBody>,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct RegisterAPPBody {
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
pub struct RegisterModel {
    pub token: String,
    pub time: String,
    pub model: String,
    pub body: Vec<RegisterModelBody>,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct RegisterModelBody {
    pub name: String,
    #[serde(rename = "type")]
    pub mtype: String,
    pub unit: String,
    pub deadzone: String,
    pub ratio: String,
    pub isReport: String,
    pub userdefine: String,
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

#[allow(non_snake_case)]
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
pub struct AoeUpdate {
    pub token: String,
    pub time: String,
    pub body: Vec<AoeUpdateBody>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AoeUpdateBody {
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

#[derive(Debug, Serialize, Deserialize)]
pub struct AoeSet {
    pub token: String,
    pub time: String,
    #[serde(rename = "SourType")]
    pub sour_type: String,
    pub body: Vec<AoeSetBody>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AoeSetBody {
    pub model: String,
    pub dev: String,
    pub event: String,
    pub timestamp: String,
    pub timestartgather: String,
    pub timeendgather: String,
    pub starttimestamp: String,
    pub endtimestamp: String,
    #[serde(rename = "HappenSrc")]
    pub happen_src: String,
    #[serde(rename = "IsNeedRpt")]
    pub is_need_rpt: String,
    pub occurnum: String,
    #[serde(rename = "EventLevel")]
    pub event_level: String,
    #[serde(rename = "RptStatus")]
    pub rpt_status: Vec<RptStatusItem>,
    pub data: String,
    pub extdata: Vec<MyPbAoeResult>,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct RptStatusItem {
    #[serde(rename = "Net-1")]
    pub net_1: String,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct KeepAliveRequest {
    pub token: String,
    pub time: String,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct KeepAliveResponse {
    pub token: String,
    pub time: String,
    pub ack: String,
    pub errmsg: String,
}