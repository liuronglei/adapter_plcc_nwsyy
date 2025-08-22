use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};

use crate::{model::north::{MyAoe, MyMeasurement, MyPbAoeResult, MyTransport}, ErrCode};

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
    pub devices: Vec<QueryDevBody>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct QueryDevBody {
    #[serde(rename = "devId")]
    pub dev_id: String,
    #[serde(rename = "serviceId")]
    pub service_id: String,
    pub attrs: Option<Vec<String>>,
    #[serde(rename = "settingCmds")]
    pub setting_cmds: Option<Vec<QueryDevBodyCmdMap>>,
    #[serde(rename = "ykCmds")]
    pub yk_cmds: Option<Vec<String>>,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct QueryDevBodyCmdMap {
    pub name: String,
    pub params: Vec<String>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct QueryDevResponse {
    pub token: String,
    pub time: String,
    pub devices: Vec<QueryDevResponseBody>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct QueryDevResponseBody {
    #[serde(rename = "devId")]
    pub dev_id: String,
    #[serde(rename = "serviceId")]
    pub service_id: String,
    pub devs: Vec<QueryDevResponseBodyDev>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct QueryDevResponseBodyDev {
    #[serde(rename = "devGuid")]
    pub dev_guid: String,
    pub addr: String,
    pub model: String,
    pub desc: String,
    pub port: String,
    pub attrs: Option<Vec<QueryDevResponseBodyMap>>,
    #[serde(rename = "settingCmds")]
    pub setting_cmds: Option<Vec<QueryDevResponseBodySettingCmd>>,
    #[serde(rename = "ykCmds")]
    pub yk_cmds: Option<Vec<QueryDevResponseBodyMap>>,
    #[serde(rename = "notFound")]
    pub not_found: Option<Vec<String>>,
    pub reason: Option<String>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct QueryDevResponseBodySettingCmd {
    pub name: String,
    pub params: Vec<QueryDevResponseBodyMap>,
}

#[allow(non_snake_case)]
#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct QueryDevResponseBodyMap {
    pub iot: String,
    pub dc: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryRegisterDev {
    pub body: Vec<String>,
    pub time: String,
    pub token: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterDevResult {
    pub body: Vec<RegisterDevResultBody>,
    pub time: String,
    pub token: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterDevResultBody {
    pub body: Vec<DeviceEntry>,
    pub model: String,
    pub port: String,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
pub struct DeviceEntry {
    pub addr: String,
    pub appname: String,
    pub desc: String,
    pub dev: String,
    #[serde(rename = "deviceType")]
    pub device_type: String,
    pub guid: String,
    pub isReport: String,
    #[serde(rename = "manuID")]
    pub manu_id: String,
    #[serde(rename = "manuName")]
    pub manu_name: String,
    #[serde(rename = "nodeID")]
    pub node_id: String,
    #[serde(rename = "proType")]
    pub pro_type: String,
    #[serde(rename = "productID")]
    pub product_id: String,
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

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct CloudEventRequest {
    pub token: String,
    #[serde(rename = "requestId")]
    pub request_id: String,
    pub time: String,
    #[serde(rename = "msgInfo")]
    pub msg_info: String,
    pub cmd: CloudEventCmd,
    pub body: Option<CloudEventRequestBody>,
}

#[serde_as]
#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct CloudEventRequestBody {
    #[serde_as(as = "Option<Vec<DisplayFromStr>>")]
    pub aoes_id: Option<Vec<u64>>,
    pub aoes_status: Option<Vec<CloudEventAoeStatus>>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct CloudEventResponse {
    pub token: String,
    #[serde(rename = "requestId")]
    pub request_id: String,
    pub time: String,
    #[serde(rename = "msgInfo")]
    pub msg_info: String,
    pub data: CloudEventResponseBody,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct CloudEventResponseBody {
    pub points: Option<Vec<MyMeasurement>>,
    pub transports: Option<Vec<MyTransport>>,
    pub aoes: Option<Vec<MyAoe>>,
    pub aoes_status: Option<Vec<CloudEventAoeStatus>>,
    pub code: ErrCode,
    pub msg: String,
}

#[serde_as]
#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct CloudEventAoeStatus {
    #[serde_as(as = "DisplayFromStr")]
    pub aoe_id: u64,
    pub aoe_status: u8,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub enum CloudEventCmd {
    GetTgPLCCConfig,
    TgAOEControl,
    GetTgAOEStatus
}

#[test]
fn test_json_parse() {
    let item = CloudEventRequest {
        token: "a".to_string(),
        request_id: "b".to_string(),
        time: "c".to_string(),
        msg_info: "d".to_string(),
        cmd: CloudEventCmd::GetTgAOEStatus,
        body: Some(CloudEventRequestBody {
            aoes_id: Some(vec![1, 2]),
            aoes_status: Some(vec![CloudEventAoeStatus {
                aoe_id: 3,
                aoe_status: 4
            }]),
        }),
    };
    let to_str = serde_json::to_string(&item).unwrap();
    println!("to_str: {}", to_str);
    match serde_json::from_slice::<CloudEventRequest>(to_str.as_bytes()) {
        Ok(msg) => println!("from_str: {:?}", msg),
        Err(e) => println!("err: {:?}", e),
    }
    // let to_str = to_str.replace("\"1\"", "1").replace("\"2\"", "2").replace("\"3\"", "3");
    println!("to_str2: {}", to_str);
    match serde_json::from_slice::<CloudEventRequest>(to_str.as_bytes()) {
        Ok(msg) => println!("from_str2: {:?}", msg),
        Err(e) => println!("err2: {:?}", e),
    }
}
