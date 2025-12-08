use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};

use crate::{ErrCode, model::{north::{MyAoe, MyDffModel, MyDffResult, MyMeasurement, MyPbAoeResult, MyTransport}, south::DffResult}};

pub trait HasToken {
    fn token(&self) -> String;
}

impl HasToken for RegisterApp {
    fn token(&self) -> String {
        self.token.clone()
    }
}

impl HasToken for QueryDevResponse {
    fn token(&self) -> String {
        self.token.clone()
    }
}

impl HasToken for QueryDev {
    fn token(&self) -> String {
        self.token.clone()
    }
}

impl HasToken for RegisterModel {
    fn token(&self) -> String {
        self.token.clone()
    }
}

impl HasToken for RegisterResponse {
    fn token(&self) -> String {
        self.token.clone()
    }
}

impl HasToken for GetModel {
    fn token(&self) -> String {
        self.token.clone()
    }
}

impl HasToken for GetModelResponse {
    fn token(&self) -> String {
        self.token.clone()
    }
}

impl HasToken for QueryRegisterDev {
    fn token(&self) -> String {
        self.token.clone()
    }
}

impl HasToken for RegisterDevResult {
    fn token(&self) -> String {
        self.token.clone()
    }
}

impl HasToken for RequestHistory {
    fn token(&self) -> String {
        self.token.clone()
    }
}

impl HasToken for ResponseHistory {
    fn token(&self) -> String {
        self.token.clone()
    }
}

impl HasToken for DataQuery {
    fn token(&self) -> String {
        self.token.clone()
    }
}

impl HasToken for KeepAliveResponse {
    fn token(&self) -> String {
        self.token.clone()
    }
}

impl HasToken for CloudEventResponse {
    fn token(&self) -> String {
        self.token.clone()
    }
}

impl HasToken for MemsEventResponse {
    fn token(&self) -> String {
        self.token.clone()
    }
}

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

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct GetModel {
    pub token: String,
    pub time: String,
    pub body: Vec<String>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct GetModelResponse {
    pub token: String,
    pub time: String,
    pub body: Vec<GetModelResponseBody>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct GetModelResponseBody {
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
    pub body: Vec<RegisterDevEntry>,
    pub model: String,
    pub port: String,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterDevEntry {
    pub addr: String,
    pub appname: String,
    pub desc: String,
    pub dev: String,
    #[serde(rename = "deviceType")]
    pub device_type: String,
    pub guid: String,
    #[serde(rename = "isReport")]
    pub is_report: String,
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
pub struct DffUpdate {
    pub token: String,
    pub time: String,
    pub body: Vec<DffUpdateBody>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DffUpdateBody {
    pub model: String,
    pub dev: String,
    pub event: String,
    pub starttime: String,
    pub endtime: String,
    #[serde(rename = "HappenSrc")]
    pub happen_src: String,
    #[serde(rename = "IsNeedRpt")]
    pub is_need_rpt: String,
    pub extdata: Vec<MyDffResult>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DffSet {
    pub token: String,
    pub time: String,
    #[serde(rename = "SourType")]
    pub sour_type: String,
    pub body: Vec<DffSetBody>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DffSetBody {
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
    pub extdata: Vec<MyDffResult>,
}

#[serde_as]
#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct CloudEventDffStatus {
    #[serde_as(as = "DisplayFromStr")]
    pub dff_id: u64,
    pub dff_status: u8,
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

#[derive(Debug, Serialize, Deserialize)]
pub struct RequestHistory {
    pub token: String,
    pub time: String,
    pub choice: String,
    pub time_type: String,
    pub start_time: String,
    pub end_time: String,
    pub time_span: String,
    pub frozentype: String,
    pub body: Vec<RequestHistoryBody>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RequestHistoryBody {
    pub dev: String,
    pub body: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseHistory {
    pub token: String,
    pub time: String,
    pub body: Vec<ResponseHistoryBody>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseHistoryBody {
    pub dev: String,
    pub body: Vec<ResponseHistoryData>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseHistoryData {
    pub timestamp: String,
    pub timestartgather: String,
    pub timeendgather: String,
    pub additionalcheck: String,
    pub body: Vec<ResponseHistoryMeasure>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseHistoryMeasure {
    pub name: String,
    pub val: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RequestGuid {
    pub token: String,
    pub time: String,
    pub body: Vec<RequestGuidBody>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RequestGuidBody {
    pub model: String,
    pub port: String,
    pub addr: String,
    pub desc: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseGuid {
    pub token: String,
    pub time: String,
    pub body: Vec<ResponseGuidBody>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ResponseGuidBody {
    pub model: String,
    pub port: String,
    pub addr: String,
    pub desc: String,
    pub guid: String,
    pub dev: String,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct MemsEventRequest {
    pub token: String,
    #[serde(rename = "requestId")]
    pub request_id: String,
    pub time: String,
    #[serde(rename = "msgInfo")]
    pub msg_info: String,
    pub cmd: MemsEventCmd,
    pub body: Option<MemsEventRequestBody>,
}

#[serde_as]
#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct MemsEventRequestBody {
    #[serde_as(as = "Option<Vec<DisplayFromStr>>")]
    pub dffs_id: Option<Vec<u64>>,
    pub dffs_status: Option<Vec<MemsEventDffStatus>>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct MemsEventResponse {
    pub token: String,
    #[serde(rename = "requestId")]
    pub request_id: String,
    pub time: String,
    #[serde(rename = "msgInfo")]
    pub msg_info: String,
    pub data: MemsEventResponseBody,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct MemsEventResponseBody {
    pub dffs: Option<Vec<MyDffModel>>,
    pub dffs_status: Option<Vec<MemsEventDffStatus>>,
    pub code: ErrCode,
    pub msg: String,
}

#[serde_as]
#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct MemsEventDffStatus {
    #[serde_as(as = "DisplayFromStr")]
    pub dff_id: u64,
    pub dff_status: u8,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub enum MemsEventCmd {
    GetTgDFFConfig,
    TgDFFControl,
    GetTgDFFStatus
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
