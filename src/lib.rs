use serde_repr::{Serialize_repr, Deserialize_repr};

pub mod runner;
pub mod parser;
pub mod db;
pub mod model;
pub mod utils;
pub mod env;

pub const ADAPTER_NAME: &str = "adapter";
pub const URL_LOGIN: &str = "api/v1/auth/login";
pub const URL_POINTS: &str = "api/v1/points/models";
pub const URL_TRANSPORTS: &str = "api/v1/transports/models";
pub const URL_AOES: &str = "api/v1/aoes/models";
pub const URL_AOE_RESULTS: &str = "api/v1/aoe_results";
pub const URL_RESET: &str = "api/v1/controls/reset";
pub const URL_UNRUN_AOES: &str = "api/v1/unrun_aoes";
pub const URL_RUNNING_AOES: &str = "api/v1/running_aoes";
pub const URL_AOE_CONTROL: &str = "api/v1/controls/aoes";
pub const URL_DFFS: &str = "api/v1/flows/models";
pub const URL_DFF_RESULTS: &str = "api/v1/flows/results";
pub const URL_RUNNING_DFFS: &str = "api/v1/flows/running";
pub const URL_UNRUN_DFFS: &str = "api/v1/flows/unrun";
pub const URL_DFF_CONTROL: &str = "api/v1/flows/controls";
pub const URL_DFF_RESET: &str = "api/v1/pscpu/reset";
pub const URL_DFF_START: &str = "api/v1/pscpu/start";
pub const MODEL_FROZEN: &str = "DC_SDTTU_frozen";
pub const MODEL_FROZEN_METER: &str = "DC_Meter_frozen";
pub const MODEL_PORT_METER: &str = "PLC";
pub const MODEL_DESC_METER: &str = "metering";

#[repr(u16)]
#[derive(Debug, Clone, PartialEq, Serialize_repr, Deserialize_repr)]
pub enum ErrCode {
    Success = 200,
    PlccAdapterNotFound = 601,
    MqttConnectErr = 610,
    PlccConnectErr = 611,
    PointJsonNotFound = 612,
    PointJsonDeserializeErr = 613,
    PointIsEmpty = 614,
    PointUndefined = 615,
    TransportJsonNotFound = 616,
    TransportJsonDeserializeErr = 617,
    TransportIsEmpty = 618,
    TransportPointNotFound = 619,
    TransportPointTagErr = 620,
    QueryDevDeserializeErr = 621,
    QueryDevAttrNotFound = 622,
    QueryDevTimeout = 623,
    DevGuidNotFound = 624,
    AoeJsonNotFound = 625,
    AoeJsonDeserializeErr = 626,
    AoeVariableErr = 627,
    AoeEventErr = 628,
    AoeActionErr = 629,
    AppRegisterErr = 630,
    ModelRegisterErr = 631,
    QueryRegisterDevErr = 632,
    InternalErr = 633,
    IoErr = 634,
    DataJsonDeserializeErr = 635,
    AoeIdNotFound = 636,
    PlccActionErr = 637,
    MqttTimeoutErr = 638,
    MemsConnectErr = 639,
    DffJsonNotFound = 640,
    DffJsonDeserializeErr = 641,
    DffVariableErr = 642,
    DffIdNotFound = 643,
    DffActionErr = 644,
    Other = 699,
}

pub struct AdapterErr {
    pub code: ErrCode,
    pub msg: String,
}
