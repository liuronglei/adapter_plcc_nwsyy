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

#[repr(u16)]
#[derive(Debug)]
pub enum ErrCode {
    Success = 200,
    MqttConnectErr = 520,
    PlccConnectErr = 521,
    PointJsonNotFound = 522,
    PointJsonDeserializeErr = 523,
    PointIsEmpty = 524,
    PointUndefined = 525,
    TransportJsonNotFound = 526,
    TransportJsonDeserializeErr = 527,
    TransportIsEmpty = 528,
    TransportPointNotFound = 529,
    TransportPointTagErr = 530,
    QueryDevDeserializeErr = 531,
    QueryDevStatusErr = 532,
    QueryDevTimeout = 533,
    DevGuidNotFound = 534,
    AoeJsonNotFound = 535,
    AoeJsonDeserializeErr = 536,
    AoeVariableErr = 537,
    AoeEventErr = 538,
    AoeActionErr = 539,
    AppRegisterErr = 540,
    ModelRegisterErr = 541,
    QueryRegisterDevErr = 542,
    InternalErr = 543,
}

pub struct AdapterErr {
    pub code: ErrCode,
    pub msg: String,
}
