pub mod runner;
pub mod parser;
pub mod db;
pub mod model;
pub mod utils;
pub mod env;

pub const APP_NAME: &str = "ext.syy.plcc";
pub const URL_LOGIN: &str = "api/v1/auth/login";
pub const URL_POINTS: &str = "api/v1/points/models";
pub const URL_TRANSPORTS: &str = "api/v1/transports/models";
pub const URL_AOES: &str = "api/v1/aoes/models";
pub const URL_AOE_RESULTS: &str = "api/v1/aoe_results";
pub const URL_RESET: &str = "api/v1/controls/reset";
