pub mod runner;
pub mod parser;
pub mod db;
pub mod model;
pub mod utils;

pub const APP_NAME: &str = "ext.syy.plcc";
pub const FILE_NAME_POINTS: &str = "points.json";
pub const FILE_NAME_TRANSPORTS: &str = "transports.json";
pub const FILE_NAME_AOES: &str = "aoes.json";
pub const PLCC_USR: &str = "admin";
pub const PLCC_PWD: &str = "easy2021";
pub const URL_LOGIN: &str = "api/v1/auth/login";
pub const URL_POINTS: &str = "api/v1/points/models";
pub const URL_TRANSPORTS: &str = "api/v1/transports/models";
pub const URL_AOES: &str = "api/v1/aoes/models";
pub const URL_AOE_RESULTS: &str = "api/v1/aoe_results";
pub const URL_RESET: &str = "api/v1/controls/reset";
pub const MQTT_HOST: &str = "localhost";
pub const MQTT_PORT: u16 = 1883;
pub const HTTP_PORT: u16 = 8088;

pub const JSON_DATA_PATH: &str = "/data1/ext.syy.plcc/cloudconf";
pub const DB_PATH_PARSER: &str = "/data1/ext.syy.plcc/cloudconf/parser";
pub const PLCC_HOST: &str = "http://192.168.1.99:58899";

// pub const JSON_DATA_PATH: &str = "C:/project/other/test_file";
// pub const DB_PATH_PARSER: &str = "C:/project/other/test_file/parser";
// pub const PLCC_HOST: &str = "http://localhost:8181";
