use std::{collections::HashMap, fmt::Debug};
use std::env;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};

use log::{error, info, warn};
use crate::utils::log_init::LogConfig;

// const DEFAULT_CONFIG_PATH : &str = "config";
// for config
pub const CONF_PATH: &str = "configFilePath";
pub const PLCC_SERVER: &str = "plccServer";
pub const PLCC_USER: &str = "plccUser";
pub const PLCC_PWD: &str = "plccPwd";
pub const MQTT_SERVER: &str = "mqttServer";
pub const HTTP_SERVER_PORT: &str = "httpServerPort";
pub const MQTT_TIMEOUT: &str = "mqttTimeout";
pub const POINT_FILE_DIR: &str = "pointFileDir";
pub const TRANSPORT_DIR: &str = "transportFileDir";
pub const AOE_DIR: &str = "aoeFileDir";
pub const JSON_DIR: &str = "jsonFileDir";
pub const APP_NAME: &str = "appName";
pub const APP_MODEL: &str = "appModel";

pub const BEE_ID: &str = "beeId";

pub const MQTT_AUTH: &str = "mqttCredential";
pub const MQTT_CLIENT_BUF_SIZE: &str = "mqttClientBufSize";
pub const MQTT_MV_LIMIT: &str = "mqttMvLimit";
pub const SOCKET_BUF_SIZE_NORTH: &str = "socketBufSizeNorth";
// 接收北向传输的最大Buf
pub const SOCKET_BUF_SIZE_SOUTH: &str = "socketBufSizeSouth";
// 接收北向传输的最大Buf
pub const EIG_HOME: &str = "eigHome";
pub const EXE_ROOT_DIR: &str = "exeRootDir";
pub const WEB_DIR: &str = "webFileDir";
pub const DB_DIR: &str = "dbFileDir";
pub const LOG_DIR: &str = "LogDir";

// 开启SSL证书
pub const IS_USE_SSL: &str = "isUseSsl";
pub const SSL_CERT_FILE_PATH: &str = "sslCertFilePath";
pub const SSL_KEY_FILE_PATH: &str = "sslKeyFilePath";
// short message service url

pub const MQTT_PACKAGE_MAX_SIZE: &str = "mqttPackageMaxSize";

pub const IS_LOCAL_FRONTEND: &str = "isLocalFrontend";
pub const IS_DB: &str = "isDb";
pub const IS_HIS_DB: &str = "isHisDb";
pub const IS_FAKE_DELETE: &str = "isFakeDelete";

pub const IS_LOCAL_MQTT: &str = "isLocalMqtt";
pub const LOCAL_MQTT_PORT: &str = "localMqttPort";
pub const DB_DIR_SIZE_LIMIT: &str = "dbDirSizeLimit";

pub const LOG_LEVEL: &str = "logLevel";
pub const LOG_SAVE_TIME: &str = "logSaveTime";
pub const LOG_SAVE_SIZE: &str = "logSaveSize";
pub const LOG_HIS_FILE_NUM: &str = "logHisFileNum";

pub const DATABASE_URL: &str = "databaseUrl";


const CONFIG_ARGS: [&str; 40] = [CONF_PATH, BEE_ID, MQTT_SERVER, MQTT_AUTH, HTTP_SERVER_PORT,
    MQTT_CLIENT_BUF_SIZE, MQTT_MV_LIMIT, SOCKET_BUF_SIZE_NORTH, SOCKET_BUF_SIZE_SOUTH, EXE_ROOT_DIR,
    POINT_FILE_DIR, TRANSPORT_DIR, JSON_DIR, AOE_DIR, WEB_DIR, DB_DIR, LOG_DIR,
    APP_NAME, APP_MODEL, MQTT_PACKAGE_MAX_SIZE, IS_LOCAL_FRONTEND, IS_DB, IS_HIS_DB, IS_FAKE_DELETE,
    DB_DIR_SIZE_LIMIT, IS_LOCAL_MQTT, LOCAL_MQTT_PORT, LOG_LEVEL, LOG_SAVE_TIME, LOG_SAVE_SIZE,
    LOG_HIS_FILE_NUM, MQTT_TIMEOUT, DATABASE_URL, PLCC_SERVER,
    PLCC_USER, PLCC_PWD, EIG_HOME, IS_USE_SSL, SSL_CERT_FILE_PATH, SSL_KEY_FILE_PATH];

pub const PING_GET: u8 = 1;
pub const CONFIG_GET: u8 = 2;
pub const CONFIG_PUT: u8 = 3;

pub const POINTS_LIMIT_NUMBER: usize = 1000;

pub static ENV: LazyLock<Mutex<HashMap<String, Env>>> = LazyLock::new(||Mutex::new(HashMap::new()));

#[derive(Debug, Clone, Default)]
pub struct Env {
    pub properties: HashMap<String, String>,
}

impl Env {

    // 避免多次初始化，请使用get_env()方法获得env
    pub fn init(app_name: &str) -> Env {
        let env = Env::init_with_args(app_name);
        (*ENV.lock().unwrap()).insert(app_name.to_string(), env.clone());
        env
    }

    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Env::default()
    }

    pub fn get_app_name(&self) -> String {
        let app_name = self.properties.get(APP_NAME).unwrap();
        app_name.clone()
    }

    pub fn get_app_model(&self) -> String {
        let app_model = self.properties.get(APP_MODEL).unwrap();
        app_model.clone()
    }

    pub fn get_mqtt_server(&self) -> String {
        if self.get_is_local_mqtt() {
            return "127.0.0.1".to_string();
        }
        let s = self.properties.get(MQTT_SERVER).unwrap().clone();
        let mut r = s.split(':');
        r.next().unwrap().to_string()
    }

    pub fn get_mqtt_server_port(&self) -> u16 {
        if self.get_is_local_mqtt() {
            return self.get_local_mqtt_port();
        }
        let s = self.properties.get(MQTT_SERVER).unwrap().clone();
        let r = s.split(':');
        r.last().unwrap().parse::<u16>().unwrap()
    }

    pub fn get_mqtt_user(&self) -> String {
        let s = self.properties.get(MQTT_AUTH).unwrap().clone();
        let mut r = s.split(':');
        if let Some(user_name) = r.next() {
            user_name.to_string()
        } else {
            "".to_string()
        }
    }

    pub fn get_mqtt_password(&self) -> String {
        let s = self.properties.get(MQTT_AUTH).unwrap().clone();
        let r = s.split(':');
        if let Some(password) = r.last() {
            password.to_string()
        } else {
            "".to_string()
        }
    }

    pub fn get_http_server_port(&self) -> u16 {
        self.properties
            .get(HTTP_SERVER_PORT)
            .unwrap()
            .parse::<u16>()
            .unwrap()
    }

    pub fn get_exe_root_dir(&self) -> String {
        self.properties.get(EXE_ROOT_DIR).unwrap().clone()
    }

    pub fn get_eig_home(&self) -> String {
        let s = self.properties.get(EIG_HOME).unwrap();
        if s.is_empty() {
            self.get_exe_root_dir()
        } else {
            s.clone()
        }
    }

    pub fn transform_path_to_absolute<P: AsRef<Path>>(&self, path: P) -> String {
        let path = path.as_ref();
        if path.is_relative() {
            let root_path = PathBuf::from(self.get_eig_home());
            root_path.join(path).to_str().unwrap().to_string()
        } else {
            path.to_str().unwrap().to_string()
        }
    }

    pub fn get_conf_path(&self) -> String {
        let path = self.properties.get(CONF_PATH).unwrap();
        self.transform_path_to_absolute(path.as_str())
    }

    pub fn get_point_dir(&self) -> String {
        if let Some(s) = self.get_property(POINT_FILE_DIR) {
            return s.to_string();
        }
        String::new()
    }

    pub fn get_transport_dir(&self) -> String {
        if let Some(s) = self.get_property(TRANSPORT_DIR) {
            return s.to_string();
        }
        String::new()
    }

    pub fn get_json_dir(&self) -> String {
        let path = self.properties.get(JSON_DIR).unwrap().to_owned();
        self.transform_path_to_absolute(path.as_str())
    }

    pub fn get_aoe_dir(&self) -> String {
        if let Some(s) = self.get_property(AOE_DIR) {
            return s.to_string();
        }
        String::new()
    }

    pub fn get_web_dir(&self) -> String {
        let path = self.properties.get(WEB_DIR).unwrap().to_owned();
        self.transform_path_to_absolute(path.as_str())
    }

    pub fn get_db_dir(&self) -> String {
        let path = self.properties.get(DB_DIR).unwrap().to_owned();
        self.transform_path_to_absolute(path.as_str())
    }

    pub fn get_log_dir(&self) -> String {
        let path = self.properties.get(LOG_DIR).unwrap().to_owned();
        self.transform_path_to_absolute(path.as_str())
    }

    pub fn get_mqtt_mv_limit(&self) -> usize {
        let s = self.properties.get(MQTT_MV_LIMIT).unwrap();
        s.trim().parse().unwrap()
    }

    pub fn get_is_local_frontend(&self) -> bool {
        let r = self.properties.get(IS_LOCAL_FRONTEND);
        match r {
            Some(s) => s.trim().to_uppercase() == "TRUE",
            None => true,
        }
    }

    pub fn get_is_db(&self) -> bool {
        let r = self.properties.get(IS_DB);
        match r {
            Some(s) => s.trim().to_uppercase() == "TRUE",
            None => true,
        }
    }

    pub fn get_is_his_db(&self) -> bool {
        let r = self.properties.get(IS_HIS_DB);
        match r {
            Some(s) => s.trim().to_uppercase() == "TRUE",
            None => true,
        }
    }

    pub fn get_is_fake_delete(&self) -> bool {
        let r = self.properties.get(IS_FAKE_DELETE);
        match r {
            Some(s) => s.trim().to_uppercase() == "TRUE",
            None => true,
        }
    }

    pub fn get_is_local_mqtt(&self) -> bool {
        let r = self.properties.get(IS_LOCAL_MQTT);
        match r {
            Some(s) => s.trim().to_uppercase() == "TRUE",
            None => false,
        }
    }

    pub fn get_local_mqtt_port(&self) -> u16 {
        let r = self.properties.get(LOCAL_MQTT_PORT);
        match r {
            Some(s) => s.trim().parse::<u16>().unwrap_or(1883),
            None => 1883,
        }
    }

    pub fn get_is_use_ssl(&self) -> bool {
        let r = self.properties.get(IS_USE_SSL);
        match r {
            Some(s) => s.trim().to_uppercase() == "TRUE",
            None => false,
        }
    }

    pub fn get_ssl_cert_file_path(&self) -> String {
        if let Some(s) = self.get_property(SSL_CERT_FILE_PATH) {
            return s.to_string();
        }
        String::new()
    }

    pub fn get_ssl_key_file_path(&self) -> String {
        if let Some(s) = self.get_property(SSL_KEY_FILE_PATH) {
            return s.to_string();
        }
        String::new()
    }

    pub fn get_mqtt_timeout(&self) -> u64 {
        let s = self.properties.get(MQTT_TIMEOUT).unwrap();
        s.trim().parse().unwrap()
    }

    pub fn get_properties(&self) -> HashMap<String, String> {
        self.properties.clone()
    }

    pub fn get_beeid(&self) -> String {
        let bee_id = self.get_property(BEE_ID).unwrap_or_default();

        if bee_id.is_empty() {
            "1234567887654321".to_string()
        } else {
            bee_id.to_string()
        }
    }

    pub fn get_database_url(&self) -> String {
        self.properties.get(DATABASE_URL).unwrap_or(&String::new()).clone()
    }

    pub fn get_plcc_server(&self) -> String {
        self.properties.get(PLCC_SERVER).unwrap_or(&String::new()).clone()
    }

    pub fn get_plcc_user(&self) -> String {
        self.properties.get(PLCC_USER).unwrap_or(&String::new()).clone()
    }

    pub fn get_plcc_pwd(&self) -> String {
        self.properties.get(PLCC_PWD).unwrap_or(&String::new()).clone()
    }

    pub fn get_default_properties() -> HashMap<String, String> {
        let mut properties: HashMap<String, String> = HashMap::with_capacity(16);

        let default_props = HashMap::from([
            (HTTP_SERVER_PORT, "80"),
            (POINT_FILE_DIR, "points.json"),
            (TRANSPORT_DIR, "transports.json"),
            (AOE_DIR, "aoes.json"),
            (JSON_DIR, "file"),
            (MQTT_SERVER, "localhost:1883"),
            (IS_LOCAL_MQTT, "false"),
            (LOCAL_MQTT_PORT, "1883"),
            (MQTT_TIMEOUT, "30"),

            (MQTT_MV_LIMIT, "1000"),
            (MQTT_AUTH, ""),
            (MQTT_CLIENT_BUF_SIZE, "100"),
            (MQTT_PACKAGE_MAX_SIZE, "2 MiB"),
            (SOCKET_BUF_SIZE_NORTH, "128 MiB"),//128MB
            (SOCKET_BUF_SIZE_SOUTH, "32 MiB"),//32MB
            // default dirs
            // (EXE_ROOT_DIR, "./"),
            (EIG_HOME, ""),
            (WEB_DIR, "www"),
            (DB_DIR, "db"),
            (LOG_DIR, "log"),

            // default eig config
            (APP_NAME, "ext.syy.plcc"),
            (APP_MODEL, "DC_PLCC"),
            (IS_LOCAL_FRONTEND, "true"),
            (IS_DB, "true"),
            (IS_HIS_DB, "true"),
            (IS_FAKE_DELETE, "false"),
            (DB_DIR_SIZE_LIMIT, "1 GB"),
            //default LogConfig
            (LOG_LEVEL, "info"),
            (LOG_SAVE_TIME, ""),//日志初始化时会初始化为1 day
            (LOG_SAVE_SIZE, ""),
            (LOG_HIS_FILE_NUM, "7"),
        ]);

        default_props.iter().for_each(|(k, v)| {
            properties.insert(k.to_string(), v.to_string());
        });

        let exe_root_dir = Env::get_exe_root();

        properties.insert(
            String::from(EXE_ROOT_DIR),
            exe_root_dir,
        );

        properties
    }

    fn init_with_path(config_path: &str) -> Env {
        //生成默认env
        let env = Env {
            properties: Env::get_default_properties(),
        };
        let mut env_new = env.clone();
        if Path::new(config_path).exists() {
            info!("Load config file from {config_path}", );
            read_file(config_path, &mut env_new.properties);
        } else {
            info!("no config file found at {config_path}, use default config", );
        }
        env_new.properties.insert(String::from(CONF_PATH), config_path.to_string());
        env_new
    }

    fn get_exe_root() -> String {
        let exe_path = env::current_exe().unwrap();
        //标准Microsoft规定的Windows路径以\\?\开头，不同Windows电脑返回的路径不一样，需要去掉。可以试用库dunce，效果与方法与此类似。
        #[cfg(windows)]
        let exe_path = std::path::PathBuf::from(exe_path.to_str().unwrap().replace("\\\\?\\", ""));
        exe_path.parent().unwrap().display().to_string()
    }

    fn init_with_args(_app_name: &str) -> Env {
        // 从参数或环境变量中加载config路径
        let config_path = Self::get_exe_root()+"/adapter";
        Env::init_with_path(&config_path)
    }

    pub fn update(app_name: &str, env: Env) {
        (*ENV.lock().unwrap()).insert(app_name.to_string(), env);
    }

    pub fn get_property(&self, key: &str) -> Option<&str> {
        if let Some(v) = self.properties.get(key) {
            Some(v.as_str())
        } else {
            None
        }
    }

    pub fn set_property(&mut self, key: &str, val: &str) {
        self.properties.insert(key.to_string(), val.to_string());
    }

    pub fn get_env(app_name: &str) -> Env {
        let env_hash = ENV.lock().unwrap();
        if app_name == "" && env_hash.len() == 1 {
            return env_hash.values().next().unwrap().clone();
        }
        if let Some(env) = env_hash.get(app_name) {
            return env.clone();
        }

        warn!("Env is not initialized, init now!");
        Env::init(app_name)
    }

    pub fn get_log_config(&self) -> LogConfig {
        let log_dir = self.get_log_dir();
        let log_level = self.get_property(LOG_LEVEL).unwrap().to_string();
        let log_save_time = self.get_property(LOG_SAVE_TIME).unwrap().to_string();
        let log_save_size = self.get_property(LOG_SAVE_SIZE).unwrap().to_string();
        let log_his_file_num = self.get_property(LOG_HIS_FILE_NUM).unwrap().to_string();
        LogConfig {
            log_dir,
            log_level,
            log_save_time,
            log_save_size,
            log_his_file_num,
            log_is_quiet: false,
        }
    }
}

fn read_buffed<R>(buffered: BufReader<R>, properties: &mut HashMap<String, String>)
where
    R: std::io::Read,
{
    // 存放`[key]`
    // let mut key: String = "".to_string();
    // 缓存 : 去掉空格
    // let mut new_line = "".to_string();

    for line_result in buffered.lines() {
        match line_result {
            Err(e) => {
                error!("!!Read config file line failed: {e:?}");
                continue;
            }
            Ok(line) => {
                // new_line.clear();
                // new_line.push_str(line.trim());
                // 定义注释为`#`, 遇到注释跳过
                if line.contains('#') {
                    continue;
                    // } else if line.contains('[') && line.contains(']') {
                    //     // 解析`[key]`为`key::`
                    //     key.clear();
                    //     new_line.pop();
                    //     new_line.remove(0);
                    //     key.push_str(new_line.as_str());
                    //     key.push_str("::");
                } else if line.contains('=') {
                    // 必须包含表达式, 才将值写入
                    let kvs: Vec<&str> = line.as_str().split('=').collect::<Vec<&str>>();
                    if kvs.len() == 2 {
                        if let Some(k) = get_parameters_name(kvs[0]) {
                            // 如果不满足则定义为异常数据，该数据暂不处理

                            // 缓存
                            // let mut new_key: String = key.clone();
                            // new_key.push_str(k.as_str());
                            properties.insert(k, kvs[1].trim().to_string());
                            continue;
                        }
                    }
                } else if line.trim().is_empty() {
                    continue;
                }
                warn!("!!Wrong config file line: {:?}", line);
            }
        }
    }
}

fn get_parameters_name(name_ori: &str) -> Option<String> {
    let name = name_ori.trim();
    if CONFIG_ARGS.contains(&name) {
        return Some(name.to_string());
    }
    let name_upper = name.chars().filter(|c| c.is_alphanumeric()).collect::<String>().to_ascii_uppercase();
    let config_args_upper = CONFIG_ARGS.iter().map(|p| p.to_ascii_uppercase()).collect::<Vec<String>>();
    let index = config_args_upper.iter().position(|s| s == &name_upper)?;
    Some(CONFIG_ARGS[index].to_string())
}

// 读取文件,path表示文件路径
pub fn read_file(path: &str, properties: &mut HashMap<String, String>) {
    // 读取文件，失败直接返回Err
    if let Ok(content) = std::fs::read(path) {
        read_env_from_bytes(content.as_slice(), properties);
    } else {
        error!("!!Read config file failed: {}", path);
    }
}

pub fn read_env_from_bytes(content: &[u8], properties: &mut HashMap<String, String>) {
    // 读取文件，失败直接返回Err
    let buffered: BufReader<&[u8]> = BufReader::new(content);
    read_buffed(buffered, properties);
}
