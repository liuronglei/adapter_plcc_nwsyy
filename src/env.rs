use std::{collections::HashMap, fmt::Debug};
use std::env;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};

use log::{error, info, warn};

// const DEFAULT_CONFIG_PATH : &str = "config";
// for config
pub const CONF_PATH: &str = "configFilePath";
pub const PLCC_SERVER: &str = "plccServer";
pub const PLCC_USER: &str = "plccUser";
pub const PLCC_PWD: &str = "plccPwd";
pub const MQTT_SERVER: &str = "mqttServer";
pub const HTTP_SERVER_PORT: &str = "httpServerPort";
pub const POINT_FILE_DIR: &str = "pointFileDir";
pub const TRANSPORT_DIR: &str = "transportFileDir";
pub const AOE_DIR: &str = "aoeFileDir";
pub const JSON_DIR: &str = "jsonFileDir";

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
pub const DOCKERS_CONFIG_DIR: &str = "dockersConfigDir";
pub const FRONT_ZIP_FILE: &str = "frontZipFile";
// store plugins and scripts files
pub const FRONT_FILE_DIR: &str = "frontFileDir";
pub const DOCKERS_FRONT_FILE_DIR: &str = "dockersFrontFileDir";

// 开启SSL证书
pub const IS_USE_SSL: &str = "isUseSsl";
pub const SSL_CERT_FILE_PATH: &str = "sslCertFilePath";
pub const SSL_KEY_FILE_PATH: &str = "sslKeyFilePath";

pub const EIG_NAME: &str = "eigName";
pub const EIG_DESC: &str = "eigDesc";
// 用于plcc配置ems地址
pub const EIG_PROXY_ADDR: &str = "eigProxyAddress";
pub const EIG_RT_MSG_PROXY_ADDR: &str = "eigRtMsgProxyAddress";
pub const EIG_IPINFO1: &str = "eigIpInfo1";
pub const EIG_IPINFO2: &str = "eigIpInfo2";
pub const EIG_IPINFO3: &str = "eigIpInfo3";
pub const EIG_IPINFO4: &str = "eigIpInfo4";
pub const EIG_IPINFO5: &str = "eigIpInfo5";
pub const EIG_IPINFO6: &str = "eigIpInfo6";
pub const EIG_IPINFO7: &str = "eigIpInfo7";
pub const EIG_IPINFO8: &str = "eigIpInfo8";
pub const EIG_IPINFO9: &str = "eigIpInfo9";
pub const EIG_IPINFO10: &str = "eigIpInfo10";
pub const CLOUD_URL1: &str = "cloudURL1";
pub const CLOUD_URL2: &str = "cloudURL2";
pub const CLOUD_URL3: &str = "cloudURL3";
pub const CLOUD_URL4: &str = "cloudURL4";
pub const CLOUD_URL5: &str = "cloudURL5";
pub const CLOUD_URL6: &str = "cloudURL6";
pub const CLOUD_CA1: &str = "cloudCA1";
pub const CLOUD_CA2: &str = "cloudCA2";
pub const CLOUD_CA3: &str = "cloudCA3";
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
// Listen Port
// ems与ems之间连接的端口
pub const EMS_LISTEN_PORT: &str = "emsListenPort";
pub const EMS_RTMSG_LISTEN_PORT: &str = "emsRtMsgListenPort";
// 镜像与ems或plcc连接的端口
// ems与plcc连接的端口
pub const LCC_LISTEN_PORT: &str = "lccListenPort";
pub const LCC_RTMSG_LISTEN_PORT: &str = "lccRtMsgListenPort";
// const UDP_BROADCAST_PORT: &str = "udpBroadcastPort";
// const UDP_LISTEN_PORT: &str = "udpListenPort";

// used for standby-active mechanism
pub const STANDBY_LCC_IP: &str = "standbyLccIp";
pub const STANDBY_LCC_BEEID: &str = "standbyLccId";

pub const MEAS_SOURRE_NUM: &str = "measSourceNum";

pub const DATABASE_URL: &str = "databaseUrl";
pub const WASI_FS_ROOT: &str = "wasiFileRoot";

// finder related properties
pub const FINDER_UDP_LISTEN_PORT: &str = "finderUdpListenPort";
pub const FINDER_UDP_BROADCAST_PORT: &str = "finderUdpBroadcastPort";
pub const FINDER_SLEEP_MILLI_BEFORE_RECONNECT: &str = "finderSleepMilliBeforeReconnect";
pub const FINDER_SERIAL_PATH: &str = "eigSerialPath";
pub const FINDER_SERIAL_BAUD: &str = "finderSerialBaud";


const CONFIG_ARGS: [&str; 77] = [CONF_PATH, BEE_ID, MQTT_SERVER, MQTT_AUTH, HTTP_SERVER_PORT,
    MQTT_CLIENT_BUF_SIZE, MQTT_MV_LIMIT, SOCKET_BUF_SIZE_NORTH, SOCKET_BUF_SIZE_SOUTH, EXE_ROOT_DIR,
    POINT_FILE_DIR, TRANSPORT_DIR, JSON_DIR, AOE_DIR, WEB_DIR, DB_DIR, LOG_DIR, DOCKERS_CONFIG_DIR,
    FRONT_ZIP_FILE, FRONT_FILE_DIR,DOCKERS_FRONT_FILE_DIR,
    EIG_NAME, EIG_DESC, EIG_PROXY_ADDR, EIG_RT_MSG_PROXY_ADDR, EIG_IPINFO1, EIG_IPINFO2,
    EIG_IPINFO3, EIG_IPINFO4, EIG_IPINFO5, EIG_IPINFO6, EIG_IPINFO7, EIG_IPINFO8, EIG_IPINFO9, EIG_IPINFO10,
    CLOUD_URL1, CLOUD_URL2, CLOUD_URL3, CLOUD_URL4, CLOUD_URL5, CLOUD_URL6, CLOUD_CA1, CLOUD_CA2,
    CLOUD_CA3, MQTT_PACKAGE_MAX_SIZE, IS_LOCAL_FRONTEND, IS_DB, IS_HIS_DB, IS_FAKE_DELETE,
    DB_DIR_SIZE_LIMIT, IS_LOCAL_MQTT, LOCAL_MQTT_PORT, LOG_LEVEL, LOG_SAVE_TIME, LOG_SAVE_SIZE,
    LOG_HIS_FILE_NUM, EMS_LISTEN_PORT, EMS_RTMSG_LISTEN_PORT, LCC_LISTEN_PORT, LCC_RTMSG_LISTEN_PORT,
    STANDBY_LCC_IP, STANDBY_LCC_BEEID, MEAS_SOURRE_NUM, DATABASE_URL, WASI_FS_ROOT, PLCC_SERVER,
    PLCC_USER, PLCC_PWD, EIG_HOME, IS_USE_SSL, SSL_CERT_FILE_PATH, SSL_KEY_FILE_PATH,
    FINDER_SERIAL_PATH, FINDER_SERIAL_BAUD, FINDER_UDP_LISTEN_PORT, FINDER_UDP_BROADCAST_PORT, FINDER_SLEEP_MILLI_BEFORE_RECONNECT];

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

    pub fn get_eig_name(&self) -> String {
        let eig_name = self.properties.get(EIG_NAME).unwrap();
        eig_name.clone()
    }

    pub fn get_eig_desc(&self) -> String {
        let eig_desc = self.properties.get(EIG_DESC).unwrap();
        eig_desc.clone()
    }

    pub fn get_finder_serial_path(&self) -> String {
        self.properties.get(FINDER_SERIAL_PATH).unwrap().clone()
    }

    pub fn get_finder_serial_baud(&self) -> u32 {
        let r = self.properties.get(FINDER_SERIAL_BAUD).unwrap().clone();
        r.parse().unwrap()
    }

    pub fn get_finder_udp_listen_port(&self) -> u16 {
        let r = self.properties.get(FINDER_UDP_LISTEN_PORT).unwrap().clone();
        r.parse().unwrap()
    }

    pub fn get_finder_udp_broadcast_port(&self) -> u16 {
        let r = self.properties.get(FINDER_UDP_BROADCAST_PORT).unwrap().clone();
        r.parse().unwrap()
    }

    pub fn get_finder_sleep_milli_before_reconnect(&self) -> u64 {
        let r = self.properties.get(FINDER_SLEEP_MILLI_BEFORE_RECONNECT).unwrap().clone();
        r.parse().unwrap()
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

    pub fn get_front_zip_file(&self) -> String {
        let path = self.properties.get(FRONT_ZIP_FILE).unwrap().to_owned();
        self.transform_path_to_absolute(path.as_str())
    }

    pub fn get_front_dir(&self) -> String {
        let path = self.properties.get(FRONT_FILE_DIR).unwrap().to_owned();
        let path = self.transform_path_to_absolute(path.as_str());
        if !PathBuf::from(path.as_str()).exists() {
            error!("front file dir not exists: {}", path.as_str());
            std::fs::create_dir_all(path.as_str()).unwrap_or_else(|e| {
                error!("create front file dir error: {}", e);
            });
        }
        path
    }

    pub fn get_docker_front_dir(&self) -> String {
        let path = self.properties.get(DOCKERS_FRONT_FILE_DIR).unwrap().to_owned();
        if !PathBuf::from(path.as_str()).exists() {
            error!("front file dir not exists: {}", path.as_str());
            std::fs::create_dir_all(path.as_str()).unwrap_or_else(|e| {
                error!("create front file dir error: {}", e);
            });
        }
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

    pub fn get_dockers_config_dir(&self) -> String {
        let path = self.properties.get(DOCKERS_CONFIG_DIR).unwrap().to_owned();
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

    pub fn get_proxy_addr(&self) -> String {
        if let Some(s) = self.get_property(EIG_PROXY_ADDR) {
            // if !s.is_empty() && is_valid_address(s) {
            return s.to_string();
            // }
        }
        String::new()
    }

    pub fn get_rt_msg_proxy_addr(&self) -> String {
        if let Some(s) = self.get_property(EIG_RT_MSG_PROXY_ADDR) {
            //if !s.is_empty() && is_valid_address(s) {
            return s.to_string();
            //}
        }
        String::new()
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

    pub fn get_measure_source_num(&self) -> u32 {
        let s = self.properties.get(MEAS_SOURRE_NUM).unwrap();
        s.trim().parse().unwrap()
    }

    // pub fn get_udp_broadcast_port(&self) -> u16 {
    //     let s = self.properties.get(UDP_BROADCAST_PORT).unwrap();
    //     s.trim().parse().unwrap()
    // }

    // pub fn get_udp_listen_port(&self) -> u16 {
    //     let s = self.properties.get(UDP_LISTEN_PORT).unwrap();
    //     s.trim().parse().unwrap()
    // }

    pub fn get_ems_listen_port(&self) -> u16 {
        let s = self.properties.get(EMS_LISTEN_PORT).unwrap();
        s.trim().parse().unwrap()
    }

    pub fn get_ems_rtmsg_listen_port(&self) -> u16 {
        let s = self.properties.get(EMS_RTMSG_LISTEN_PORT).unwrap();
        s.trim().parse().unwrap()
    }

    pub fn get_lcc_listen_port(&self) -> u16 {
        let s = self.properties.get(LCC_LISTEN_PORT).unwrap();
        s.trim().parse().unwrap()
    }

    pub fn get_lcc_rtmsg_listen_port(&self) -> u16 {
        let s = self.properties.get(LCC_RTMSG_LISTEN_PORT).unwrap();
        s.trim().parse().unwrap()
    }

    pub fn get_standby_lcc_ip(&self) -> String {
        self.properties.get(STANDBY_LCC_IP).unwrap_or(&String::new()).clone()
    }
    pub fn get_standby_lcc_beeid(&self) -> String {
        self.properties.get(STANDBY_LCC_BEEID).unwrap_or(&String::new()).clone()
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

    pub fn get_wasi_fs_root(&self) -> String {
        self.properties.get(WASI_FS_ROOT).unwrap_or(&String::from(".")).clone()
    }

    pub fn get_default_properties() -> HashMap<String, String> {
        let mut properties: HashMap<String, String> = HashMap::with_capacity(16);

        let default_props = HashMap::from([
            (MQTT_SERVER, "localhost:1883"),
            (MQTT_AUTH, ""),
            (MQTT_CLIENT_BUF_SIZE, "100"),
            (MQTT_PACKAGE_MAX_SIZE, "2 MiB"),
            (IS_LOCAL_MQTT, "false"),
            (LOCAL_MQTT_PORT, "1883"),
            (MQTT_MV_LIMIT, "1000"),
            (HTTP_SERVER_PORT, "80"),
            (SOCKET_BUF_SIZE_NORTH, "128 MiB"),//128MB
            (SOCKET_BUF_SIZE_SOUTH, "32 MiB"),//32MB

            (MEAS_SOURRE_NUM, "0"),
            // default dirs
            // (EXE_ROOT_DIR, "./"),
            (EIG_HOME, ""),
            (POINT_FILE_DIR, "points"),
            (TRANSPORT_DIR, "transports"),
            (AOE_DIR, "aoes"),
            (JSON_DIR, "svgs"),
            (WEB_DIR, "www"),
            (DB_DIR, "db"),
            (LOG_DIR, "log"),
            // (CONF_PATH, "config"),
            // (CONF_X_PATH, "config-x"),
            (DOCKERS_CONFIG_DIR, "dockers/configs"),
            (FRONT_ZIP_FILE, "www.zip"),
            (FRONT_FILE_DIR, "resources"),
            (DOCKERS_FRONT_FILE_DIR, "dockers/resources"),

            // default eig config
            (EIG_NAME, ""),
            (EIG_DESC, ""),
            (IS_LOCAL_FRONTEND, "true"),
            (IS_DB, "true"),
            (IS_HIS_DB, "true"),
            (IS_FAKE_DELETE, "false"),
            (DB_DIR_SIZE_LIMIT, "1 GB"),
            (EIG_PROXY_ADDR, ""),
            (EIG_RT_MSG_PROXY_ADDR, ""),
            //default LogConfig
            (LOG_LEVEL, "info"),
            (LOG_SAVE_TIME, ""),//日志初始化时会初始化为1 day
            (LOG_SAVE_SIZE, ""),
            (LOG_HIS_FILE_NUM, "7"),
            // Listen Port
            // (UDP_BROADCAST_PORT, "7054"),
            // (UDP_LISTEN_PORT, "7055"),
            (LCC_LISTEN_PORT, "7056"),
            (LCC_RTMSG_LISTEN_PORT, "7057"),
            (EMS_LISTEN_PORT, "7058"),
            (EMS_RTMSG_LISTEN_PORT, "7059"),
            // standby setting
            (STANDBY_LCC_IP, ""),
            (STANDBY_LCC_BEEID, ""),
            (WASI_FS_ROOT, "."),
            (FINDER_SERIAL_PATH, ""),
            (FINDER_SERIAL_BAUD, "115200"),
            (FINDER_UDP_LISTEN_PORT, "7054"),
            (FINDER_UDP_BROADCAST_PORT, "7055"),
            (FINDER_SLEEP_MILLI_BEFORE_RECONNECT, "5000"),
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
        let mut env = Env {
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

    fn init_with_args(app_name: &str) -> Env {
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
