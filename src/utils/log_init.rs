use std::io::prelude::*;
use std::path::Path;
use std::{fs::create_dir_all, path::PathBuf};

use log::warn;

const LOG_CONFIG_FILE_NAME: &str = "log_config.yaml";

#[cfg(target_os = "windows")]
const SLASH: &str = "\\\\";
#[cfg(not(target_os = "windows"))]
const SLASH: &str = "/";

#[derive(Debug, Clone)]
pub struct LogConfig {
    pub log_dir: String,
    pub log_level: String,
    pub log_save_time: String,
    pub log_save_size: String,
    pub log_his_file_num: String,
    pub log_is_quiet: bool,
}

fn create_log_config_bytes_rolling_file(app_name: &str, config: &LogConfig) -> String {
    let cfg = r#"
refresh_rate: 30 seconds
appenders:
  stderr:
    kind: console
    target: stderr
    encoder:
      pattern: "[{d(%Y-%m-%dT%H:%M:%S%.6f)} {h({l}):<5.5} {M}] {m}{n}"
  rollingfile:
    kind: rolling_file
    path: "{LOG_PATH}{SLASH}{APP_NAME}.log"
    encoder:
      pattern: "[{d(%Y-%m-%dT%H:%M:%S%.6f)} {h({l}):<5.5} {M}] {m}{n}"
    policy:
{TIME_TRIGER}{SIZE_TRIGER}
      roller:
        kind: fixed_window
        pattern: "{LOG_PATH}{SLASH}{APP_NAME}-history-{}.log.gz"
        base: 0
        count: {LOG_HIS_FILE_NUM}
#  rollingfile_warn:
#    kind: rolling_file
#    path: "{LOG_PATH}{SLASH}{APP_NAME}-warn.log"
#    encoder:
#      pattern: "[{d(%Y-%m-%dT%H:%M:%S%.6f)} {h({l}):<5.5} {M}] {m}{n}"
#    policy:
#      trigger:
#        kind: time
#        interval: {LOG_SAVE_TIME}
#      roller:
#        kind: fixed_window
#        pattern: "{LOG_PATH}{SLASH}{APP_NAME}-warn-history-{}.log.gz"
#        base: 0
#        count: {LOG_HIS_FILE_NUM}
#    filters:
#        - kind: threshold
#          level: warn
root:
  level: {LOG_LEVEL}
  appenders:
{STDERR}
    - rollingfile
#    - rollingfile_warn
loggers:
    rumqttd:
        level: off
        additive: false
"#;
    let str_err = r#"    - stderr"#;
    let time_triger = r#"
      trigger:
        kind: time
        interval: {LOG_SAVE_TIME}"#;
    let size_triger: &str = r#"
      trigger:
        kind: size
        limit: {LOG_SAVE_SIZE}"#;

    let mut config = config.to_owned();
    let cfg = if !config.log_is_quiet {
        cfg.replace("{STDERR}", str_err)
    } else {
        cfg.replace("{STDERR}", "")
    };

    let cfg = if !config.log_save_size.is_empty() && !config.log_save_time.is_empty() {
        warn!("log_save_size and log_save_time can not be set at the same time, log_save_time will be ignored");
        cfg.replace("{SIZE_TRIGER}", size_triger)
        .replace("{TIME_TRIGER}", "")
    } else if !config.log_save_size.is_empty() {
        cfg.replace("{SIZE_TRIGER}", size_triger)
            .replace("{TIME_TRIGER}", "")
    } else if !config.log_save_time.is_empty() {
        cfg.replace("{SIZE_TRIGER}", "")
            .replace("{TIME_TRIGER}", time_triger)
    } else {
        config.log_save_time = "1 day".to_string();
        cfg.replace("{SIZE_TRIGER}", "")
            .replace("{TIME_TRIGER}", time_triger)
    };

    let log_dir = config.log_dir
        .replace('\\', "\\\\");

    cfg.replace("{LOG_HIS_FILE_NUM}", config.log_his_file_num.as_str())
        .replace("{LOG_LEVEL}", config.log_level.as_str())
        .replace("{LOG_SAVE_TIME}", config.log_save_time.as_str())
        .replace("{LOG_SAVE_SIZE}", config.log_save_size.as_str())
        .replace("{LOG_PATH}", log_dir.as_str())
        .replace("{APP_NAME}", app_name)
        .replace("{SLASH}", SLASH)
}

fn create_log_config_bytes_only_console(log_level: &str) -> String {
    let cfg = r#"
refresh_rate: 1 seconds
appenders:
  stderr:
    kind: console
    target: stderr
    encoder:
      pattern: "[{d(%Y-%m-%dT%H:%M:%S%.6f)} {h({l}):<5.5} {M}] {m}{n}"
root:
  level: {LOG_LEVEL}
  appenders:
    - stderr
loggers:
    rumqttd:
        level: off
        additive: false
"#;

    cfg.replace("{LOG_LEVEL}", log_level)
}

fn write_to_file<P: AsRef<Path>>(path: P, content: &str) -> std::io::Result<()> {
    let path = path.as_ref();
    let path_parent = path.parent().unwrap();
    if !path_parent.exists() && create_dir_all(path_parent).is_err() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "create dir failed",
        ));
    }
    match std::fs::File::create(path) {
        Ok(mut file) => {
            file.write_all(content.as_bytes())?;
            file.flush()?;
        } 
        _ => {
            warn!("create file failed");
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "create file failed",
            ));
        }
    };

    Ok(())
}

pub fn get_log_config_file_path(app_name: &str) -> PathBuf {
    let cache_dir = PathBuf::from(crate::env::Env::get_env(app_name).get_log_dir()).join("tmp");
    let user = if cfg!(target_os = "linux") {
        std::env::var("USER").unwrap_or_default()
    } else if cfg!(target_os = "windows") {
        std::env::var("USERNAME").unwrap_or_default()
    } else {
        String::new()
    };
    let dir_name = app_name.to_string() + &user;
    let target_path = cache_dir.join(dir_name);

    target_path.join(LOG_CONFIG_FILE_NAME)
}

fn refresh_log_config_rolling_file<P:AsRef<Path>>(app_name: &str, path:P, config: &LogConfig) {

    let content = create_log_config_bytes_rolling_file(app_name, config);
    if write_to_file(path, content.as_str()).is_err() {
        warn!("write log config file failed");
    };
}

fn refresh_log_config_only_console<P:AsRef<Path>>(path:P) {
    let content = create_log_config_bytes_only_console("info");

    if write_to_file(path, content.as_str()).is_err() {
        warn!("write log config file failed");
    };
}

pub fn log_config_init(app_name: &str) -> (bool, String) {
    let path = get_log_config_file_path(app_name);
    let is_new = !path.exists();
    refresh_log_config_only_console(path.clone());
    let path = path
        .to_str()
        .unwrap()
        .to_string();
    (is_new, path)
}

pub fn write_log_config(app_name: &str, config: &LogConfig) -> (bool, String) {
    let path = get_log_config_file_path(app_name);
    let is_new = !path.exists();
    refresh_log_config_rolling_file(app_name, path.clone(), config);
    let path = path
        .to_str()
        .unwrap()
        .to_string();
    (is_new, path)
}

#[cfg(test)]
impl Default for LogConfig {
    fn default() -> Self {
        LogConfig {
            log_dir: "log".to_string(),
            log_level: "info".to_string(),
            log_save_time: "1 day".to_string(),
            log_save_size: "".to_string(),
            log_his_file_num: "7".to_string(),
            log_is_quiet: false,
        }
    }
}

#[test]
fn test() {
    use log4rs;

    let config = LogConfig::default();
    // let file = create_log_config(config);
    let file = get_log_config_file_path("plcc");
    write_log_config("plcc", &config);
    log4rs::init_file(file, Default::default()).unwrap();
}

#[test]
fn test2() {
    use log4rs;
    // 初始化配置
    let env = crate::env::Env::init("");
    let mut log_config = env.get_log_config();
    log_config.log_dir = std::env::temp_dir().join("eigtest").to_str().unwrap().to_string();

    // 清空历史日志
    let log_file = log_config.log_dir.clone() + "/plcc.log";
    if PathBuf::from(log_file.clone()).exists() {
        std::fs::remove_file(&log_file).unwrap();
    }
    std::thread::sleep(std::time::Duration::from_secs(1)); //TODO delay to make file be refreshed

    // 初始化日志
    let (_, log_config_file) = log_config_init("plcc");
    log4rs::init_file(log_config_file.as_str(), Default::default()).unwrap();

    //重新更新日志配置
    std::thread::sleep(std::time::Duration::from_secs(1)); //TODO delay to make file be refreshed
    write_log_config("plcc", &log_config);
    std::thread::sleep(std::time::Duration::from_secs(1)); //TODO
    log::trace!("TRACE LOG IS ON");
    log::debug!("DEBUG LOG IS ON");
    log::info!("INFO LOG IS ON");
    warn!("WARN LOG IS ON");
    log::error!("ERROR LOG IS ON");

    let content = String::from_utf8(std::fs::read(log_file).unwrap()).unwrap();
    let result = content.lines().map(|line| {
        let tmps: Vec<_> = line.splitn(2, ']').collect();
        tmps[1].trim().to_string()
    }).collect::<Vec<_>>().join("\n");
    
    let result_correct_info = "INFO LOG IS ON\nWARN LOG IS ON\nERROR LOG IS ON".to_string();

    assert_eq!(result, result_correct_info);
}

#[test]
fn test_time_size_triger() {
    use log4rs;

    // 初始化配置
    let env = crate::env::Env::init("");
    let mut log_config = env.get_log_config();
    log_config.log_save_time = "1 second".to_string();
    log_config.log_save_size = "1 MB".to_string();

    // 初始化日志
    let (_, log_config_file) = log_config_init("plcc");
    log4rs::init_file(log_config_file.as_str(), Default::default()).unwrap();

    //重新更新日志配置
    std::thread::sleep(std::time::Duration::from_secs(1)); //TODO delay to make file be refreshed
    write_log_config("plcc", &log_config);
    std::thread::sleep(std::time::Duration::from_secs(1)); //TODO
    log::trace!("TRACE LOG IS ON");
    log::debug!("DEBUG LOG IS ON");
    log::info!("INFO LOG IS ON");
    warn!("WARN LOG IS ON");
    log::error!("ERROR LOG IS ON");
    
    for _ in 0..10 {
        std::thread::sleep(std::time::Duration::from_secs(1)); 
        log::info!("INFO LOG IS ON");
    }
}