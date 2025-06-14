use actix_web::{post, get, HttpResponse, web};
use async_channel::{bounded, Sender};
use log::{info, warn};
use rocksdb::DB;
use std::fs::File;
use std::io::BufReader;
use std::collections::HashMap;

use crate::db::mydb;
use crate::{JSON_DATA_PATH, FILE_NAME_POINTS, FILE_NAME_TRANSPORTS, FILE_NAME_AOES};
use crate::model::north::MyTransports;
use crate::model::{ParserResult, points_to_south, transports_to_south, aoes_to_south};
use crate::utils::plccapi::{update_points, update_transports, update_aoes};
use crate::utils::mqttclient::do_query_dev;
use crate::db::dbutils::*;
use crate::utils::{param_point_map, point_param_map};

const POINT_TREE: &str = "point";
const DEV_TREE: &str = "dev";

pub const OPERATION_RECEIVE_BUFF_NUM: usize = 100;

pub enum ParserOperation {
    UpdateJson(Sender<ParserResult>),
    GetPointMapping(Sender<HashMap<u64, String>>),
    GetDevMapping(Sender<HashMap<String, String>>),
    // 退出数据库服务
    Quit,
}

struct ParserManager {
    inner_db: DB,
}

impl ParserManager {
    pub(crate) fn new(file_path: &str) -> Option<ParserManager> {
        // 保证不会重复打开错误的db，导致文件系统崩溃
        if mydb::is_error_db_path(file_path) {
            return None;
        }
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let cfs = [POINT_TREE, DEV_TREE];
        if let Ok(inner_db) = DB::open_cf(&opts, file_path, cfs) {
            Some(ParserManager { inner_db })
        } else {
            mydb::add_error_db_path(file_path);
            warn!("open db {:?} failed", file_path);
            None
        }
    }

    async fn do_operation(&self, op: ParserOperation) {
        match op {
            ParserOperation::UpdateJson(sender) => {
                let file_name_points = format!("{JSON_DATA_PATH}/{FILE_NAME_POINTS}");
                let file_name_transports = format!("{JSON_DATA_PATH}/{FILE_NAME_TRANSPORTS}");
                let file_name_aoes = format!("{JSON_DATA_PATH}/{FILE_NAME_AOES}");
                let mut points_mapping: HashMap<String, u64> = HashMap::default();
                let mut result = ParserResult{
                    result: true,
                    err: "".to_string()
                };
                let mut current_id = 65535_u64;
                match self.parse_points(file_name_points).await {
                    Ok(mapping) => points_mapping = mapping.into_iter().collect(),
                    Err(err) => {
                        result.result = false;
                        result.err = err;
                    }
                }
                match self.parse_transports(file_name_transports, &points_mapping).await {
                    Ok(id) => current_id = id,
                    Err(err) => {
                        result.result = false;
                        result.err = err;
                    }
                }
                match self.parse_aoes(file_name_aoes, &points_mapping, current_id).await {
                    Ok(()) => {},
                    Err(err) => {
                        result.result = false;
                        result.err = err;
                    }
                }
                // 保存到全局变量中
                param_point_map::save_all(points_mapping.clone());
                point_param_map::save_reversal(points_mapping);
                if let Err(e) = sender.send(result).await {
                    warn!("!!Failed to send update json : {e:?}");
                }
            }
            ParserOperation::GetPointMapping(sender) => {
                
            }
            ParserOperation::GetDevMapping(sender) => {
                
            }
            ParserOperation::Quit => {}
        }
    }

    async fn parse_points(&self, path: String) -> Result<Vec<(String, u64)>, String> {
        // 打开文件
        if let Ok(file) = File::open(path) {
            let reader = BufReader::new(file);
            // 反序列化为对象
            if let Ok(points) = serde_json::from_reader(reader) {
                let (new_points, points_mapping) = points_to_south(points)?;
                let _ = update_points(new_points).await?;
                self.save_point_mapping(&points_mapping);
                Ok(points_mapping)
            } else {
                Err("测点JSON反序列化失败".to_string())
            }
        } else {
            Err("测点JSON文件不存在".to_string())
        }
    }

    async fn parse_transports(&self, path: String, points_mapping: &HashMap<String, u64>) -> Result<u64, String> {
        // 打开文件
        if let Ok(file) = File::open(path) {
            let reader = BufReader::new(file);
            // 反序列化为对象
            if let Ok(transports) = serde_json::from_reader(reader) {
                let dev_guids = query_dev_guid(&transports).await?;
                let (new_transports, current_id) = transports_to_south(transports, &points_mapping, &dev_guids)?;
                let _ = update_transports(new_transports).await?;
                self.save_dev_mapping(&dev_guids);
                Ok(current_id)
            } else {
                Err("通道JSON反序列化失败".to_string())
            }
        } else {
            Err("通道JSON文件不存在".to_string())
        }
    }

    async fn parse_aoes(&self, path: String, points_mapping: &HashMap<String, u64>, current_id: u64) -> Result<(), String> {
        // 打开文件
        if let Ok(file) = File::open(path) {
            let reader = BufReader::new(file);
            // 反序列化为对象
            if let Ok(aoes) = serde_json::from_reader(reader) {
                
                let new_aoes= aoes_to_south(aoes, &points_mapping, current_id)?;
                let _ = update_aoes(new_aoes).await?;
                Ok(())
            } else {
                Err("策略JSON反序列化失败".to_string())
            }
        } else {
            Err("策略JSON文件不存在".to_string())
        }
    }

    fn save_point_mapping(&self, points_mapping: &Vec<(String, u64)>) {
        let mut keys = Vec::with_capacity(points_mapping.len());
        let mut values = Vec::with_capacity(points_mapping.len());
        for (ptag, pid) in points_mapping {
            keys.push(ptag.as_bytes().to_vec());
            values.push(pid.to_string().as_bytes().to_vec());
        }
        if save_items_to_db_with_tree_name(&self.inner_db, POINT_TREE, &keys, &values) {
            info!("insert point mapping success");
        } else {
            warn!("!!Failed to insert point mapping");
        }
    }

    fn save_dev_mapping(&self, dev_guids: &HashMap<String, String>) {
        let keys: Vec<Vec<u8>> = dev_guids.keys().map(|k| k.as_bytes().to_vec()).collect();
        let values: Vec<Vec<u8>> = dev_guids.values().map(|v| v.as_bytes().to_vec()).collect();
        if save_items_to_db_with_tree_name(&self.inner_db, DEV_TREE, &keys, &values) {
            info!("insert point mapping success");
        } else {
            warn!("!!Failed to insert point mapping");
        }
    }

}

pub fn start_parser_service(parser_db_dir: String) -> Sender<ParserOperation> {
    info!("start parser service job...");
    // 启动解析服务
    let (op_sender, op_receiver) = bounded(OPERATION_RECEIVE_BUFF_NUM);
    tokio::spawn(async move {
        if let Some(mut db) = ParserManager::new(&parser_db_dir) {
            loop {
                match op_receiver.recv().await {
                    Ok(op) => {
                        if matches!(op, ParserOperation::Quit) {
                            break;
                        }
                        db.do_operation(op).await;
                    }
                    Err(e) => {
                        warn!("!!Error occurs when listening new db operation, err: {e:?}");
                        break;
                    }
                }
            }
        } else {
            warn!("!!Failed to start common service because of db file not found");
        }
    });
    op_sender
}

#[get("/api/v1/parser/update_json")]
async fn update_json(
    sender: web::Data<Sender<ParserOperation>>,
) -> HttpResponse {
    let (tx, rx) = bounded(1);
    if let Ok(()) = sender.send(ParserOperation::UpdateJson(tx)).await {
        if let Ok(r) = rx.recv().await {
            return HttpResponse::Ok().content_type("application/json").json(r);
        }
    }
    HttpResponse::RequestTimeout().finish()
}

#[get("/api/v1/parser/point_mapping")]
async fn get_point_mapping(
    sender: web::Data<Sender<ParserOperation>>,
) -> HttpResponse {
    let (tx, rx) = bounded(1);
    if let Ok(()) = sender.send(ParserOperation::GetPointMapping(tx)).await {
        if let Ok(r) = rx.recv().await {
            return HttpResponse::Ok().content_type("application/json").json(r);
        }
    }
    HttpResponse::RequestTimeout().finish()
}

#[get("/api/v1/parser/dev_mapping")]
async fn get_dev_mapping(
    sender: web::Data<Sender<ParserOperation>>,
) -> HttpResponse {
    let (tx, rx) = bounded(1);
    if let Ok(()) = sender.send(ParserOperation::GetDevMapping(tx)).await {
        if let Ok(r) = rx.recv().await {
            return HttpResponse::Ok().content_type("application/json").json(r);
        }
    }
    HttpResponse::RequestTimeout().finish()
}

pub fn config_parser_web_service(cfg: &mut web::ServiceConfig) {
    // 开放控制接口
    cfg.service(update_json)
    .service(get_point_mapping)
    .service(get_dev_mapping);
}

async fn query_dev_guid(transports: &MyTransports) -> Result<HashMap<String, String>, String> {
    if transports.transports.is_empty() {
        return Err("通道为空".to_string());
    }
    let dev_ids = transports.transports.iter().map(|t|t.dev_id()).collect::<Vec<_>>();
    let (host, port) = transports.transports.first().unwrap().mqtt_broker();
    do_query_dev("plcc_dev", &host, port, dev_ids).await
    // let mut a = HashMap::new();
    // a.insert("dev1".to_string(), "a_guid".to_string());
    // Ok(a)
}
