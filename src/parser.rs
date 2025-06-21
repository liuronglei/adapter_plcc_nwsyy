use actix_web::{get, HttpResponse, web};
use async_channel::{bounded, Sender};
use log::{info, warn};
use rocksdb::DB;
use std::fs::File;
use std::io::BufReader;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};

use crate::db::mydb;
use crate::model::datacenter::QueryDevResponseBody;
use crate::ADAPTER_NAME;
use crate::model::north::{MyTransports, PointParam};
use crate::model::{ParserResult, points_to_south, transports_to_south, aoes_to_south,};
use crate::utils::plccapi::{update_points, update_transports, update_aoes, do_reset};
use crate::utils::mqttclient::{do_query_dev, do_data_query};
use crate::db::dbutils::*;
use crate::utils::{param_point_map, point_param_map};
use crate::env::Env;

const POINT_TREE: &str = "point";
const AOE_TREE: &str = "aoe";
const DEV_TREE: &str = "dev";

pub const OPERATION_RECEIVE_BUFF_NUM: usize = 100;

pub enum ParserOperation {
    UpdateJson(Sender<ParserResult>),
    GetPointMapping(Sender<HashMap<String, u64>>),
    GetDevMapping(Sender<Vec<QueryDevResponseBody>>),
    GetAoeMapping(Sender<HashMap<u64, u64>>),
    // 退出数据库服务
    Quit,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct AoeMapping {
    pub sid: u64,
    pub nid: u64,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct PointMapping {
    pub sid: u64,
    pub nid: String,
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
        let cfs = [POINT_TREE, DEV_TREE, AOE_TREE];
        if let Ok(inner_db) = DB::open_cf(&opts, file_path, cfs) {
            Some(ParserManager { inner_db })
        } else {
            mydb::add_error_db_path(file_path);
            log::error!("open db {:?} error", file_path);
            None
        }
    }

    async fn do_operation(&self, op: ParserOperation) {
        let env = Env::get_env(ADAPTER_NAME);
        let json_dir = env.get_json_dir();
        let point_dir = env.get_point_dir();
        let transport_dir = env.get_transport_dir();
        let aoe_dir = env.get_aoe_dir();
        match op {
            ParserOperation::UpdateJson(sender) => {
                let file_name_points = format!("{json_dir}/{point_dir}");
                let file_name_transports = format!("{json_dir}/{transport_dir}");
                let file_name_aoes = format!("{json_dir}/{aoe_dir}");
                let old_point_mapping = self.query_point_mapping();
                let mut points_mapping: HashMap<String, u64> = HashMap::default();
                let mut point_param: HashMap<String, PointParam> = HashMap::default();
                let mut point_discrete: HashMap<String, bool> = HashMap::default();
                let mut result = ParserResult {
                    result: true,
                    err: "".to_string()
                };
                let mut current_id = 65535_u64;
                log::info!("start parse point.json");
                match self.parse_points(file_name_points, &old_point_mapping).await {
                    Ok((mapping, param, discrete)) => {
                        points_mapping = mapping;
                        point_param = param;
                        point_discrete = discrete;
                    },
                    Err(err) => {
                        log::error!("{err}");
                        result.result = false;
                        let new_err = if result.err.is_empty() {err} else {format!(";{err}")};
                        result.err.push_str(&new_err);
                    }
                }
                log::info!("end parse point.json");
                log::info!("start parse transports.json");
                match self.parse_transports(file_name_transports, &points_mapping, &point_param, &point_discrete).await {
                    Ok(id) => current_id = id,
                    Err(err) => {
                        log::error!("{err}");
                        result.result = false;
                        let new_err = if result.err.is_empty() {err} else {format!(";{err}")};
                        result.err.push_str(&new_err);
                    }
                }
                log::info!("end parse transports.json");
                log::info!("start parse aoes.json");
                match self.parse_aoes(file_name_aoes, &points_mapping, current_id).await {
                    Ok(_) => {},
                    Err(err) => {
                        log::error!("{err}");
                        result.result = false;
                        let new_err = if result.err.is_empty() {err} else {format!(";{err}")};
                        result.err.push_str(&new_err);
                    }
                }
                log::info!("end parse aoes.json");
                log::info!("start do reset");
                match do_reset().await {
                    Ok(()) => {},
                    Err(err) => {
                        log::error!("{err}");
                        result.result = false;
                        let new_err = if result.err.is_empty() {err} else {format!(";{err}")};
                        result.err.push_str(&new_err);
                    }
                }
                log::info!("end do reset");
                log::info!("start do query_data mqtt");
                // 等待2秒
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                match do_data_query().await {
                    Ok(()) => {},
                    Err(err) => {
                        log::error!("{err}");
                        result.result = false;
                        let new_err = if result.err.is_empty() {err} else {format!(";{err}")};
                        result.err.push_str(&new_err);
                    },
                }
                log::info!("end do query_data mqtt");
                // 保存到全局变量中
                param_point_map::save_all(points_mapping.clone());
                point_param_map::save_reversal(points_mapping);
                if let Err(e) = sender.send(result).await {
                    warn!("!!Failed to send update json : {e:?}");
                }
            }
            ParserOperation::GetPointMapping(sender) => {
                if let Err(e) = sender.send(self.query_point_mapping()).await {
                    warn!("!!Failed to send get aoe_mapping : {e:?}");
                }
            }
            ParserOperation::GetAoeMapping(sender) => {
                if let Err(e) = sender.send(self.query_aoe_mapping()).await {
                    warn!("!!Failed to send get aoe_mapping : {e:?}");
                }
            }
            ParserOperation::GetDevMapping(sender) => {
                if let Err(e) = sender.send(self.query_dev_mapping()).await {
                    warn!("!!Failed to send get dev_mapping : {e:?}");
                }
            }
            ParserOperation::Quit => {}
        }
    }

    async fn parse_points(&self, path: String, old_point_mapping: &HashMap<String, u64>) -> Result<(HashMap<String, u64>, HashMap<String, PointParam>, HashMap<String, bool>), String> {
        // 打开文件
        if let Ok(file) = File::open(path) {
            let reader = BufReader::new(file);
            // 反序列化为对象
            match serde_json::from_reader(reader) {
                Ok(points) => {
                    let (new_points, points_mapping, point_param, point_discrete) = points_to_south(points, old_point_mapping)?;
                    let _ = update_points(new_points).await?;
                    self.replace_point_mapping(old_point_mapping, &points_mapping);
                    Ok((points_mapping, point_param, point_discrete))
                },
                Err(err) => Err(format!("测点JSON反序列化失败：{err}"))
            }
        } else {
            Err("测点JSON文件不存在".to_string())
        }
    }

    async fn parse_transports(&self, path: String, points_mapping: &HashMap<String, u64>, point_param: &HashMap<String, PointParam>, point_discrete: &HashMap<String, bool>) -> Result<u64, String> {
        // 打开文件
        if let Ok(file) = File::open(path) {
            let reader = BufReader::new(file);
            // 反序列化为对象
            match serde_json::from_reader(reader) {
                Ok(transports) => {
                    log::info!("start do dev_guid mqtt");
                    let devs = query_dev(&transports).await?;
                    let dev_guids = devs.iter().map(|v| {
                        (v.devID.clone(), v.guid.clone().unwrap_or("".to_string()))
                    }).collect::<HashMap<String, String>>();
                    log::info!("end do dev_guid mqtt");
                    let (new_transports, current_id) = transports_to_south(transports, points_mapping, &dev_guids, point_param, point_discrete)?;
                    let _ = update_transports(new_transports).await?;
                    self.delete_all_dev_mapping();
                    self.save_dev_mapping(&devs);
                    Ok(current_id)
                },
                Err(err) => Err(format!("通道JSON反序列化失败：{err}"))
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
            match serde_json::from_reader(reader) {
                Ok(aoes) => {
                    let (new_aoes, aoes_mapping) = aoes_to_south(aoes, &points_mapping, current_id)?;
                    let _ = update_aoes(new_aoes).await?;
                    self.save_aoe_mapping(&aoes_mapping);
                    Ok(())
                },
                Err(err) => Err(format!("策略JSON反序列化失败：{err}"))
            }
        } else {
            Err("策略JSON文件不存在".to_string())
        }
    }

    fn query_point_mapping(&self) -> HashMap<String, u64> {
        let points: Vec<PointMapping> = query_values_cbor_with_tree_name(&self.inner_db, POINT_TREE);
        let mut map = HashMap::with_capacity(points.len());
        for point in points {
            map.insert(point.nid, point.sid);
        }
        map
    }

    fn save_point_mapping(&self, points_mapping: &HashMap<String, u64>) {
        let points = points_mapping.iter().map(|(k, v)|{
            PointMapping {
                sid: *v,
                nid: k.to_string(),
            }
        }).collect::<Vec<PointMapping>>();
        if save_items_cbor_to_db_with_tree_name(&self.inner_db, POINT_TREE, &points, |point| {
            point.nid.as_bytes().to_vec()
        }) {
            info!("insert point_mapping success");
        } else {
            warn!("!!Failed to insert point_mapping");
        }
    }

    fn replace_point_mapping(&self, old_mapping: &HashMap<String, u64>, new_mapping: &HashMap<String, u64>) {
        delete_items_by_keys_with_tree_name(
            &self.inner_db,
            POINT_TREE,
            old_mapping.keys().map(|id| id.to_string().as_bytes().to_vec()).collect(),
        );
        self.save_point_mapping(new_mapping);
    }

    fn query_aoe_mapping(&self) -> HashMap<u64, u64> {
        let aoes: Vec<AoeMapping> = query_values_cbor_with_tree_name(&self.inner_db, AOE_TREE);
        let mut map = HashMap::with_capacity(aoes.len());
        for aoe in aoes {
            map.insert(aoe.sid, aoe.nid);
        }
        map
    }

    fn save_aoe_mapping(&self, aoes_mapping: &HashMap<u64, u64>) {
        let aoes = aoes_mapping.iter().map(|(k, v)|{
            AoeMapping {
                sid: *k,
                nid: *v,
            }
        }).collect::<Vec<AoeMapping>>();
        if save_items_cbor_to_db_with_tree_name(&self.inner_db, AOE_TREE, &aoes, |aoe| {
            aoe.sid.to_string().as_bytes().to_vec()
        }) {
            info!("insert aoe_mapping success");
        } else {
            warn!("!!Failed to insert aoe_mapping");
        }
    }

    fn query_dev_mapping(&self) -> Vec<QueryDevResponseBody> {
        query_values_cbor_with_tree_name(&self.inner_db, DEV_TREE)
    }

    fn save_dev_mapping(&self, devs: &Vec<QueryDevResponseBody>) {
        if save_items_cbor_to_db_with_tree_name(&self.inner_db, DEV_TREE, &devs, |dev| {
            dev.devID.as_bytes().to_vec()
        }) {
            info!("insert dev_mapping success");
        } else {
            warn!("!!Failed to insert dev_mapping");
        }
    }

    fn delete_all_dev_mapping(&self) -> bool {
        let devs = self.query_dev_mapping();
        self.delete_dev_mapping(&devs)
    }

    fn delete_dev_mapping(&self, devs: &Vec<QueryDevResponseBody>) -> bool {
        let keys = devs.iter().map(|v|v.devID.as_bytes().to_vec()).collect::<Vec<Vec<u8>>>();
        delete_items_by_keys_with_tree_name(&self.inner_db, DEV_TREE, keys)
    }

}

pub fn start_parser_service(parser_db_dir: String) -> Sender<ParserOperation> {
    info!("start parser service job...");
    // 启动解析服务
    let (op_sender, op_receiver) = bounded(OPERATION_RECEIVE_BUFF_NUM);
    tokio::spawn(async move {
        if let Some(db) = ParserManager::new(&parser_db_dir) {
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

#[get("/api/v1/parser/aoe_mapping")]
async fn get_aoe_mapping(
    sender: web::Data<Sender<ParserOperation>>,
) -> HttpResponse {
    let (tx, rx) = bounded(1);
    if let Ok(()) = sender.send(ParserOperation::GetAoeMapping(tx)).await {
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
    .service(get_dev_mapping)
    .service(get_aoe_mapping);
}

async fn query_dev(transports: &MyTransports) -> Result<Vec<QueryDevResponseBody>, String> {
    if transports.transports.is_empty() {
        return Err("通道为空".to_string());
    }
    let dev_ids = transports.transports.iter().map(|t|t.dev_id()).collect::<Vec<_>>();
    do_query_dev(dev_ids).await
}
