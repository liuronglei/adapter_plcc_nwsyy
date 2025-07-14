use actix_web::{get, HttpResponse, web};
use async_channel::{bounded, Sender};
use log::{info, warn};
use rocksdb::DB;
use std::fs::File;
use std::io::{BufReader, Write};
use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};

use crate::db::mydb;
use crate::model::datacenter::QueryDevResponseBody;
use crate::{AdapterErr, ErrCode, ADAPTER_NAME};
use crate::model::north::{MyAoe, MyAoes, MyMeasurement, MyPoints, MyTransport, MyTransports, PointParam};
use crate::model::{points_to_south, transports_to_south, aoes_to_south,};
use crate::utils::plccapi::{update_points, update_transports, update_aoes, do_reset};
use crate::utils::mqttclient::{do_query_dev, do_data_query, do_register_sync, build_dev_mapping};
use crate::db::dbutils::*;
use crate::utils::{param_point_map, point_param_map, register_result};
use crate::env::Env;

const POINT_TREE: &str = "point";
const AOE_TREE: &str = "aoe";
const DEV_TREE: &str = "dev";

pub const OPERATION_RECEIVE_BUFF_NUM: usize = 100;

pub enum ParserOperation {
    UpdateJson(Sender<u16>),
    RecoverJson(Sender<u16>),
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
        let result_dir = env.get_result_dir();
        let point_dir = env.get_point_dir();
        let transport_dir = env.get_transport_dir();
        let aoe_dir = env.get_aoe_dir();
        match op {
            ParserOperation::UpdateJson(sender) => {
                let (mut result_code, mut has_err) = (ErrCode::Success, false);
                match self.join_points_json(&json_dir, &result_dir, &point_dir).await {
                    Ok(_) => {}
                    Err(e) => {
                        result_code = e.code;
                        has_err = true;
                    }
                }
                if !has_err {
                    match self.join_transports_json(&json_dir, &result_dir, &transport_dir).await {
                        Ok(_) => {}
                        Err(e) => {
                            result_code = e.code;
                            has_err = true;
                        }
                    }
                }
                if !has_err {
                    match self.join_aoes_json(&json_dir, &result_dir, &aoe_dir).await {
                        Ok(_) => {}
                        Err(e) => {
                            result_code = e.code;
                            has_err = true;
                        }
                    }
                }
                if !has_err {
                    result_code = self.start_parser(result_dir, point_dir, transport_dir, aoe_dir).await;
                }
                if let Err(e) = sender.send(result_code as u16).await {
                    warn!("!!Failed to send update json : {e:?}");
                }
            }
            ParserOperation::RecoverJson(sender) => {
                let result_code = self.start_parser(result_dir, point_dir, transport_dir, aoe_dir).await;
                if let Err(e) = sender.send(result_code as u16).await {
                    warn!("!!Failed to send recover json : {e:?}");
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

    async fn join_points_json(&self, parser_path: &str, result_path: &str, point_dir: &str) -> Result<(), AdapterErr> {
        let file_name_points = format!("{parser_path}/{point_dir}");
        let result_name_points = format!("{result_path}/{point_dir}");
        if let Ok(file) = File::open(file_name_points) {
            let reader = BufReader::new(file);
            match serde_json::from_reader::<_, MyPoints>(reader) {
                Ok(points) => {
                    let mut old_points = if let Ok(file) = File::open(&result_name_points) {
                        let reader = BufReader::new(file);
                        match serde_json::from_reader::<_, MyPoints>(reader) {
                            Ok(points) => {
                                points
                            },
                            Err(err) => {
                                return Err(AdapterErr {
                                    code: ErrCode::PointJsonDeserializeErr,
                                    msg: format!("测点JSON反序列化失败：{err}"),
                                });
                            }
                        }
                    } else {
                        MyPoints {
                            points: None,
                            add: None,
                            edit: None,
                            delete: None,
                        }
                    };
                    if points.points.is_some() {
                        old_points.points = points.points;
                    } else {
                        if let Some(add) = points.add {
                            old_points.points = if let Some(old_add) = old_points.points {
                                Some(old_add.iter().chain(add.iter()).cloned().collect::<Vec<MyMeasurement>>())
                            } else {
                                Some(add)
                            };
                        }
                        if let Some(edit) = points.edit {
                            if let Some(ref mut points) = old_points.points {
                                let edit_map = edit.into_iter()
                                    .map(|m| (m.point_id.clone(), m))
                                    .collect::<HashMap<String, MyMeasurement>>();
                                for point in points.iter_mut() {
                                    if let Some(updated) = edit_map.get(&point.point_id) {
                                        *point = updated.clone();
                                    }
                                }
                            } else {
                                old_points.points = Some(edit);
                            }
                        }
                        if let Some(delete) = points.delete {
                            let remove_set = delete.into_iter().collect::<HashSet<String>>();
                            if let Some(ref mut points) = old_points.points {
                                points.retain(|m| !remove_set.contains(&m.point_id));
                            }
                        }
                    }
                    let mut points_file = File::create(&result_name_points).unwrap();
                    points_file.write_all(serde_json::to_string(&old_points).unwrap().as_bytes()).unwrap();
                    Ok(())
                },
                Err(err) => Err(AdapterErr {
                    code: ErrCode::PointJsonDeserializeErr,
                    msg: format!("测点JSON反序列化失败：{err}"),
                })
            }
        } else {
            Err(AdapterErr {
                code: ErrCode::PointJsonNotFound,
                msg: "测点JSON文件不存在".to_string(),
            })
        }
    }

    async fn join_transports_json(&self, parser_path: &str, result_path: &str, transport_dir: &str) -> Result<(), AdapterErr> {
        let file_name_transports = format!("{parser_path}/{transport_dir}");
        let result_name_transports = format!("{result_path}/{transport_dir}");
        if let Ok(file) = File::open(file_name_transports) {
            let reader = BufReader::new(file);
            match serde_json::from_reader::<_, MyTransports>(reader) {
                Ok(transports) => {
                    let mut old_transports = if let Ok(file) = File::open(&result_name_transports) {
                        let reader = BufReader::new(file);
                        match serde_json::from_reader::<_, MyTransports>(reader) {
                            Ok(transports) => {
                                transports
                            },
                            Err(err) => {
                                return Err(AdapterErr {
                                    code: ErrCode::TransportJsonDeserializeErr,
                                    msg: format!("通道JSON反序列化失败：{err}"),
                                });
                            }
                        }
                    } else {
                        MyTransports {
                            transports: None,
                            add: None,
                            edit: None,
                            delete: None,
                        }
                    };
                    if transports.transports.is_some() {
                        old_transports.transports = transports.transports;
                    } else {
                        if let Some(add) = transports.add {
                            old_transports.transports = if let Some(old_add) = old_transports.transports {
                                Some(old_add.iter().chain(add.iter()).cloned().collect::<Vec<MyTransport>>())
                            } else {
                                Some(add)
                            };
                        }
                        if let Some(edit) = transports.edit {
                            if let Some(ref mut transports) = old_transports.transports {
                                let edit_map = edit.into_iter()
                                    .map(|m| (m.dev_id().clone(), m))
                                    .collect::<HashMap<String, MyTransport>>();
                                for transport in transports.iter_mut() {
                                    if let Some(updated) = edit_map.get(&transport.dev_id()) {
                                        *transport = updated.clone();
                                    }
                                }
                            } else {
                                old_transports.transports = Some(edit);
                            }
                        }
                        if let Some(delete) = transports.delete {
                            let remove_set = delete.into_iter().collect::<HashSet<String>>();
                            if let Some(ref mut transports) = old_transports.transports {
                                transports.retain(|m| !remove_set.contains(&m.dev_id()));
                            }
                        }
                    }
                    let mut transports_file = File::create(&result_name_transports).unwrap();
                    transports_file.write_all(serde_json::to_string(&old_transports).unwrap().as_bytes()).unwrap();
                    Ok(())
                },
                Err(err) => Err(AdapterErr {
                    code: ErrCode::TransportJsonDeserializeErr,
                    msg: format!("通道JSON反序列化失败：{err}"),
                })
            }
        } else {
            Err(AdapterErr {
                code: ErrCode::TransportJsonNotFound,
                msg: "通道JSON文件不存在".to_string(),
            })
        }
    }

    async fn join_aoes_json(&self, parser_path: &str, result_path: &str, aoe_dir: &str) -> Result<(), AdapterErr> {
        let file_name_aoes = format!("{parser_path}/{aoe_dir}");
        let result_name_aoes = format!("{result_path}/{aoe_dir}");
        if let Ok(file) = File::open(file_name_aoes) {
            let reader = BufReader::new(file);
            match serde_json::from_reader::<_, MyAoes>(reader) {
                Ok(aoes) => {
                    let mut old_aoes = if let Ok(file) = File::open(&result_name_aoes) {
                        let reader = BufReader::new(file);
                        match serde_json::from_reader::<_, MyAoes>(reader) {
                            Ok(aoes) => {
                                aoes
                            },
                            Err(err) => {
                                return Err(AdapterErr {
                                    code: ErrCode::AoeJsonDeserializeErr,
                                    msg: format!("策略JSON反序列化失败：{err}"),
                                });
                            }
                        }
                    } else {
                        MyAoes {
                            aoes: None,
                            add: None,
                            edit: None,
                            delete: None,
                        }
                    };
                    if aoes.aoes.is_some() {
                        old_aoes.aoes = aoes.aoes;
                    } else {
                        if let Some(add) = aoes.add {
                            old_aoes.aoes = if let Some(old_add) = old_aoes.aoes {
                                Some(old_add.iter().chain(add.iter()).cloned().collect::<Vec<MyAoe>>())
                            } else {
                                Some(add)
                            };
                        }
                        if let Some(edit) = aoes.edit {
                            if let Some(ref mut aoes) = old_aoes.aoes {
                                let edit_map = edit.into_iter()
                                    .map(|m| (m.id, m))
                                    .collect::<HashMap<u64, MyAoe>>();
                                for aoe in aoes.iter_mut() {
                                    if let Some(updated) = edit_map.get(&aoe.id) {
                                        *aoe = updated.clone();
                                    }
                                }
                            } else {
                                old_aoes.aoes = Some(edit);
                            }
                        }
                        if let Some(delete) = aoes.delete {
                            let remove_set = delete.into_iter().collect::<HashSet<u64>>();
                            if let Some(ref mut aoes) = old_aoes.aoes {
                                aoes.retain(|m| !remove_set.contains(&m.id));
                            }
                        }
                    }
                    let mut aoes_file = File::create(&result_name_aoes).unwrap();
                    aoes_file.write_all(serde_json::to_string(&old_aoes).unwrap().as_bytes()).unwrap();
                    Ok(())
                },
                Err(err) => Err(AdapterErr {
                    code: ErrCode::AoeJsonDeserializeErr,
                    msg: format!("策略JSON反序列化失败：{err}"),
                })
            }
        } else {
            Err(AdapterErr {
                code: ErrCode::AoeJsonNotFound,
                msg: "策略JSON文件不存在".to_string(),
            })
        }
    }

    async fn start_parser(&self, path: String, point_dir: String, transport_dir: String, aoe_dir: String) -> ErrCode {
        let file_name_points = format!("{path}/{point_dir}");
        let file_name_transports = format!("{path}/{transport_dir}");
        let file_name_aoes = format!("{path}/{aoe_dir}");
        let old_point_mapping = self.query_point_mapping();
        let mut points_mapping: HashMap<String, u64> = HashMap::default();
        let mut point_param: HashMap<String, PointParam> = HashMap::default();
        let mut point_discrete: HashMap<String, bool> = HashMap::default();
        let mut current_id = 65535_u64;
        let mut result_code = ErrCode::Success;
        let mut has_err = false;
        if !register_result::get_result() {
            log::info!("start do register");
            match do_register_sync().await {
                Ok(_) => {},
                Err(err) => {
                    log::error!("{}", err.msg);
                    has_err = true;
                    result_code = err.code;
                }
            }
            log::info!("end do register");
        }
        if !has_err {
            log::info!("start parse point.json");
            match self.parse_points(file_name_points, &old_point_mapping).await {
                Ok((mapping, param, discrete)) => {
                    points_mapping = mapping;
                    point_param = param;
                    point_discrete = discrete;
                    // 保存到全局变量中
                    param_point_map::save_all(points_mapping.clone());
                    point_param_map::save_reversal(points_mapping.clone());
                },
                Err(err) => {
                    log::error!("{}", err.msg);
                    has_err = true;
                    result_code = err.code;
                }
            }
            log::info!("end parse point.json");
        }
        if !has_err {
            log::info!("start parse transports.json");
            match self.parse_transports(file_name_transports, &points_mapping, &point_param, &point_discrete).await {
                Ok(id) => current_id = id,
                Err(err) => {
                    log::error!("{}", err.msg);
                    has_err = true;
                    result_code = err.code;
                }
            }
            log::info!("end parse transports.json");
        }
        if !has_err {
            log::info!("start parse aoes.json");
            match self.parse_aoes(file_name_aoes, &points_mapping, current_id).await {
                Ok(_) => {},
                Err(err) => {
                    log::error!("{}", err.msg);
                    has_err = true;
                    result_code = err.code;
                }
            }
            log::info!("end parse aoes.json");
        }
        if !has_err {
            log::info!("start do reset");
            match do_reset().await {
                Ok(()) => {},
                Err(err) => {
                    log::error!("{}", err.msg);
                    has_err = true;
                    result_code = err.code;
                }
            }
            log::info!("end do reset");
        }
        if !has_err {
            log::info!("start do query_data mqtt");
            match do_data_query().await {
                Ok(()) => {},
                Err(err) => {
                    log::error!("{}", err.msg);
                    // has_err = true;
                    result_code = err.code;
                },
            }
            log::info!("end do query_data mqtt");
        }
        result_code
    }

    async fn parse_points(&self, path: String, old_point_mapping: &HashMap<String, u64>) -> Result<(HashMap<String, u64>, HashMap<String, PointParam>, HashMap<String, bool>), AdapterErr> {
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
                Err(err) => Err(AdapterErr {
                    code: ErrCode::PointJsonDeserializeErr,
                    msg: format!("测点JSON反序列化失败：{err}"),
                })
            }
        } else {
            Err(AdapterErr {
                code: ErrCode::PointJsonNotFound,
                msg: "测点JSON文件不存在".to_string(),
            })
        }
    }

    async fn parse_transports(&self, path: String, points_mapping: &HashMap<String, u64>, point_param: &HashMap<String, PointParam>, point_discrete: &HashMap<String, bool>) -> Result<u64, AdapterErr> {
        // 打开文件
        if let Ok(file) = File::open(path) {
            let reader = BufReader::new(file);
            // 反序列化为对象
            match serde_json::from_reader(reader) {
                Ok(transports) => {
                    log::info!("start do dev_guid mqtt");
                    let devs = query_dev(&transports).await?;
                    log::info!("end do dev_guid mqtt");
                    let dev_mapping = build_dev_mapping(&devs);
                    let (new_transports, current_id) = transports_to_south(transports, points_mapping, &dev_mapping, point_param, point_discrete)?;
                    let _ = update_transports(new_transports).await?;
                    self.delete_all_dev_mapping();
                    self.save_dev_mapping(&devs);
                    Ok(current_id)
                },
                Err(err) => Err(AdapterErr {
                    code: ErrCode::TransportJsonDeserializeErr,
                    msg: format!("通道JSON反序列化失败：{err}"),
                })
            }
        } else {
            Err(AdapterErr {
                code: ErrCode::TransportJsonNotFound,
                msg: "通道JSON文件不存在".to_string(),
            })
        }
    }

    async fn parse_aoes(&self, path: String, points_mapping: &HashMap<String, u64>, current_id: u64) -> Result<(), AdapterErr> {
        // 打开文件
        if let Ok(file) = File::open(path) {
            let reader = BufReader::new(file);
            // 反序列化为对象
            match serde_json::from_reader(reader) {
                Ok(aoes) => {
                    let (new_aoes, aoes_mapping) = aoes_to_south(aoes, &points_mapping, current_id)?;
                    let _ = update_aoes(new_aoes).await?;
                    let _ = self.delete_all_aoe_mapping();
                    self.save_aoe_mapping(&aoes_mapping);
                    Ok(())
                },
                Err(err) => Err(AdapterErr {
                    code: ErrCode::AoeJsonDeserializeErr,
                    msg: format!("策略JSON反序列化失败：{err}"),
                })
            }
        } else {
            Err(AdapterErr {
                code: ErrCode::AoeJsonNotFound,
                msg: "策略JSON文件不存在".to_string(),
            })
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

    fn delete_all_aoe_mapping(&self) -> bool {
        let aoes = self.query_aoe_mapping();
        self.delete_aoe_mapping(&aoes)
    }

    fn delete_aoe_mapping(&self, aoes: &HashMap<u64, u64>) -> bool {
        let keys = aoes.keys().map(|k| {
            k.to_string().as_bytes().to_vec()
        }).collect::<Vec<Vec<u8>>>();
        delete_items_by_keys_with_tree_name(&self.inner_db, AOE_TREE, keys)
    }

    fn query_dev_mapping(&self) -> Vec<QueryDevResponseBody> {
        query_values_cbor_with_tree_name(&self.inner_db, DEV_TREE)
    }

    fn save_dev_mapping(&self, devs: &Vec<QueryDevResponseBody>) {
        if save_items_cbor_to_db_with_tree_name(&self.inner_db, DEV_TREE, &devs, |dev| {
            let my_id = format!("{}.{}", dev.dev_id, dev.service_id);
            my_id.as_bytes().to_vec()
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
        let keys = devs.iter().map(|dev| {
            let my_id = format!("{}.{}", dev.dev_id, dev.service_id);
            my_id.as_bytes().to_vec()
        }).collect::<Vec<Vec<u8>>>();
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

#[get("/api/v1/parser/recover_json")]
async fn recover_json(
    sender: web::Data<Sender<ParserOperation>>,
) -> HttpResponse {
    let (tx, rx) = bounded(1);
    if let Ok(()) = sender.send(ParserOperation::RecoverJson(tx)).await {
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
    .service(recover_json)
    .service(get_point_mapping)
    .service(get_dev_mapping)
    .service(get_aoe_mapping);
}

async fn query_dev(transports: &MyTransports) -> Result<Vec<QueryDevResponseBody>, AdapterErr> {
    if let Some(transports) = &transports.transports {
        if transports.is_empty() {
            return Err(AdapterErr {
                code: ErrCode::TransportIsEmpty,
                msg: "通道列表不能为空".to_string(),
            });
        }
        // let dev_ids = transports.iter().map(|t|t.dev_id()).collect::<Vec<_>>();
        do_query_dev(transports).await
    } else {
        return Err(AdapterErr {
            code: ErrCode::TransportIsEmpty,
            msg: "通道列表不能为空".to_string(),
        });
    }
}
