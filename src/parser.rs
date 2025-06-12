/// 目前主要为key_value键值对的存储服务
use actix_web::{delete, get, HttpResponse, post, put, web};
use async_channel::{bounded, Sender};
use log::{info, warn};
use rocksdb::{DB, Direction, IteratorMode};
use std::fs::File;
use std::io::BufReader;

use crate::db::mydb;
use crate::model::north::{MyAoes, MyPoints};
use crate::model::south::{Measurement};
use crate::model::ParserResult;

const PARSER_TREE: &str = "parser";
pub const OPERATION_RECEIVE_BUFF_NUM: usize = 100;

pub enum ParserOperation {
    UpdateJson(Sender<ParserResult>),
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
        let cfs = [PARSER_TREE];
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
                // let path = "/data/ext.syy.plcc";
                let path = "C:/project/other/test_file";
                let file_name_aoes = format!("{path}/aoes.json");
                let file_name_points = format!("{path}/points.json");
                let file_name_transports = format!("{path}/transports.json");
                let (result, value) = self.update_points(file_name_points);
                // self.update_aoes(file_name_aoes);
                
                if let Err(e) = sender.send(ParserResult {
                    result,
                    value
                }).await {
                    warn!("!!Failed to send update json : {e:?}");
                }
            }
            ParserOperation::Quit => {}
        }
    }

    fn update_points(&self, path: String) -> (bool, String) {
        // 打开文件
        if let Ok(file) = File::open(path) {
            let reader = BufReader::new(file);
            // 反序列化为对象
            let points: MyPoints = serde_json::from_reader(reader)
                .expect("Failed to parse JSON file");
            log::debug!("{:?}", points);
            (true, format!("{:?}", points))
        } else {
            (false, "文件不存在".to_string())
        }
    }

    fn update_aoes(&self, path: String) -> bool {
        // 打开文件
        if let Ok(file) = File::open(path) {
            let reader = BufReader::new(file);
            // 反序列化为对象
            let aoes: MyAoes = serde_json::from_reader(reader)
                .expect("Failed to parse JSON file");
            true
        } else {
            false
        }
    }

    // 后端保存测点
    fn save_points(points: Vec<Measurement>) {
        let url = "http://localhost:8888/api/v1/points/models";
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

pub fn config_parser_web_service(cfg: &mut web::ServiceConfig) {
    // 开放控制接口
    cfg.service(update_json);
}