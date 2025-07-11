use log::info;

use actix_cors::Cors;
use actix_web::{App, HttpServer, web};
use actix_web::middleware::Compress;
use actix_web::web::Data;
use crate::ADAPTER_NAME;
use crate::parser::{start_parser_service, config_parser_web_service};
use crate::utils::mqttclient::{do_register, do_data_query, do_keep_alive, do_cloud_event};
use crate::utils::plccapi::aoe_result_upload;
use crate::utils::log_init::write_log_config;
use crate::env::Env;

pub async fn run_adapter() -> std::io::Result<()> {
    Env::init(ADAPTER_NAME);
    let env = Env::get_env(ADAPTER_NAME);
    // 初始化日志
    let log_config = env.get_log_config();
    let (_, log_config_file) = write_log_config(ADAPTER_NAME, &log_config);
    match log4rs::init_file(log_config_file.as_str(), Default::default()) {
        Ok(_) => {
            log::info!("Log4rs initialized");
            log::trace!("TRACE LOG IS ON");
            log::debug!("DEBUG LOG IS ON");
            log::info!("INFO LOG IS ON");
            log::warn!("WARN LOG IS ON");
            log::error!("ERROR LOG IS ON");
        }
        Err(e) => {
            log::error!("Failed to initialize log4rs, err: {e}");
        }
    }
    let http_server_port = env.get_http_server_port();
    let data_path = env.get_db_dir();
    // APP注册和数据查询
    log::info!("start do register mqtt");
    match do_register().await {
        Ok(_) => {},
        Err(err) => {
            log::error!("do register error: {}", err.msg);
        },
    }
    log::info!("end do register mqtt");
    log::info!("start do data_query mqtt");
    match do_data_query().await {
        Ok(_) => {},
        Err(err) => {
            log::error!("do data_query error: {}", err.msg);
        },
    }
    log::info!("end do data_query mqtt");
    log::info!("start do aoe_result_upload mqtt");
    match aoe_result_upload().await {
        Ok(_) => {},
        Err(err) => {
            log::error!("do aoe_result_upload error: {}", err.msg);
        },
    }
    log::info!("end do aoe_result_upload mqtt");
    log::info!("start do keep_alive mqtt");
    match do_keep_alive().await {
        Ok(_) => {},
        Err(err) => {
            log::error!("do keep_alive error: {}", err.msg);
        },
    }
    log::info!("end do keep_alive mqtt");
    log::info!("start do cloud_event mqtt");
    match do_cloud_event().await {
        Ok(_) => {},
        Err(err) => {
            log::error!("do cloud_event error: {}", err.msg);
        },
    }
    log::info!("end do cloud_event mqtt");
    let parser_sender = start_parser_service(data_path.to_string());
    let cloned_parser_sender = Data::new(parser_sender.clone());
    let actix_web_job = std::thread::spawn(move || {
        // 启动web服务，提供resutful服务
        let actix_rt = actix_rt::Runtime::new().expect("!!Failed to build actix web runtime.");
        actix_rt.block_on(async move {
            // 启动web服务，提供resutful服务
            let addr = format!("0.0.0.0:{http_server_port}");
            info!("Http server addr: {}", addr);
            let app = HttpServer::new(move || {
                let cors = Cors::default()
                    .allow_any_origin()
                    .allow_any_method()
                    .allow_any_header();
                let app = App::new()
                    .wrap(cors)
                    .wrap(Compress::default())
                    .app_data(cloned_parser_sender.clone())
                    // sets payload size limit to 2147Mb
                    .app_data(web::PayloadConfig::new(1usize << 31))
                    .app_data(web::JsonConfig::default().limit(1usize << 31))
                    .configure(config_parser_web_service);
                app
            });
            app.bind(&addr).unwrap_or_else(|_| panic!("Failed to bind {addr}"))
                .run()
                .await
                .unwrap_or_else(|_| panic!("Failed to run web server at {addr}"));
        });
    });
    // waiting web service to quit
    actix_web_job.join().expect("actix web job is down.");
    Ok(())
}