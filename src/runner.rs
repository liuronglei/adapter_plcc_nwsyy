use log::info;

use actix_cors::Cors;
use actix_web::{App, HttpServer, web};
use actix_web::middleware::Compress;
use actix_web::web::Data;
use crate::ADAPTER_NAME;
use crate::parser::{start_parser_service, config_parser_web_service};
use crate::utils::mqttclient::{do_register, do_data_query, do_keep_alive};
use crate::utils::plccapi::aoe_result_upload;
use crate::env::Env;

pub async fn run_adapter() -> std::io::Result<()> {
    Env::init(ADAPTER_NAME);
    let env = Env::get_env(ADAPTER_NAME);
    let http_server_port = env.get_http_server_port();
    let mqtt_server = env.get_mqtt_server();
    let mqtt_server_port = env.get_mqtt_server_port();
    let data_path = env.get_db_dir();
    // APP注册和数据查询
    println!("进入注册流程");
    match do_register("plcc_register", &mqtt_server, mqtt_server_port).await {
        Ok(_) => {},
        Err(err) => {
            log::error!("{err}");
            println!("{err}");
        },
    }
    log::info!("注册流程结束");
    println!("进入数据查询流程");
    match do_data_query("plcc_data_query", &mqtt_server, mqtt_server_port).await {
        Ok(_) => {},
        Err(err) => {
            log::error!("{err}");
            println!("{err}");
        },
    }
    println!("数据查询流程结束");
    println!("进入AOE结果监听流程");
    match aoe_result_upload().await {
        Ok(_) => {},
        Err(err) => {
            log::error!("{err}");
            println!("{err}");
        },
    }
    println!("AOE结果监听流程结束");
    println!("进入保活监听流程");
    match do_keep_alive().await {
        Ok(_) => {},
        Err(err) => {
            log::error!("{err}");
            println!("{err}");
        },
    }
    println!("保活监听流程结束");
    println!("开始启动API服务");
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