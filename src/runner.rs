use log::info;

use actix_cors::Cors;
use actix_web::{App, HttpServer, web};
use actix_web::middleware::Compress;
use actix_web::web::Data;
use crate::{DB_PATH_PARSER, MQTT_HOST, MQTT_PORT, HTTP_PORT};
use crate::parser::{start_parser_service, config_parser_web_service};
use crate::utils::mqttclient::do_register_and_query;
use crate::utils::plccapi::aoe_result_upload;

pub async fn run_adapter() -> std::io::Result<()> {
    // APP注册和数据查询
    let _ = do_register_and_query("plcc_register", MQTT_HOST, MQTT_PORT).await;
    let _ = aoe_result_upload().await;
    let parser_sender = start_parser_service(DB_PATH_PARSER.to_string());
    let cloned_parser_sender = Data::new(parser_sender.clone());
    let actix_web_job = std::thread::spawn(move || {
        // 启动web服务，提供resutful服务
        let actix_rt = actix_rt::Runtime::new().expect("!!Failed to build actix web runtime.");
        actix_rt.block_on(async move {
            // 启动web服务，提供resutful服务
            let addr = format!("0.0.0.0:{HTTP_PORT}");
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