use std::fs::File;
use std::io::{BufReader, Write};
use std::collections::{HashSet, HashMap};
use chrono::{Local, TimeZone, FixedOffset, Duration as ChronoDuration};
use tokio_cron_scheduler::{Job, JobScheduler};

use crate::utils::memsapi::{do_query_dff_status, do_dff_action};
use crate::utils::meter_data::export_meter_csv;
use crate::utils::mqttclient::{mqtt_acquirer, mqtt_provider};
use crate::{ADAPTER_NAME, AdapterErr, ErrCode, MODEL_FROZEN_METER};
use crate::env::Env;
use crate::model::datacenter::*;
use crate::model::north::{MyDffModels, MyDffResult};
use crate::model::south::FlowOperation;
use crate::utils::localapi::query_dff_mapping;

pub async fn do_mems_event_dff() -> Result<(), AdapterErr> {
    tokio::spawn(async {
        if let Err(e) = mems_event_dff().await {
            log::error!("do mems_event_dff error: {}", e.msg);
        }
    });
    Ok(())
}

pub async fn mems_event_dff() -> Result<(), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let app_name = env.get_app_name();
    mqtt_provider(
        "mems_event".to_string(),
        format!("/ext.syy.phSmc/{app_name}/S-smclink/F-MemsEvent"),
        format!("/{app_name}/ext.syy.phSmc/S-smclink/F-MemsEvent"),
        move |payload| {
            Box::pin(async move {
                if let Ok(msg) = serde_json::from_slice::<MemsEventRequest>(&payload) {
                    match msg.cmd {
                        MemsEventCmd::GetTgDFFConfig => {
                            do_get_dff_config(msg)
                        },
                        MemsEventCmd::TgDFFControl => {
                            do_dff_control(msg).await
                        },
                        MemsEventCmd::GetTgDFFStatus => {
                            do_get_dff_status(msg).await
                        }
                    }
                } else {
                    log::error!("do mems_event 序列化错误: {payload:?}");
                    let time = Local::now().timestamp_millis();
                    let data = get_dff_status_body(None, ErrCode::DataJsonDeserializeErr, "Json格式错误".to_string());
                    MemsEventResponse {
                        token: time.to_string(),
                        request_id: time.to_string(),
                        time: generate_current_time(),
                        msg_info: "".to_string(),
                        data,
                    }
                }
            })
        },
    ).await
}

fn do_get_dff_config(mems_event: MemsEventRequest) -> MemsEventResponse {
    let env = Env::get_env(ADAPTER_NAME);
    let result_dir = env.get_result_dir();
    let dff_dir = env.get_dff_dir();
    let file_name_dffs = format!("{result_dir}/{dff_dir}");
    let mut dffs = None;
    if let Ok(file) = File::open(file_name_dffs) {
        let reader = BufReader::new(file);
        match serde_json::from_reader::<_, MyDffModels>(reader) {
            Ok(my_dffs) => {
                dffs = my_dffs.dffs;
            },
            Err(_) => {}
        }
    }
    MemsEventResponse {
        token: mems_event.token,
        request_id: mems_event.request_id,
        time: generate_current_time(),
        msg_info: "".to_string(),
        data: MemsEventResponseBody {
            dffs,
            dffs_status: None,
            code: ErrCode::Success,
            msg: "".to_string(),
        }
    }
}

async fn do_dff_control(mems_event: MemsEventRequest) -> MemsEventResponse {
    let data = 'result: {
        if let Some(body) = mems_event.body {
            if let Some(dffs_status) = body.dffs_status {
                let mut south_dffs_status = dffs_status.clone();
                match query_dff_mapping().await {
                    Ok(dff_mapping) => {
                        for status in south_dffs_status.iter_mut() {
                            if let Some(south_dff_id) = dff_mapping.iter().find_map(|(k, v)| if *v == status.dff_id { Some(*k) } else { None }) {
                                status.dff_id = south_dff_id;
                            } else {
                                break 'result get_dff_status_body(None, ErrCode::DffIdNotFound, "未找到北向dff_id".to_string());
                            }
                        }
                    },
                    Err(e) => {
                        break 'result get_dff_status_body(None, ErrCode::InternalErr, e.msg);
                    }
                }
                let mut stop_dffs = vec![];
                let mut start_dffs = vec![];
                for status in south_dffs_status {
                    match status.dff_status {
                        0 => {
                            stop_dffs.push(status.dff_id);
                        },
                        _ => {
                            start_dffs.push(status.dff_id);
                        }
                    }
                }
                let result = if !stop_dffs.is_empty() {
                    match do_dff_action(FlowOperation::StopFlows(stop_dffs)).await {
                        Ok(_) => {
                            get_dff_status_body(Some(dffs_status.clone()), ErrCode::Success, "".to_string())
                        }
                        Err(e) => get_dff_status_body(None, ErrCode::DffActionErr, e.msg)
                    }
                } else {
                    get_dff_status_body(Some(dffs_status.clone()), ErrCode::Success, "".to_string())
                };
                if result.code == ErrCode::Success {
                    if !start_dffs.is_empty() {
                        match do_dff_action(FlowOperation::StartFlows(start_dffs)).await {
                            Ok(_) => {
                                get_dff_status_body(Some(dffs_status), ErrCode::Success, "".to_string())
                            }
                            Err(e) => get_dff_status_body(None, ErrCode::DffActionErr, e.msg)
                        }
                    } else {
                        get_dff_status_body(Some(dffs_status), ErrCode::Success, "".to_string())
                    }
                } else {
                    result
                }
            } else {
                get_dff_status_body(None, ErrCode::DataJsonDeserializeErr, "body.dffs_status不能为空".to_string())
            }
        } else {
            get_dff_status_body(None, ErrCode::DataJsonDeserializeErr, "body不能为空".to_string())
        }
    };
    MemsEventResponse {
        token: mems_event.token,
        request_id: mems_event.request_id,
        time: generate_current_time(),
        msg_info: "".to_string(),
        data,
    }
}

async fn do_get_dff_status(mems_event: MemsEventRequest) -> MemsEventResponse {
    let (dffs_status, code, msg) = 'result: {
        match do_query_dff_status().await {
            Ok(mut dffs_status) => {
                match query_dff_mapping().await {
                    Ok(dff_mapping) => {
                        for status in dffs_status.iter_mut() {
                            if let Some(north_dff_id) = dff_mapping.get(&status.dff_id) {
                                status.dff_id = *north_dff_id;
                            } else {
                                break 'result (None, ErrCode::DffIdNotFound, "未找到北向报表id".to_string());
                            }
                        }
                        if let Some(body) = mems_event.body {
                            if let Some(dffs_id) = body.dffs_id {
                                let b_set: HashSet<u64> = dffs_id.into_iter().collect();
                                dffs_status.retain(|x| b_set.contains(&x.dff_id));
                            }
                        };
                        (Some(dffs_status), ErrCode::Success, "".to_string())
                    },
                    Err(e) => {
                        (None, ErrCode::InternalErr, e.msg)
                    }
                }
            },
            Err(e) => {
                (None, ErrCode::PlccActionErr, e.msg)
            }
        }
    };
    MemsEventResponse {
        token: mems_event.token,
        request_id: mems_event.request_id,
        time: generate_current_time(),
        msg_info: "".to_string(),
        data: MemsEventResponseBody {
            dffs: None,
            dffs_status,
            code,
            msg,
        },
    }
}

fn get_dff_status_body(dffs_status: Option<Vec<MemsEventDffStatus>>, code: ErrCode, msg: String) -> MemsEventResponseBody {
    MemsEventResponseBody {
        dffs: None,
        dffs_status,
        code,
        msg,
    }
}

fn generate_current_time() -> String {
    let now = Local::now();
    now.format("%Y-%m-%dT%H:%M:%S%.3f%z").to_string()
}

fn generate_history_data_time() -> (String, String) {
    let tz = FixedOffset::east_opt(8 * 3600).unwrap();
    // 今天 0 点
    let today_0 = Local::now().date_naive().and_hms_milli_opt(0, 0, 0, 0).unwrap();
    // 前一天 0 点
    let yesterday_start = tz.from_local_datetime(&(today_0 - ChronoDuration::days(1))).unwrap();
    let yesterday_end = tz.from_local_datetime(&(today_0 - ChronoDuration::seconds(1))).unwrap();
    let start_time = yesterday_start.format("%Y-%m-%dT%H:%M:%S%.3f%z").to_string();
    let end_time = yesterday_end.format("%Y-%m-%dT%H:%M:%S%.3f%z").to_string();
    (start_time, end_time)
}

fn generate_query_meter_dev() -> QueryRegisterDev {
    let time = Local::now().timestamp_millis();
    QueryRegisterDev {
        token: time.to_string(),
        time: generate_current_time(),
        body: vec![MODEL_FROZEN_METER.to_string()],
    }
}

pub fn generate_dff_update(dff_result: Vec<MyDffResult>, model: String, dev: String, app_name: String) -> DffUpdate {
    let time = Local::now().timestamp_millis();
    let (min_start, max_end) = find_min_start_max_end_dff(&dff_result);
    let body = DffUpdateBody {
        model,
        dev,
        event: "tgDFFResult".to_string(),
        starttime: generate_time(min_start),
        endtime: generate_time(max_end),
        happen_src: app_name,
        is_need_rpt: "Yes".to_string(),
        extdata: dff_result,
    };
    DffUpdate {
        token: time.to_string(),
        time: generate_current_time(),
        body: vec![body],
    }
}

pub fn generate_dff_set(dff_result: Vec<MyDffResult>, model: String, dev: String, app_name: String) -> DffSet {
    let time = Local::now().timestamp_millis();
    let timestamp = generate_current_time();
    let (min_start, max_end) = find_min_start_max_end_dff(&dff_result);
    let start_time = generate_time(min_start);
    let end_time = generate_time(max_end);
    let body = DffSetBody {
        model,
        dev,
        event: "tgDFFResult".to_string(),
        timestamp: timestamp.clone(),
        timestartgather: start_time.clone(),
        timeendgather: end_time.clone(),
        starttimestamp: start_time.clone(),
        endtimestamp: end_time.clone(),
        happen_src: app_name,
        is_need_rpt: "Yes".to_string(),
        occurnum: "1".to_string(),
        event_level: "common".to_string(),
        rpt_status: vec![RptStatusItem { net_1: "00".to_string() }],
        data: "".to_string(),
        extdata: dff_result,
    };
    DffSet {
        token: time.to_string(),
        time: timestamp,
        body: vec![body],
        sour_type: "104".to_string(),
    }
}

fn generate_query_meter_history(devs: Vec<String>) -> RequestHistory {
    let time = Local::now().timestamp_millis();
    let timestamp = generate_current_time();
    let (start_time, end_time) = generate_history_data_time();
    let body = devs.iter().map(|dev| RequestHistoryBody {
        dev: dev.clone(),
        body: vec!["tgSupWh".to_string()],
    }).collect();
    RequestHistory {
        token: time.to_string(),
        time: timestamp,
        choice: "1".to_string(),
        time_type: "timestartgather".to_string(),
        start_time,
        end_time,
        time_span: "5".to_string(),
        frozentype: "min".to_string(),
        body,
    }
}

// fn generate_query_meter_guid(addrs: Vec<String>) -> RequestGuid {
//     let time = Local::now().timestamp_millis();
//     let timestamp = generate_current_time();
//     let body = addrs.iter().map(|addr| RequestGuidBody {
//         model: MODEL_FROZEN_METER.to_string(),
//         port: MODEL_PORT_METER.to_string(),
//         addr: addr.clone(),
//         desc: MODEL_DESC_METER.to_string(),
//     }).collect();
//     RequestGuid {
//         token: time.to_string(),
//         time: timestamp,
//         body,
//     }
// }

fn find_min_start_max_end_dff(dff_result: &Vec<MyDffResult>) -> (Option<u64>, Option<u64>) {
    let min_start = dff_result
        .iter()
        .filter_map(|r| r.start_time)
        .min();
    let max_end = dff_result
        .iter()
        .filter_map(|r| r.end_time)
        .max();
    (min_start, max_end)
}

fn generate_time(ts_millis: Option<u64>) -> String {
    let time = if let Some(time) = ts_millis {
        time as i64
    } else {
        Local::now().timestamp_millis()
    };
    if let chrono::LocalResult::Single(dt) = Local.timestamp_millis_opt(time) {
        dt.format("%Y-%m-%dT%H:%M:%S%.3f%z").to_string()
    } else {
        generate_current_time()
    }
}

pub async fn do_meter_data_query() -> Result<(), AdapterErr> {
    let sched = JobScheduler::new().await.unwrap();
    let job = Job::new_async("0 0 1 * * *", |_uuid, _l| {
        Box::pin(async move {
            let env = Env::get_env(ADAPTER_NAME);
            let app_name = env.get_app_name();
            let meter_sum_no = env.get_meter_sum_no();
            let meter_dir = env.get_meter_dir();
            if let Ok(meter_dev_addr) = query_meter_dev().await {
                let dev_guids = meter_dev_addr.keys().cloned().collect::<Vec<_>>();
                let body = generate_query_meter_history(dev_guids);
                match mqtt_acquirer::<_, ResponseHistory>(
                    "mems_query_history_data".to_string(),
                    format!("/sys.dbc/{app_name}/S-dataservice/F-GetFrozenData"),
                    format!("/{app_name}/sys.dbc/S-dataservice/F-GetFrozenData"),
                    body,
                ).await {
                    Ok(msg) => {
                        // let mut meter_nos = vec![];
                        let mut timestamps = vec![];
                        let mut meter_data = HashMap::new();
                        for data_body in msg.body {
                            if let Some(addr) = meter_dev_addr.get(&data_body.dev) {
                                for measures in data_body.body {
                                    // meter_nos.push(addr.clone());
                                    if !timestamps.contains(&measures.timestamp) {
                                        timestamps.push(measures.timestamp.clone());
                                    }
                                    if let Some(measure) = measures.body.first() {
                                        meter_data.insert((addr.clone(), measures.timestamp), measure.val.clone());
                                    }
                                }
                            }
                        }
                        let csv = export_meter_csv(
                            &meter_sum_no,
                            timestamps,
                            meter_data,
                        );
                        let mut meter_data_file = File::create(&format!("{meter_dir}/meter_data.csv")).unwrap();
                        meter_data_file.write_all(csv.as_bytes()).unwrap();
                    }
                    Err(e) => {
                        log::error!("do mems_query_history_data error: {}", e.msg);
                    }
                }
            }
        })
    }).unwrap();
    sched.add(job).await.unwrap();
    sched.start().await.unwrap();
    Ok(())

    // let id = job.guid();
    // sched.remove(&id).await.unwrap();
}

pub async fn query_meter_dev() -> Result<HashMap<String, String>, AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let app_name = env.get_app_name();
    let body = generate_query_meter_dev();
    match mqtt_acquirer::<_, RegisterDevResult>(
        "mems_meter_dev".to_string(),
        format!("/sys.dbc/{app_name}/S-dataservice/F-GetRegister"),
        format!("/{app_name}/sys.dbc/S-dataservice/F-GetRegister"),
        body,
    ).await {
        Ok(msg) => {
            let mut addr_guid_map = HashMap::new();
            for dev_body in msg.body {
                if dev_body.model == MODEL_FROZEN_METER {
                    for dev in dev_body.body {
                        addr_guid_map.insert(dev.dev, dev.addr);
                    }
                }
            }
            Ok(addr_guid_map)
        }
        Err(e) => Err(AdapterErr {
            code: e.code,
            msg: format!("查询表计dev失败，{}", e.msg),
        }),
    }
}
