use std::collections::HashMap;
use std::str::FromStr;
use serde::{Deserialize, Serialize};

use north::MyPoints;
use south::Measurement;
use south::{DataUnit, Expr};

use crate::model::north::MyTransports;
use crate::model::south::{MqttTransport, Transport};
use crate::utils::get_north_tag;
use crate::APP_NAME;

pub mod north;
pub mod south;
pub mod datacenter;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ParserResult {
    pub result: bool,
    pub err: String,
}

pub fn points_to_south(points: MyPoints) -> (Vec<Measurement>, Vec<(String, u64)>) {
    let mut points_result = vec![];
    let mut mapping_result = vec![];
    let mut current_pid = 100000_u64;
    for p in points.points {
        let unit = DataUnit::from_str(p.data_unit.as_str()).unwrap_or(DataUnit::Unknown);
        let alarm_level1 = Expr::from_str(p.alarm_level1_expr.as_str()).ok();
        let alarm_level2 = Expr::from_str(p.alarm_level2_expr.as_str()).ok();
        current_pid = current_pid + 1;
        points_result.push(Measurement {
            point_id: current_pid,
            point_name: p.point_name,
            alias_id: p.alias_id,
            is_discrete: p.is_discrete,
            is_computing_point: p.is_computing_point,
            expression: p.expression,
            trans_expr: p.trans_expr,
            inv_trans_expr: p.inv_trans_expr,
            change_expr: p.change_expr,
            zero_expr: p.zero_expr,
            data_unit: p.data_unit,
            unit,
            upper_limit: p.upper_limit,
            lower_limit: p.lower_limit,
            alarm_level1_expr: p.alarm_level1_expr,
            alarm_level1,
            alarm_level2_expr: p.alarm_level2_expr,
            alarm_level2,
            is_realtime: p.is_realtime,
            is_soe: p.is_soe,
            init_value: p.init_value,
            desc: "".to_string(),
            is_remote: false,
        });
        mapping_result.push((p.point_id, current_pid));
    }
    (points_result, mapping_result)
}

pub fn transports_to_south(transports: MyTransports, points_mapping: HashMap<String, u64>) -> Vec<Transport> {
    let mut transports_result = vec![];
    let mut current_tid = 65536_u64;
    for t in transports.transports {
        let dev_id = t.dev_id();
        let dev_guid = dev_id.clone();
        let point_ycyx = t.point_ycyx_ids();
        let point_ycyx_ids = point_ycyx.iter()
            .filter(|v|points_mapping.contains_key(*v))
            .map(|v| (*points_mapping.get(v).unwrap(), false)).collect::<Vec<(u64, bool)>>();
        let mut json_tags = HashMap::new();
        for (i, v) in point_ycyx.iter().enumerate() {
            if points_mapping.contains_key(v) {
                if let Some(tag) = get_north_tag(v) {
                    let mut value_map = HashMap::with_capacity(1);
                    value_map.insert("val".to_string(), i);
                    json_tags.insert(format!("[\"{tag}\",0]"), value_map);
                }
            }
        }
        // 遥测遥信通道
        current_tid = current_tid + 1;
        let ycyx_mt1 = MqttTransport {
            id: current_tid,
            name: format!("{}_实时数据写", t.name()),
            mqtt_broker: t.mqtt_broker(),
            point_id: 0_u64,
            point_ids: point_ycyx_ids.clone(),
            read_topic: "/svc.dbc/+/S-dataservice/F-SetRealData".to_string(),
            write_topic: "".to_string(),
            is_json: true,
            is_transfer: false,
            keep_alive: None,
            user_name: None,
            user_password: None,
            array_filter: Some("body".to_string()),
            json_filters: Some(vec![vec!["name".to_string()]]),
            json_tags: Some(json_tags.clone()),
            json_write_template: None,
            json_write_tag: None,
        };
        current_tid = current_tid + 1;
        let ycyx_mt2 = MqttTransport {
            id: current_tid,
            name: format!("{}_实时数据更新通知", t.name()),
            mqtt_broker: t.mqtt_broker(),
            point_id: 0_u64,
            point_ids: point_ycyx_ids,
            read_topic: "/svc.brd/+/S-dataservice/F-UpdateRealData".to_string(),
            write_topic: "".to_string(),
            is_json: true,
            is_transfer: false,
            keep_alive: None,
            user_name: None,
            user_password: None,
            array_filter: Some("body".to_string()),
            json_filters: Some(vec![vec!["name".to_string()]]),
            json_tags: Some(json_tags),
            json_write_template: None,
            json_write_tag: None,
        };
        let point_yt = t.point_yt_ids();
        let point_yt_ids = point_yt.iter()
            .filter(|v|points_mapping.contains_key(*v))
            .map(|v| (*points_mapping.get(v).unwrap(), true)).collect::<Vec<(u64, bool)>>();
        let mut json_write_template = HashMap::new();
        let mut json_write_tag = HashMap::new();
        for v in point_yt.iter() {
            if points_mapping.contains_key(v) {
                if let Some(tag) = get_north_tag(v) {
                    let pid = *points_mapping.get(v).unwrap();
                    json_write_template.insert(
                        pid,
                        format!("{{\"token\": \"234\",\"timestamp\": \"%Y-%m-%dT%H:%M:%S.%3f%z\",\"body\": [{{\"dev\": \"{dev_guid}\",\"body\": [{{\"name\": \"{tag}\",\"val\": \"\",\"unit\": \"\",\"datatype\": \"\"}}]}}]}}")
                    );
                    json_write_tag.insert(
                        pid,
                        "body/_array/body/_array/val;timestamp".to_string()
                    );
                }
            }
        }
        // 遥调通道
        current_tid = current_tid + 1;
        let yt_mt = MqttTransport {
            id: current_tid,
            name: format!("{}_定值设置", t.name()),
            mqtt_broker: t.mqtt_broker(),
            point_id: 0_u64,
            point_ids: point_yt_ids,
            read_topic: "".to_string(),
            write_topic: format!("/svc.dbc/{APP_NAME}/S-dataservice/F-SetPara"),
            is_json: true,
            is_transfer: false,
            keep_alive: None,
            user_name: None,
            user_password: None,
            array_filter: Some("body".to_string()),
            json_filters: None,
            json_tags: None,
            json_write_template: Some(json_write_template),
            json_write_tag: Some(json_write_tag),
        };
        let point_yk = t.point_yk_ids();
        let point_yk_ids = point_yk.iter()
            .filter(|v|points_mapping.contains_key(*v))
            .map(|v| (*points_mapping.get(v).unwrap(), true)).collect::<Vec<(u64, bool)>>();
        let mut json_write_template = HashMap::new();
        let mut json_write_tag = HashMap::new();
        for v in point_yk.iter() {
            if points_mapping.contains_key(v) {
                if let Some(tag) = get_north_tag(v) {
                    let pid = *points_mapping.get(v).unwrap();
                    json_write_template.insert(
                        pid,
                        format!("{{\"token\": \"123\",\"time\": \"%Y-%m-%dT%H:%M:%S.%3f%z\",\"body\": [{{\"dev\": \"{dev_guid}\",\"name\": \"{tag}\",\"type\": \"SCO\",\"cmd\": \"0\",\"action\": \"1\",\"mode\": \"0\",\"timeout\": \"10\"}}]}}")
                    );
                    json_write_tag.insert(
                        pid,
                        "body/_array/body/_array/val;timestamp".to_string()
                    );
                }
            }
        }
        current_tid = current_tid + 1;
        let yk_mt = MqttTransport {
            id: current_tid,
            name: format!("{}_遥控命令转发", t.name()),
            mqtt_broker: t.mqtt_broker(),
            point_id: 0_u64,
            point_ids: point_yk_ids,
            read_topic: "".to_string(),
            write_topic: format!("/svc.brd/{APP_NAME}/S-dataservice/F-RemoteCtrl"),
            is_json: true,
            is_transfer: false,
            keep_alive: None,
            user_name: None,
            user_password: None,
            array_filter: Some("body".to_string()),
            json_filters: None,
            json_tags: None,
            json_write_template: Some(json_write_template),
            json_write_tag: Some(json_write_tag),
        };
        // 遥控通道
        transports_result.push(Transport::Mqtt(ycyx_mt1));
        transports_result.push(Transport::Mqtt(ycyx_mt2));
        transports_result.push(Transport::Mqtt(yt_mt));
        transports_result.push(Transport::Mqtt(yk_mt));
        println!("{:?}", transports_result);
    }
    transports_result
}
