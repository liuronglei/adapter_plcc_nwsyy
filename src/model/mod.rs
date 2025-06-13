use std::str::FromStr;
use serde::{Deserialize, Serialize};

use north::MyPoints;
use south::Measurement;
use south::{DataUnit, Expr};

use crate::model::north::MyTransports;
use crate::model::south::MqttTransport;

pub mod north;
pub mod south;
pub mod datacenter;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ParserResult {
    pub result: bool,
    pub err: String,
}

pub fn points_to_south(points: MyPoints) -> (Vec<Measurement>, Vec<(u64, String)>) {
    let mut points_result = vec![];
    let mut mapping_result = vec![];
    let current_pid = 100000_u64;
    for p in points.points {
        let unit = DataUnit::from_str(p.data_unit.as_str()).unwrap_or(DataUnit::Unknown);
        let alarm_level1 = Expr::from_str(p.alarm_level1_expr.as_str()).ok();
        let alarm_level2 = Expr::from_str(p.alarm_level2_expr.as_str()).ok();
        let new_point = current_pid + 1;
        points_result.push(Measurement {
            point_id: new_point,
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
        mapping_result.push((new_point, p.point_id));
    }
    (points_result, mapping_result)
}

pub fn transports_to_south(transports: MyTransports) {
    let current_tid = 65536_u64;
    for t in transports.transports {
        let new_trans = current_tid + 1;
        let point_ycyx = t.point_ycyx_ids();
        let point_yt = t.point_yt_ids();
        let point_yk = t.point_yk_ids();
        // MqttTransport {
        //     id: new_trans,
        //     name: t.name(),
        //     mqtt_broker: t.mqtt_broker(),
        //     point_id: todo!(),
        //     point_ids: todo!(),
        //     read_topic: todo!(),
        //     write_topic: todo!(),
        //     is_json: todo!(),
        //     is_transfer: todo!(),
        //     keep_alive: todo!(),
        //     user_name: "".to_string(),
        //     user_password: "".to_string(),
        //     array_filter: todo!(),
        //     json_filters: todo!(),
        //     json_tags: todo!(),
        //     json_write_template: todo!(),
        //     json_write_tag: todo!(),
        // }
    }
}
