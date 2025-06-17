use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;
use std::vec;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use north::MyPoints;
use south::Measurement;
use south::{DataUnit, Expr};

use crate::model::north::*;
use crate::model::south::*;
use crate::utils::{get_north_tag, replace_point, get_point_tag};
use crate::ADAPTER_NAME;
use crate::env::Env;

pub mod north;
pub mod south;
pub mod datacenter;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ParserResult {
    pub result: bool,
    pub err: String,
}

pub fn points_to_south(points: MyPoints) -> Result<(Vec<Measurement>, Vec<(String, u64)>), String> {
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
    Ok((points_result, mapping_result))
}

pub fn transports_to_south(transports: MyTransports, points_mapping: &HashMap<String, u64>, dev_guids: &HashMap<String, String>) -> Result<(Vec<Transport>, u64), String> {
    let env = Env::get_env(ADAPTER_NAME);
    let mqtt_broker = (env.get_mqtt_server(), env.get_mqtt_server_port());
    let new_transport = MyMqttTransportJoin::from_vec(transports.transports)?;
    let app_name = env.get_app_name();
    let mut transports_result = vec![];

    // 遥测遥信
    let mut filter_keys = vec![];
    let mut filter_values = vec![];
    let mut filter_keys_cx = vec![];
    let mut json_tags = HashMap::new();
    let mut point_ycyx_index = 0;
    let mut point_ycyx_ids = vec![];

    // 遥调
    let mut point_yt_ids = vec![];
    let mut json_write_template_yt = HashMap::new();
    let mut json_write_tag_yt = HashMap::new();
    
    // 遥控
    let mut point_yk_ids = vec![];
    let mut json_write_template_yk = HashMap::new();
    let mut json_write_tag_yk = HashMap::new();

    let mut current_tid = 65536_u64;
    for (dev_id, (point_ycyx, point_yt, point_yk)) in new_transport.dev_ids_map.iter() {
        if !dev_guids.contains_key(dev_id) {
            return Err(format!("找不到设备GUID：{dev_id}"));
        }
        let dev_guid = dev_guids.get(dev_id).unwrap();
        let points = point_ycyx.iter()
            .filter(|v|points_mapping.contains_key(*v))
            .map(|v| (*points_mapping.get(v).unwrap(), false)).collect::<Vec<(u64, bool)>>();
        point_ycyx_ids.extend(points);
        for v in point_ycyx.iter() {
            if points_mapping.contains_key(v) {
                let mut value_map = HashMap::with_capacity(1);
                value_map.insert("val".to_string(), point_ycyx_index);
                json_tags.insert(format!("[{point_ycyx_index}]"), value_map);
                if let Some(tag) = get_north_tag(v) {
                    filter_keys.push(vec![
                        "body/_array/name".to_string(),
                        "body/_array/quality".to_string(),
                        "dev".to_string()
                    ]);
                    filter_values.push(Some(vec![str_to_json_value(&tag),
                        str_to_json_value("1"),
                        str_to_json_value(&dev_guid)
                    ]));
                    filter_keys_cx.push(vec![
                        "body/_array/body/_array/name".to_string(),
                        "body/_array/body/_array/quality".to_string(),
                        "body/_array/dev".to_string()
                    ]);
                }
            }
            point_ycyx_index = point_ycyx_index + 1;
        }
        let points = point_yt.iter()
            .filter(|v|points_mapping.contains_key(*v))
            .map(|v| (*points_mapping.get(v).unwrap(), true)).collect::<Vec<(u64, bool)>>();
        point_yt_ids.extend(points);
        for v in point_yt.iter() {
            if points_mapping.contains_key(v) {
                if let Some(tag) = get_north_tag(v) {
                    let pid = *points_mapping.get(v).unwrap();
                    json_write_template_yt.insert(
                        pid,
                        format!("{{\"token\": \"plcc_yt\",\"time\": \"%Y-%m-%dT%H:%M:%S.%3f%z\",\"body\": [{{\"dev\": \"{dev_guid}\",\"body\": [{{\"name\": \"{tag}\",\"val\": \"\",\"unit\": \"\",\"datatype\": \"\"}}]}}]}}")
                    );
                    json_write_tag_yt.insert(
                        pid,
                        "body/_array/body/_array/val;time".to_string()
                    );
                }
            }
        }
        let points = point_yk.iter()
            .filter(|v|points_mapping.contains_key(*v))
            .map(|v| (*points_mapping.get(v).unwrap(), true)).collect::<Vec<(u64, bool)>>();
        point_yk_ids.extend(points);
        for v in point_yk.iter() {
            if points_mapping.contains_key(v) {
                if let Some(tag) = get_north_tag(v) {
                    let pid = *points_mapping.get(v).unwrap();
                    json_write_template_yk.insert(
                        pid,
                        format!("{{\"token\": \"plcc_yk\",\"time\": \"%Y-%m-%dT%H:%M:%S.%3f%z\",\"body\": [{{\"dev\": \"{dev_guid}\",\"name\": \"{tag}\",\"type\": \"SCO\",\"cmd\": \"0\",\"action\": \"1\",\"mode\": \"0\",\"timeout\": \"10\"}}]}}")
                    );
                    json_write_tag_yk.insert(
                        pid,
                        "body/_array/cmd;time".to_string()
                    );
                }
            }
        }
    }
    // 遥测遥信通道
    if !point_ycyx_ids.is_empty() {
        current_tid = current_tid + 1;
        let ycyx_mt1 = MqttTransport {
            id: current_tid,
            name: format!("{}_实时数据写", new_transport.name.clone()),
            mqtt_broker: mqtt_broker.clone(),
            point_id: 0_u64,
            point_ids: point_ycyx_ids.clone(),
            read_topic: "/sys.dbc/+/S-dataservice/F-SetRealData".to_string(),
            write_topic: "".to_string(),
            is_json: true,
            is_transfer: false,
            keep_alive: None,
            user_name: None,
            user_password: None,
            array_filter: None,
            filter_keys: Some(filter_keys.clone()),
            filter_values: Some(filter_values.clone()),
            json_tags: Some(json_tags.clone()),
            json_write_template: None,
            json_write_tag: None,
        };
        current_tid = current_tid + 1;
        let ycyx_mt2 = MqttTransport {
            id: current_tid,
            name: format!("{}_实时数据更新通知", new_transport.name.clone()),
            mqtt_broker: mqtt_broker.clone(),
            point_id: 0_u64,
            point_ids: point_ycyx_ids.clone(),
            read_topic: "/sys.brd/+/S-dataservice/F-UpdateRealData".to_string(),
            write_topic: "".to_string(),
            is_json: true,
            is_transfer: false,
            keep_alive: None,
            user_name: None,
            user_password: None,
            array_filter: None,
            filter_keys: Some(filter_keys),
            filter_values: Some(filter_values.clone()),
            json_tags: Some(json_tags.clone()),
            json_write_template: None,
            json_write_tag: None,
        };
        current_tid = current_tid + 1;
        let ycyx_mt3 = MqttTransport {
            id: current_tid,
            name: format!("{}_实时数据查询", new_transport.name.clone()),
            mqtt_broker: mqtt_broker.clone(),
            point_id: 0_u64,
            point_ids: point_ycyx_ids,
            read_topic: format!("/{app_name}/sys.dbc/S-dataservice/F-GetRealData"),
            write_topic: "".to_string(),
            is_json: true,
            is_transfer: false,
            keep_alive: None,
            user_name: None,
            user_password: None,
            array_filter: None,
            filter_keys: Some(filter_keys_cx),
            filter_values: Some(filter_values),
            json_tags: Some(json_tags),
            json_write_template: None,
            json_write_tag: None,
        };
        transports_result.push(Transport::Mqtt(ycyx_mt1));
        transports_result.push(Transport::Mqtt(ycyx_mt2));
        transports_result.push(Transport::Mqtt(ycyx_mt3));
    }
    // 遥调通道
    if !point_yt_ids.is_empty() {
        current_tid = current_tid + 1;
        let yt_mt = MqttTransport {
            id: current_tid,
            name: format!("{}_定值设置", new_transport.name.clone()),
            mqtt_broker: mqtt_broker.clone(),
            point_id: 0_u64,
            point_ids: point_yt_ids,
            read_topic: "".to_string(),
            write_topic: format!("/sys.dbc/{app_name}/S-dataservice/F-SetPara"),
            is_json: true,
            is_transfer: false,
            keep_alive: None,
            user_name: None,
            user_password: None,
            array_filter: Some("body".to_string()),
            filter_keys: None,
            filter_values: None,
            json_tags: None,
            json_write_template: Some(json_write_template_yt),
            json_write_tag: Some(json_write_tag_yt),
        };
        transports_result.push(Transport::Mqtt(yt_mt));
    }
    // 遥控通道
    if !point_yk_ids.is_empty() {
        current_tid = current_tid + 1;
        let yk_mt = MqttTransport {
            id: current_tid,
            name: format!("{}_遥控命令转发", new_transport.name.clone()),
            mqtt_broker: mqtt_broker.clone(),
            point_id: 0_u64,
            point_ids: point_yk_ids,
            read_topic: "".to_string(),
            write_topic: format!("/sys.brd/{app_name}/S-dataservice/F-RemoteCtrl"),
            is_json: true,
            is_transfer: false,
            keep_alive: None,
            user_name: None,
            user_password: None,
            array_filter: Some("body".to_string()),
            filter_keys: None,
            filter_values: None,
            json_tags: None,
            json_write_template: Some(json_write_template_yk),
            json_write_tag: Some(json_write_tag_yk),
        };
        transports_result.push(Transport::Mqtt(yk_mt));
    }
    Ok((transports_result, current_tid))
}

pub fn aoes_to_south(aoes: MyAoes, points_mapping: &HashMap<String, u64>, current_id: u64) -> Result<Vec<AoeModel>, String> {
    let aoes = replace_point_for_aoe(aoes, points_mapping)?;
    let mut aoes_result = vec![];
    let mut current_id = current_id;
    for a in aoes.aoes {
        current_id = current_id + 1;
        let trigger_type = trigger_type_to_south(a.trigger_type)?;
        let variables = variables_to_south(a.variables)?;
        let events = events_to_south(current_id, a.events)?;
        let actions = actions_to_south(current_id, a.actions)?;
        let aoe = AoeModel {
            id: current_id,
            name: a.name,
            events,
            actions,
            trigger_type,
            variables,
        };
        aoes_result.push(aoe);
    }
    Ok(aoes_result)
}

fn trigger_type_to_south(north: MyTriggerType) -> Result<TriggerType, String> {
    match north {
        MyTriggerType::SimpleRepeat(v) => {
            if let Ok(m) = v.parse::<u64>() {
                Ok(TriggerType::SimpleRepeat(Duration::from_millis(m)))
            } else {
                Err("触发类型SimpleRepeat参数错误".to_string())
            }
        },
        MyTriggerType::TimeDrive(v) => Ok(TriggerType::TimeDrive(v)),
        MyTriggerType::EventDrive(_) => Ok(TriggerType::EventDrive),
        MyTriggerType::EventRepeatMix(v) => {
            if let Ok(m) = v.parse::<u64>() {
                Ok(TriggerType::EventRepeatMix(Duration::from_millis(m)))
            } else {
                Err("触发类型EventRepeatMix参数错误".to_string())
            }
        },
        MyTriggerType::EventTimeMix(v) => Ok(TriggerType::EventTimeMix(v)),
    }
}

fn variables_to_south(north: Vec<(String, String)>) -> Result<Vec<(String, Expr)>, String> {
    let mut variables: Vec<(String, Expr)> = vec![];
    for id_to_value in north {
        let mut var_name = id_to_value.0.trim().to_string();
        // 检查变量名
        if !var_name.is_empty() {
            if let Ok(var_name_expr) = var_name.parse::<Expr>() {
                for token in &var_name_expr.rpn {
                    match token {
                        Token::Var(n) => var_name = n.clone(),
                        _ => return Err(format!("解析错误：{var_name}")),
                    }
                }
            } else {
                return Err(format!("解析错误：{var_name}"));
            }
        }
        // 检查是否重复变量定义
        for (vari, _) in &variables {
            if vari.eq(&var_name) {
                return Err(format!("变量重复：{var_name}"));
            }
        }
        let init_v: Expr = id_to_value.1.parse().map_err(|_| format!("解析错误：{var_name}"))?;
        variables.push((var_name, init_v));
    }
    Ok(variables)
}

fn events_to_south(aoe_id: u64, north: Vec<MyEventNode>) -> Result<Vec<EventNode>, String> {
    let mut events = vec![];
    for event_n in north {
        let expr_n = event_n.expr;
        let expr: Expr = expr_n.parse().map_err(|_| format!("解析错误：{expr_n}"))?;
        if !expr.check_validity() {
            return Err(format!("公式不可用：{expr_n}"));
        }
        let event_s = EventNode {
            id: event_n.id,
            aoe_id,
            name: event_n.name,
            node_type: event_n.node_type,
            expr,
            timeout: event_n.timeout,
        };
        events.push(event_s);
    }
    Ok(events)
}

fn actions_to_south(aoe_id: u64, north: Vec<MyActionEdge>) -> Result<Vec<ActionEdge>, String> {
    let mut actions = vec![];
    for action_n in north {
        let action = match action_n.action {
            MyEigAction::None => EigAction::None,
            MyEigAction::SetPoints(my_set_points) => EigAction::SetPoints(get_set_points(my_set_points)?),
            MyEigAction::SetPointsWithCheck(my_set_points) => EigAction::SetPointsWithCheck(get_set_points(my_set_points)?),
            MyEigAction::SetPoints2(my_set_points) => EigAction::SetPoints2(get_set_points2(my_set_points)?),
            MyEigAction::SetPointsWithCheck2(my_set_points) => EigAction::SetPointsWithCheck2(get_set_points2(my_set_points)?),
            MyEigAction::Solve(my_solver) => EigAction::Solve(get_sparse_solver(my_solver)?),
            MyEigAction::Nlsolve(my_solver) => EigAction::Nlsolve(get_newton_solver(my_solver)?),
            MyEigAction::Milp(my_programming) => EigAction::Milp(get_milp(my_programming)?),
            MyEigAction::SimpleMilp(my_programming) => EigAction::SimpleMilp(get_simple_milp(my_programming)?),
            MyEigAction::Nlp(my_programming) => EigAction::Nlp(get_nlp(my_programming)?),
            MyEigAction::Url(url) => EigAction::Url(url),
        };
        let action_s = ActionEdge {
            aoe_id,
            name: action_n.name,
            source_node: action_n.source_node,
            target_node: action_n.target_node,
            failure_mode: action_n.failure_mode,
            action,
        };
        actions.push(action_s);
    }
    Ok(actions)
}

fn get_set_points(my_set_points: MySetPoints) -> Result<SetPoints, String> {
    let discrete_id = my_set_points.discretes.keys().cloned().collect();
    let discrete_v = my_set_points.discretes.values().map(|v| {
        v.parse().map_err(|_| format!("解析错误：{v}"))
    }).collect::<Result<_, _>>()?;
    let analog_id = my_set_points.analogs.keys().cloned().collect();
    let analog_v = my_set_points.analogs.values().map(|v| {
        v.parse().map_err(|_| format!("解析错误：{v}"))
    }).collect::<Result<_, _>>()?;
    Ok(SetPoints {
        discrete_id,
        discrete_v,
        analog_id,
        analog_v,
    })
}

fn get_set_points2(my_set_points: MySetPoints) -> Result<SetPoints2, String> {
    let discretes = my_set_points.discretes.iter().map(|(k, v)| {
        let ids = k.split(";").map(|v| v.to_string()).collect::<Vec<String>>();
        let expr: Expr = v.parse().map_err(|_| format!("解析错误：{v}"))?;
        if !expr.check_validity() {
            return Err(format!("公式不可用：{v}"));
        }
        Ok(PointsToExp {
            ids,
            expr
        })
    }).collect::<Result<_, _>>()?;
    let analogs = my_set_points.analogs.iter().map(|(k, v)| {
        let ids = k.split(";").map(|v| v.to_string()).collect::<Vec<String>>();
        let expr: Expr = v.parse().map_err(|_| format!("解析错误：{v}"))?;
        if !expr.check_validity() {
            return Err(format!("公式不可用：{v}"));
        }
        Ok(PointsToExp {
            ids,
            expr
        })
    }).collect::<Result<_, _>>()?;
    Ok(SetPoints2 {
        discretes,
        analogs,
    })
}

fn get_sparse_solver(my_solver: MySolver) -> Result<SparseSolver, String> {
    let expr_ori= my_solver.f;
    if expr_ori.is_empty() {
        return Err("公式不能为空".to_string());
    }
    let expr_str: Vec<&str> = expr_ori.split(';').collect();
    let para_str = my_solver.parameters.iter().map(|(k, v)|format!("{k}:{v}")).collect::<Vec<String>>();
    let para_str: Vec<&str> = para_str.iter().map(|s| s.as_str()).collect();
    match SparseSolver::from_str_with_parameters(&expr_str, &para_str) {
        Ok(model) => Ok(model),
        Err((i, line)) => {
            let err_str = if i == 0 {
                expr_str[line - 1]
            } else {
                para_str[line - 1]
            };
            Err(format!("公式解析错误：{line},{err_str}"))
        }
    }
}

fn get_newton_solver(my_solver: MySolver) -> Result<NewtonSolver, String> {
    let expr_ori= my_solver.f;
    if expr_ori.is_empty() {
        return Err("公式不能为空".to_string());
    }
    let expr_str: Vec<&str> = expr_ori.split(';').collect();
    let para_str = my_solver.parameters.iter().map(|(k, v)|format!("{k}:{v}")).collect::<Vec<String>>();
    let para_str: Vec<&str> = para_str.iter().map(|s| s.as_str()).collect();
    match NewtonSolver::from_str_with_parameters(&expr_str, &para_str) {
        Ok(model) => Ok(model),
        Err((i, line)) => {
            let err_str = if i == 0 {
                expr_str[line - 1]
            } else {
                para_str[line - 1]
            };
            Err(format!("公式解析错误：{line},{err_str}"))
        }
    }
}

fn get_milp(my_programming: MyProgramming) -> Result<SparseMILP, String> {
    let mut expr_ori= my_programming.f;
    if expr_ori.is_empty() {
        return Err("公式不能为空".to_string());
    }
    let constraint = my_programming.constraint;
    if !constraint.is_empty() {
        expr_ori = format!("{expr_ori};{constraint}");
    }
    let expr_str: Vec<&str> = expr_ori.split(';').collect();
    let para_str = my_programming.parameters.iter().map(|(k, v)|format!("{k}:{v}")).collect::<Vec<String>>();
    let para_str: Vec<&str> = para_str.iter().map(|s| s.as_str()).collect();
    match SparseMILP::from_str_with_parameters(&expr_str, &para_str) {
        Ok(model) => Ok(model),
        Err((i, line)) => {
            let err_str = if i == 0 {
                expr_str[line - 1]
            } else {
                para_str[line - 1]
            };
            Err(format!("公式解析错误：{line},{err_str}"))
        }
    }
}

fn get_simple_milp(my_programming: MyProgramming) -> Result<MILP, String> {
    let mut expr_ori= my_programming.f;
    if expr_ori.is_empty() {
        return Err("公式不能为空".to_string());
    }
    let constraint = my_programming.constraint;
    if !constraint.is_empty() {
        expr_ori = format!("{expr_ori};{constraint}");
    }
    let expr_str: Vec<&str> = expr_ori.split(';').collect();
    let para_str = my_programming.parameters.iter().map(|(k, v)|format!("{k}:{v}")).collect::<Vec<String>>();
    let para_str: Vec<&str> = para_str.iter().map(|s| s.as_str()).collect();
    match MILP::from_str_with_parameters(&expr_str, &para_str) {
        Ok(model) => Ok(model),
        Err((i, line)) => {
            let err_str = if i == 0 {
                expr_str[line - 1]
            } else {
                para_str[line - 1]
            };
            Err(format!("公式解析错误：{line},{err_str}"))
        }
    }
}

fn get_nlp(my_programming: MyProgramming) -> Result<NLP, String> {
    let mut expr_ori= my_programming.f;
    if expr_ori.is_empty() {
        return Err("公式不能为空".to_string());
    }
    let constraint = my_programming.constraint;
    if !constraint.is_empty() {
        expr_ori = format!("{expr_ori};{constraint}");
    }
    let expr_str: Vec<&str> = expr_ori.split(';').collect();
    let para_str = my_programming.parameters.iter().map(|(k, v)|format!("{k}:{v}")).collect::<Vec<String>>();
    let para_str: Vec<&str> = para_str.iter().map(|s| s.as_str()).collect();
    match NLP::from_str_with_parameters(&expr_str, &para_str) {
        Ok(model) => Ok(model),
        Err((i, line)) => {
            let err_str = if i == 0 {
                expr_str[line - 1]
            } else {
                para_str[line - 1]
            };
            Err(format!("公式解析错误：{line},{err_str}"))
        }
    }
}

fn replace_point_for_aoe(aoes: MyAoes, points_mapping: &HashMap<String, u64>) -> Result<MyAoes, String> {
    let mut aoes = aoes.aoes;
    for aoe in aoes.iter_mut() {
        let mut new_variables = vec![];
        for (key, value) in &aoe.variables {
            let key = replace_point(key, points_mapping)?;
            let value = replace_point(value, points_mapping)?;
            new_variables.push((key, value));
        }
        aoe.variables = new_variables;
        let mut new_events = vec![];
        for mut event in aoe.events.clone() {
            event.expr = replace_point(&event.expr, points_mapping)?;
            new_events.push(event);
        }
        aoe.events = new_events;
        let mut new_actions = vec![];
        for mut edge in aoe.actions.clone() {
            let new_action = match edge.action {
                MyEigAction::None => MyEigAction::None,
                MyEigAction::SetPoints(my_set_points) => MyEigAction::SetPoints(replace_point_for_set_points(my_set_points, points_mapping)?),
                MyEigAction::SetPointsWithCheck(my_set_points) => MyEigAction::SetPointsWithCheck(replace_point_for_set_points(my_set_points, points_mapping)?),
                MyEigAction::SetPoints2(my_set_points) => MyEigAction::SetPoints2(replace_point_for_set_points(my_set_points, points_mapping)?),
                MyEigAction::SetPointsWithCheck2(my_set_points) => MyEigAction::SetPointsWithCheck2(replace_point_for_set_points(my_set_points, points_mapping)?),
                MyEigAction::Solve(my_solver) => MyEigAction::Solve(replace_point_for_solver(my_solver, points_mapping)?),
                MyEigAction::Nlsolve(my_solver) => MyEigAction::Nlsolve(replace_point_for_solver(my_solver, points_mapping)?),
                MyEigAction::Milp(my_programming) => MyEigAction::Milp(replace_point_for_programming(my_programming, points_mapping)?),
                MyEigAction::SimpleMilp(my_programming) => MyEigAction::SimpleMilp(replace_point_for_programming(my_programming, points_mapping)?),
                MyEigAction::Nlp(my_programming) => MyEigAction::Nlp(replace_point_for_programming(my_programming, points_mapping)?),
                MyEigAction::Url(url) => MyEigAction::Url(url),
            };
            edge.action = new_action;
            new_actions.push(edge);
        }
        aoe.actions = new_actions;
    }
    Ok(MyAoes{aoes})
}

fn replace_point_for_set_points(my_set_points: MySetPoints, points_mapping: &HashMap<String, u64>) -> Result<MySetPoints, String> {
    let mut discretes = HashMap::with_capacity(my_set_points.discretes.len());
    for (key, value) in &my_set_points.discretes {
        let key = replace_point(key, &points_mapping)?;
        let value = replace_point(value, &points_mapping)?;
        discretes.insert(key, value);
    }
    let mut analogs = HashMap::with_capacity(my_set_points.analogs.len());
    for (key, value) in &my_set_points.analogs {
        let key = replace_point(key, &points_mapping)?;
        let value = replace_point(value, &points_mapping)?;
        analogs.insert(key, value);
    }
    Ok(MySetPoints{discretes, analogs})
}

fn replace_point_for_solver(my_solver: MySolver, points_mapping: &HashMap<String, u64>) -> Result<MySolver, String> {
    let mut parameters = HashMap::with_capacity(my_solver.parameters.len());
    for (key, value) in &my_solver.parameters {
        let key = replace_point(key, &points_mapping)?;
        let value = replace_point(value, &points_mapping)?;
        parameters.insert(key, value);
    }
    let f = replace_point(&my_solver.f, &points_mapping)?;
    let x = replace_point(&my_solver.x, &points_mapping)?;
    Ok(MySolver{f, x, parameters})
}

fn replace_point_for_programming(my_programming: MyProgramming, points_mapping: &HashMap<String, u64>) -> Result<MyProgramming, String> {
    let mut parameters = HashMap::with_capacity(my_programming.parameters.len());
    for (key, value) in &my_programming.parameters {
        let key = replace_point(key, &points_mapping)?;
        let value = replace_point(value, &points_mapping)?;
        parameters.insert(key, value);
    }
    let f = replace_point(&my_programming.f, &points_mapping)?;
    let x = replace_point(&my_programming.x, &points_mapping)?;
    let constraint = replace_point(&my_programming.constraint, &points_mapping)?;
    Ok(MyProgramming{f, x, constraint, parameters})
}

fn str_to_json_value(value: &str) -> Value {
    Value::String(value.to_string())
}

pub fn aoe_action_result_to_north(action_result: PbActionResult, points_mapping: &HashMap<u64, String>) -> Result<MyPbActionResult, String> {
    let yk_points = action_result.yk_points.iter().filter_map(|point|{
        get_point_tag(point, &points_mapping).ok()
    }).collect::<Vec<String>>();
    let yt_points = action_result.yt_points.iter().filter_map(|point|{
        get_point_tag(point, &points_mapping).ok()
    }).collect::<Vec<String>>();
    Ok(MyPbActionResult{
        source_id: action_result.source_id,
        target_id: action_result.target_id,
        start_time: action_result.start_time,
        end_time: action_result.end_time,
        final_result: action_result.final_result,
        fail_code: action_result.fail_code,
        yk_points,
        yk_values: action_result.yk_values,
        yt_points,
        yt_values: action_result.yt_values,
        variables: action_result.variables,
        var_values: action_result.var_values,
    })
}