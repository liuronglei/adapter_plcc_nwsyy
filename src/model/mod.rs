use core::f64;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;
use std::vec;
use serde_json::Value;
use polars_core::frame::DataFrame;

use north::MyPoints;
use south::Measurement;
use south::{DataUnit, Expr};

use crate::model::north::*;
use crate::model::south::*;
use crate::utils::{replace_point, replace_point_without_prefix, get_point_attr, get_point_tag};
use crate::{AdapterErr, ErrCode, ADAPTER_NAME};
use crate::env::Env;
use crate::utils::parse::{load_prog, create_stmt_tree};

pub mod north;
pub mod south;
pub mod datacenter;

pub fn points_to_south(points: MyPoints, old_point_mapping: &HashMap<String, u64>) -> Result<(Vec<Measurement>, HashMap<String, u64>, HashMap<String, PointParam>, HashMap<String, bool>), AdapterErr> {
    let points = points.points;
    if points.is_none() {
        return Err(AdapterErr {
            code: ErrCode::PointIsEmpty,
            msg: "测点列表不能为空".to_string(),
        });
    }
    let mut points = points.unwrap();
    let mut points_result = vec![];
    let mut mapping_result = HashMap::new();
    let mut point_param = HashMap::new();
    let mut point_discrete = HashMap::new();
    let mut current_pid = if let Some(max_point) = old_point_mapping.values().copied().max() {
        max_point
    } else {
        100000_u64
    };
    // 排序，先普通测点，后计算测点，避免计算公式替换的时候所引用的测点还未创建
    points.sort_by_key(|m| m.is_computing_point);
    for p in points {
        let unit = DataUnit::from_str(p.data_unit.as_str()).unwrap_or(DataUnit::Unknown);
        let alarm_level1 = Expr::from_str(p.alarm_level1_expr.as_str()).ok();
        let alarm_level2 = Expr::from_str(p.alarm_level2_expr.as_str()).ok();
        let expression = if p.is_computing_point {
            replace_point(&p.expression, &mapping_result)?
        } else {
            p.expression
        };
        let point_id = if let Some(pid) = old_point_mapping.get(&p.point_id) {
            *pid
        } else {
            current_pid = current_pid + 1;
            current_pid
        };
        points_result.push(Measurement {
            point_id,
            point_name: p.point_name,
            alias_id: p.alias_id,
            is_discrete: p.is_discrete,
            is_computing_point: p.is_computing_point,
            expression: expression.clone(),
            trans_expr: p.trans_expr,
            inv_trans_expr: p.inv_trans_expr,
            change_expr: p.change_expr,
            zero_expr: p.zero_expr,
            data_unit: p.data_unit,
            unit,
            upper_limit: p.upper_limit.unwrap_or(f64::MAX),
            lower_limit: p.lower_limit.unwrap_or(f64::MIN),
            alarm_level1_expr: p.alarm_level1_expr,
            alarm_level1,
            alarm_level2_expr: p.alarm_level2_expr,
            alarm_level2,
            is_realtime: p.is_realtime,
            is_soe: p.is_soe,
            init_value: p.init_value,
            desc: p.desc,
            is_remote: false,
        });
        if !p.point_id.is_empty() {
            point_discrete.insert(p.point_id.clone(), p.is_discrete);
            mapping_result.insert(p.point_id.clone(), point_id);
        } else if !expression.is_empty() {
            mapping_result.insert(expression, point_id);
        }
        if let Some(param) = p.param {
           point_param.insert(p.point_id, param);
        }
    }
    Ok((points_result, mapping_result, point_param, point_discrete))
}

pub fn transports_to_south(
        transports: MyTransports,
        points_mapping: &HashMap<String, u64>,
        dev_mapping: &HashMap<(String, String, String), (String, String, String)>,
        point_param: &HashMap<String, PointParam>,
        point_discrete: &HashMap<String, bool>) -> Result<(Vec<Transport>, u64), AdapterErr> {
    let env = Env::get_env(ADAPTER_NAME);
    let mqtt_broker = (env.get_mqtt_server(), env.get_mqtt_server_port());
    let new_transport = MyMqttTransportJoin::from_vec(transports.transports)?;
    let app_name = env.get_app_name();
    let mut transports_result = vec![];

    // 遥测遥信
    let mut point_ycyx_ids = vec![];
    let mut filter_keys_ycyx = vec![];
    let mut filter_values_ycyx = vec![];
    let mut json_tags_ycyx = HashMap::new();
    let mut point_index_ycyx = 0;
    let mut filter_keys_cx = vec![];

    // 遥调
    let mut point_yt_ids = vec![];
    let mut filter_keys_yt = vec![];
    let mut filter_values_yt = vec![];
    let mut json_write_template_yt = HashMap::new();
    let mut json_write_tag_yt = HashMap::new();
    let mut json_tags_yt = HashMap::new();
    let mut point_yt_index = 0;
    
    // 遥控
    let mut point_yk_ids = vec![];
    let mut filter_keys_yk = vec![];
    let mut filter_values_yk = vec![];
    let mut json_write_template_yk = HashMap::new();
    let mut json_write_tag_yk = HashMap::new();
    let mut json_tags_yk = HashMap::new();
    let mut point_yk_index = 0;

    let mut current_tid = 65536_u64;
    for (_, (point_ycyx, point_yt, point_yk)) in new_transport.dev_ids_map.iter() {
        let points = point_ycyx.iter()
            .filter(|v|points_mapping.contains_key(*v))
            .map(|v| (*points_mapping.get(v).unwrap(), false)).collect::<Vec<(u64, bool)>>();
        point_ycyx_ids.extend(points);
        for v in point_ycyx.iter() {
            if points_mapping.contains_key(v) {
                if let Some(dev_key) = get_point_attr(v) {
                    if let Some((dev_guid, _, dc_attr)) = dev_mapping.get(&dev_key) {
                        let mut value_map = HashMap::with_capacity(1);
                        value_map.insert("val".to_string(), point_index_ycyx);
                        json_tags_ycyx.insert(format!("[{point_index_ycyx}]"), value_map);
                        filter_keys_ycyx.push(vec![
                            "body/_array/name".to_string(),
                            "body/_array/quality".to_string(),
                            "dev".to_string()
                        ]);
                        filter_values_ycyx.push(Some(vec![str_to_json_value(&dc_attr),
                            str_to_json_value("0"),
                            str_to_json_value(&dev_guid)
                        ]));
                        filter_keys_cx.push(vec![
                            "body/_array/body/_array/name".to_string(),
                            "body/_array/body/_array/quality".to_string(),
                            "body/_array/dev".to_string()
                        ]);
                        point_index_ycyx = point_index_ycyx + 1;
                    } else {
                        return Err(AdapterErr {
                            code: ErrCode::TransportPointTagErr,
                            msg: format!("通道解析失败，属性在数据中心未找到：{v}"),
                        });
                    }
                } else {
                    return Err(AdapterErr {
                        code: ErrCode::TransportPointTagErr,
                        msg: format!("通道解析失败，测点格式错误：{v}"),
                    });
                }
            } else {
                return Err(AdapterErr {
                    code: ErrCode::TransportPointNotFound,
                    msg: format!("通道解析失败，找不到测点：{v}"),
                });
            }
        }
        let points = point_yt.iter()
            .filter(|v|points_mapping.contains_key(*v))
            .map(|v| (*points_mapping.get(v).unwrap(), true)).collect::<Vec<(u64, bool)>>();
        point_yt_ids.extend(points);
        for v in point_yt.iter() {
            if points_mapping.contains_key(v) {
                if let Some(dev_key) = get_point_attr(v) {
                    if let Some((dev_guid, _, dc_attr)) = dev_mapping.get(&dev_key) {
                        let mut value_map = HashMap::with_capacity(1);
                        value_map.insert("val".to_string(), point_yt_index);
                        json_tags_yt.insert(format!("[{point_yt_index}]"), value_map);
                        filter_keys_yt.push(vec!["dev".to_string()]);
                        filter_values_yt.push(Some(vec![str_to_json_value("0")]));
                        let pid = *points_mapping.get(v).unwrap();
                        let is_discrete = if let Some(is_discrete) = point_discrete.get(v) {
                            *is_discrete
                        } else {
                            false
                        };
                        let datatype = if is_discrete {"int"} else {"float"};
                        json_write_template_yt.insert(
                            pid,
                            format!("{{\"token\": \"increment:u16\",\"timestamp\": \"%Y-%m-%dT%H:%M:%S.%3f%z\",\"body\": [{{\"dev\": \"{dev_guid}\",\"timeout\": \"60\",\"body\": [{{\"name\": \"{dc_attr}\",\"val\": \"\",\"unit\": \"\",\"datatype\": \"{datatype}\"}}]}}]}}")
                        );
                        json_write_tag_yt.insert(
                            pid,
                            "body/_array/body/_array/val;timestamp;token".to_string()
                        );
                        point_yt_index = point_yt_index + 1;
                    } else {
                        return Err(AdapterErr {
                            code: ErrCode::TransportPointTagErr,
                            msg: format!("通道解析失败，属性在数据中心未找到：{v}"),
                        });
                    }
                } else {
                    return Err(AdapterErr {
                        code: ErrCode::TransportPointTagErr,
                        msg: format!("通道解析失败，测点格式错误：{v}"),
                    });
                }
            } else {
                return Err(AdapterErr {
                    code: ErrCode::TransportPointNotFound,
                    msg: format!("通道解析失败，找不到测点：{v}"),
                });
            }
        }
        let points = point_yk.iter()
            .filter(|v|points_mapping.contains_key(*v))
            .map(|v| (*points_mapping.get(v).unwrap(), true)).collect::<Vec<(u64, bool)>>();
        point_yk_ids.extend(points);
        for v in point_yk.iter() {
            if points_mapping.contains_key(v) {
                if let Some(dev_key) = get_point_attr(v) {
                    if let Some((dev_guid, _, dc_attr)) = dev_mapping.get(&dev_key) {
                        let mut value_map = HashMap::with_capacity(1);
                        value_map.insert("val".to_string(), point_yk_index);
                        json_tags_yk.insert(format!("[{point_yk_index}]"), value_map);
                        filter_keys_yk.push(vec!["dev".to_string()]);
                        filter_values_yk.push(Some(vec![str_to_json_value("0")]));
                        let pid = *points_mapping.get(v).unwrap();
                        let (action, timeout, mtype, mode) = if let Some(param) = point_param.get(v) {
                            (param.action.clone().unwrap_or("1".to_string()),
                            param.timeout.clone().unwrap_or("60".to_string()),
                            param.mtype.clone().unwrap_or("SCO".to_string()),
                            param.mode.clone().unwrap_or("0".to_string()))
                        } else {
                            ("1".to_string(), "60".to_string(), "SCO".to_string(), "0".to_string())
                        };
                        json_write_template_yk.insert(
                            pid,
                            format!("{{\"token\": \"increment:u16\",\"time\": \"%Y-%m-%dT%H:%M:%S.%3f%z\",\"body\": [{{\"dev\": \"{dev_guid}\",\"name\": \"{dc_attr}\",\"type\": \"{mtype}\",\"cmd\": \"0\",\"action\": \"{action}\",\"mode\": \"{mode}\",\"timeout\": \"{timeout}\"}}]}}")
                        );
                        json_write_tag_yk.insert(
                            pid,
                            "body/_array/cmd;time;token".to_string()
                        );
                        point_yk_index = point_yk_index + 1;
                    } else {
                        return Err(AdapterErr {
                            code: ErrCode::TransportPointTagErr,
                            msg: format!("通道解析失败，属性在数据中心未找到：{v}"),
                        });
                    }
                } else {
                    return Err(AdapterErr {
                        code: ErrCode::TransportPointTagErr,
                        msg: format!("通道解析失败，测点格式错误：{v}"),
                    });
                }
            } else {
                return Err(AdapterErr {
                    code: ErrCode::TransportPointNotFound,
                    msg: format!("通道解析失败，找不到测点：{v}"),
                });
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
            filter_keys: Some(filter_keys_ycyx.clone()),
            filter_values: Some(filter_values_ycyx.clone()),
            json_tags: Some(json_tags_ycyx.clone()),
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
            filter_keys: Some(filter_keys_ycyx),
            filter_values: Some(filter_values_ycyx.clone()),
            json_tags: Some(json_tags_ycyx.clone()),
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
            filter_values: Some(filter_values_ycyx),
            json_tags: Some(json_tags_ycyx),
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
            read_topic: format!("/plcc/yt"),
            write_topic: format!("/sys.brd/{app_name}/S-dataservice/F-SetPara"),
            is_json: true,
            is_transfer: false,
            keep_alive: None,
            user_name: None,
            user_password: None,
            array_filter: Some("body".to_string()),
            filter_keys: Some(filter_keys_yt),
            filter_values: Some(filter_values_yt),
            json_tags: Some(json_tags_yt),
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
            read_topic: format!("/plcc/yk"),
            write_topic: format!("/sys.brd/{app_name}/S-dataservice/F-RemoteCtrl"),
            is_json: true,
            is_transfer: false,
            keep_alive: None,
            user_name: None,
            user_password: None,
            array_filter: Some("body".to_string()),
            filter_keys: Some(filter_keys_yk),
            filter_values: Some(filter_values_yk),
            json_tags: Some(json_tags_yk),
            json_write_template: Some(json_write_template_yk),
            json_write_tag: Some(json_write_tag_yk),
        };
        transports_result.push(Transport::Mqtt(yk_mt));
    }
    Ok((transports_result, current_tid))
}

pub fn aoes_to_south(aoes: MyAoes, points_mapping: &HashMap<String, u64>, current_id: u64) -> Result<(Vec<AoeModel>, HashMap<u64, u64>), AdapterErr> {
    let aoes = replace_point_for_aoe(aoes, points_mapping)?;
    let mut aoes_result = vec![];
    let mut aoes_mapping = HashMap::new();
    let mut current_id = current_id;
    if let Some(aoes) = aoes.aoes {
        for a in aoes {
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
            aoes_mapping.insert(current_id, a.id);
        }
    }
    Ok((aoes_result, aoes_mapping))
}

fn trigger_type_to_south(north: MyTriggerType) -> Result<TriggerType, AdapterErr> {
    match north {
        MyTriggerType::SimpleRepeat(v) => Ok(TriggerType::SimpleRepeat(Duration::from_millis(v))),
        MyTriggerType::TimeDrive(v) => Ok(TriggerType::TimeDrive(v)),
        MyTriggerType::EventDrive(_) => Ok(TriggerType::EventDrive),
        MyTriggerType::EventRepeatMix(v) => Ok(TriggerType::EventRepeatMix(Duration::from_millis(v))),
        MyTriggerType::EventTimeMix(v) => Ok(TriggerType::EventTimeMix(v)),
    }
}

fn variables_to_south(north: Vec<(String, String)>) -> Result<Vec<(String, Expr)>, AdapterErr> {
    let mut variables: Vec<(String, Expr)> = vec![];
    for id_to_value in north {
        let mut var_name = id_to_value.0.trim().to_string();
        // 检查变量名
        if !var_name.is_empty() {
            if let Ok(var_name_expr) = var_name.parse::<Expr>() {
                for token in &var_name_expr.rpn {
                    match token {
                        Token::Var(n) => var_name = n.clone(),
                        _ => return Err(AdapterErr {
                            code: ErrCode::AoeVariableErr,
                            msg: format!("策略变量名解析错误：{var_name}"),
                        }),
                    }
                }
            } else {
                return Err(AdapterErr {
                    code: ErrCode::AoeVariableErr,
                    msg: format!("策略变量名解析错误：{var_name}"),
                });
            }
        }
        // 检查是否重复变量定义
        for (vari, _) in &variables {
            if vari.eq(&var_name) {
                return Err(AdapterErr {
                    code: ErrCode::AoeVariableErr,
                    msg: format!("策略变量名重复：{var_name}"),
                });
            }
        }
        let var_value = id_to_value.1;
        let init_v: Expr = var_value.parse().map_err(|_| AdapterErr {
            code: ErrCode::AoeVariableErr,
            msg: format!("策略变量值解析错误：{var_value}"),
        })?;
        variables.push((var_name, init_v));
    }
    Ok(variables)
}

fn events_to_south(aoe_id: u64, north: Vec<MyEventNode>) -> Result<Vec<EventNode>, AdapterErr> {
    let mut events = vec![];
    for event_n in north {
        let expr_n = event_n.expr;
        let expr: Expr = expr_n.parse().map_err(|_| AdapterErr {
            code: ErrCode::AoeEventErr,
            msg: format!("策略事件公式解析错误：{expr_n}"),
        })?;
        if !expr.check_validity() {
            return Err(AdapterErr {
                code: ErrCode::AoeEventErr,
                msg: format!("策略事件公式不可用：{expr_n}"),
            });
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

fn actions_to_south(aoe_id: u64, north: Vec<MyActionEdge>) -> Result<Vec<ActionEdge>, AdapterErr> {
    let mut actions = vec![];
    for action_n in north {
        let action = match action_n.action {
            MyEigAction::None(_) => EigAction::None,
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

fn get_set_points(my_set_points: MySetPoints) -> Result<SetPoints, AdapterErr> {
    let discrete_id = my_set_points.discretes.keys().cloned().collect();
    let discrete_v = my_set_points.discretes.values().map(|v| {
        v.parse().map_err(|_| AdapterErr {
            code: ErrCode::AoeActionErr,
            msg: format!("策略动作解析错误：{v}"),
        })
    }).collect::<Result<_, _>>()?;
    let analog_id = my_set_points.analogs.keys().cloned().collect();
    let analog_v = my_set_points.analogs.values().map(|v| {
        v.parse().map_err(|_| AdapterErr {
            code: ErrCode::AoeActionErr,
            msg: format!("策略动作解析错误：{v}"),
        })
    }).collect::<Result<_, _>>()?;
    Ok(SetPoints {
        discrete_id,
        discrete_v,
        analog_id,
        analog_v,
    })
}

fn get_set_points2(my_set_points: MySetPoints) -> Result<SetPoints2, AdapterErr> {
    let discretes = my_set_points.discretes.iter().map(|(k, v)| {
        let ids = k.split(";").map(|v| v.to_string()).collect::<Vec<String>>();
        let expr: Expr = v.parse().map_err(|_| AdapterErr {
            code: ErrCode::AoeActionErr,
            msg: format!("策略动作解析错误：{v}"),
        })?;
        if !expr.check_validity() {
            return Err(AdapterErr {
                code: ErrCode::AoeActionErr,
                msg: format!("策略动作公式不可用：{v}"),
            });
        }
        Ok(PointsToExp {
            ids,
            expr
        })
    }).collect::<Result<_, _>>()?;
    let analogs = my_set_points.analogs.iter().map(|(k, v)| {
        let ids = k.split(";").map(|v| v.to_string()).collect::<Vec<String>>();
        let expr: Expr = v.parse().map_err(|_| AdapterErr {
            code: ErrCode::AoeActionErr,
            msg: format!("策略动作解析错误：{v}"),
        })?;
        if !expr.check_validity() {
            return Err(AdapterErr {
                code: ErrCode::AoeActionErr,
                msg: format!("策略动作公式不可用：{v}"),
            });
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

fn get_sparse_solver(my_solver: MySolver) -> Result<SparseSolver, AdapterErr> {
    let expr_ori= my_solver.f;
    if expr_ori.is_empty() {
        return Err(AdapterErr {
            code: ErrCode::AoeActionErr,
            msg: "策略动作公式不能为空".to_string(),
        })
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
            Err(AdapterErr {
                code: ErrCode::AoeActionErr,
                msg: format!("策略动作公式解析错误：{line},{err_str}"),
            })
        }
    }
}

fn get_newton_solver(my_solver: MySolver) -> Result<NewtonSolver, AdapterErr> {
    let expr_ori= my_solver.f;
    if expr_ori.is_empty() {
        return Err(AdapterErr {
            code: ErrCode::AoeActionErr,
            msg: "策略动作公式不能为空".to_string(),
        });
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
            Err(AdapterErr {
                code: ErrCode::AoeActionErr,
                msg: format!("策略动作公式解析错误：{line},{err_str}"),
            })
        }
    }
}

fn get_milp(my_programming: MyProgramming) -> Result<SparseMILP, AdapterErr> {
    let mut expr_ori= my_programming.f;
    if expr_ori.is_empty() {
        return Err(AdapterErr {
            code: ErrCode::AoeActionErr,
            msg: "策略动作公式不能为空".to_string(),
        });
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
            Err(AdapterErr {
                code: ErrCode::AoeActionErr,
                msg: format!("策略动作公式解析错误：{line},{err_str}"),
            })
        }
    }
}

fn get_simple_milp(my_programming: MyProgramming) -> Result<MILP, AdapterErr> {
    let mut expr_ori= my_programming.f;
    if expr_ori.is_empty() {
        return Err(AdapterErr {
            code: ErrCode::AoeActionErr,
            msg: "策略动作公式不能为空".to_string(),
        });
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
            Err(AdapterErr {
                code: ErrCode::AoeActionErr,
                msg: format!("策略动作公式解析错误：{line},{err_str}"),
            })
        }
    }
}

fn get_nlp(my_programming: MyProgramming) -> Result<NLP, AdapterErr> {
    let mut expr_ori= my_programming.f;
    if expr_ori.is_empty() {
        return Err(AdapterErr {
            code: ErrCode::AoeActionErr,
            msg: "策略动作公式不能为空".to_string(),
        });
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
            Err(AdapterErr {
                code: ErrCode::AoeActionErr,
                msg: format!("策略动作公式解析错误：{line},{err_str}"),
            })
        }
    }
}

fn replace_point_for_aoe(mut aoes: MyAoes, points_mapping: &HashMap<String, u64>) -> Result<MyAoes, AdapterErr> {
    if let Some(ref mut aoes) = aoes.aoes {
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
                    MyEigAction::None(str) => MyEigAction::None(str),
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
    }
    Ok(MyAoes{aoes: aoes.aoes, add: None, edit: None, delete: None})
}

fn replace_point_for_set_points(my_set_points: MySetPoints, points_mapping: &HashMap<String, u64>) -> Result<MySetPoints, AdapterErr> {
    let mut discretes = HashMap::with_capacity(my_set_points.discretes.len());
    for (key, value) in &my_set_points.discretes {
        let key = replace_point_without_prefix(key, &points_mapping)?;
        let value = replace_point(value, &points_mapping)?;
        discretes.insert(key, value);
    }
    let mut analogs = HashMap::with_capacity(my_set_points.analogs.len());
    for (key, value) in &my_set_points.analogs {
        let key = replace_point_without_prefix(key, &points_mapping)?;
        let value = replace_point(value, &points_mapping)?;
        analogs.insert(key, value);
    }
    Ok(MySetPoints{discretes, analogs})
}

fn replace_point_for_solver(my_solver: MySolver, points_mapping: &HashMap<String, u64>) -> Result<MySolver, AdapterErr> {
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

fn replace_point_for_programming(my_programming: MyProgramming, points_mapping: &HashMap<String, u64>) -> Result<MyProgramming, AdapterErr> {
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

pub fn aoe_action_result_to_north(action_result: PbActionResult, points_mapping: &HashMap<u64, String>) -> Result<MyPbActionResult, AdapterErr> {
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

pub fn aoe_event_result_to_north(event_result: PbEventResult) -> Result<MyPbEventResult, AdapterErr> {
    Ok(MyPbEventResult{
        id: event_result.id,
        start_time: event_result.start_time,
        end_time: event_result.end_time,
        final_result: event_result.final_result,
    })
}

pub fn dffs_to_south(dffs: MyDffModels, points_mapping: &HashMap<String, u64>, current_id: u64) -> Result<(Vec<DffModel>, HashMap<u64, u64>), AdapterErr> {
    let dffs = replace_point_for_dff(&dffs, points_mapping)?;
    let mut dffs_result = vec![];
    let mut dffs_mapping = HashMap::new();
    let mut current_id = current_id;
    if let Some(dffs) = dffs.dffs {
        for d in dffs {
            current_id = current_id + 1;
            let trigger_type = dfftrigger_type_to_south(d.trigger_type)?;
            let nodes = dffnodes_to_south(current_id, d.nodes)?;
            let actions = dffactions_to_south(current_id, d.actions)?;
            let dff = DffModel {
                id: current_id,
                name: d.name,
                nodes,
                actions,
                trigger_type,
                save_mode: d.save_mode,
                is_on: d.is_on,
                aoe_var: d.aoe_var,
            };
            dffs_result.push(dff);
            dffs_mapping.insert(current_id, d.id);
        }
    }
    Ok((dffs_result, dffs_mapping))
}

fn replace_point_for_dff(dffs: &MyDffModels, points_mapping: &HashMap<String, u64>) -> Result<MyDffModels, AdapterErr> {
    let mut dffs = dffs.clone();
    if let Some(dffs) = dffs.dffs.as_mut() {
        for dff in dffs.iter_mut() {
            for node in dff.nodes.iter_mut() {
                if let MyDfNodeType::Source(MyDfSource::Points(points)) = &node.node_type {
                    node.node_type = MyDfNodeType::Source(MyDfSource::Points(replace_point(points, points_mapping)?));
                }
            }
        }
    }
    Ok(dffs)
}

fn dfftrigger_type_to_south(north: MyDfTriggerType) -> Result<DfTriggerType, AdapterErr> {
    match north {
        MyDfTriggerType::SimpleRepeat(v) => Ok(DfTriggerType::SimpleRepeat(Duration::from_millis(v))),
        MyDfTriggerType::TimeDrive(v) => Ok(DfTriggerType::TimeDrive(v)),
        MyDfTriggerType::EventDrive(v) => {
            if let Ok(expr) = Expr::from_str(v.as_str()) {
                Ok(DfTriggerType::EventDrive(expr))
            } else {
                Err(AdapterErr {
                    code: ErrCode::DffVariableErr,
                    msg: format!("报表事件驱动变量名解析错误：{v}"),
                })
            }
        },
        MyDfTriggerType::DataSource(_) => Ok(DfTriggerType::DataSource),
        MyDfTriggerType::Manual(_) => Ok(DfTriggerType::Manual),
    }
}

fn dffnodes_to_south(dff_id: u64, north: Vec<MyDfNode>) -> Result<Vec<DfNode>, AdapterErr> {
    let mut nodes = vec![];
    for node_n in north {
        let node_type = match node_n.node_type {
            MyDfNodeType::Source(v) => {
                let source = match v {
                    MyDfSource::Data(v) => {
                        if let Ok(value) = serde_json::from_slice::<Value>(&v) {
                            if let Ok(df) = serde_json::from_value::<DataFrame>(value) {
                                DfSource::Data(df)
                            } else {
                                DfSource::Data(DataFrame::empty())
                            }
                        } else {
                            DfSource::Data(DataFrame::empty())
                        }
                    },
                    MyDfSource::File(v) => DfSource::File(v),
                    MyDfSource::Url(v) => DfSource::Url(v),
                    MyDfSource::Image(v) => DfSource::Image(v),
                    MyDfSource::Sql(v1, v2) => DfSource::Sql(v1, v2),
                    MyDfSource::OtherFlow(v) => DfSource::OtherFlow(v),
                    MyDfSource::Dev(v) => DfSource::Dev(v),
                    MyDfSource::Points(v) => DfSource::Points(v),
                    MyDfSource::Meas(v1, v2) => DfSource::Meas(v1, v2),
                    MyDfSource::Plan(v) => DfSource::Plan(v),
                    MyDfSource::PointsEval(v1, v2) => {
                        let mut exprs = vec![];
                        for v in v2 {
                            if let Ok(expr) = Expr::from_str(v.as_str()) {
                                exprs.push(expr);
                            } else {
                                return Err(AdapterErr {
                                    code: ErrCode::DffVariableErr,
                                    msg: format!("报表PointsEval变量名解析错误：{v}"),
                                });
                            }
                        }
                        DfSource::PointsEval(v1, exprs)
                    },
                    MyDfSource::MeasEval(v1, v2, v3) => {
                        let mut exprs = vec![];
                        for v in v3 {
                            if let Ok(expr) = Expr::from_str(v.as_str()) {
                                exprs.push(expr);
                            } else {
                                return Err(AdapterErr {
                                    code: ErrCode::DffVariableErr,
                                    msg: format!("报表MeasEval变量名解析错误：{v}"),
                                });
                            }
                        }
                        DfSource::MeasEval(v1, v2, exprs)
                    },
                };
                DfNodeType::Source(source)
            },
            MyDfNodeType::Transform(v) => {
                if let Ok(expr) = Expr::from_str(v.as_str()) {
                    DfNodeType::Transform(expr)
                } else {
                    return Err(AdapterErr {
                        code: ErrCode::DffVariableErr,
                        msg: format!("报表node变量名解析错误：{v}"),
                    });
                }
            },
            MyDfNodeType::TensorEval(_, _, _, v4) => {
                if let Ok(prog) = load_prog(&v4) {
                    let g = create_stmt_tree(&prog);
                    DfNodeType::TensorEval(g, 0, None)
                } else {
                    return Err(AdapterErr {
                        code: ErrCode::DffVariableErr,
                        msg: format!("报表TensorEval变量名解析错误：{v4}"),
                    });
                }
            },
            MyDfNodeType::Sql(v) => DfNodeType::Sql(v),
            MyDfNodeType::Solve(v) => DfNodeType::Solve(v),
            MyDfNodeType::NLSolve(_) => DfNodeType::NLSolve,
            MyDfNodeType::MILP(v1, v2, v3) => DfNodeType::MILP(v1, v2, v3),
            MyDfNodeType::NLP(v1, v2) => DfNodeType::NLP(v1, v2),
            MyDfNodeType::Wasm(v1, v2) => {
                let mut model_types = vec![];
                for v in v1 {
                    model_types.push(match v {
                        MyModelType::Island(_) => ModelType::Island,
                        MyModelType::Meas(_) => ModelType::Meas,
                        MyModelType::File(items) => ModelType::File(items),
                        MyModelType::Outgoing(items) => ModelType::Outgoing(items),
                    });
                }
                DfNodeType::Wasm(model_types, v2)
            },
            MyDfNodeType::None(_) => DfNodeType::None,
        };
        let node_s = DfNode {
            id: node_n.id,
            flow_id: dff_id,
            name: node_n.name,
            node_type: node_type,
        };
        nodes.push(node_s);
    }
    Ok(nodes)
}

fn dffactions_to_south(dff_id: u64, north: Vec<MyDfActionEdge>) -> Result<Vec<DfActionEdge>, AdapterErr> {
    let mut actions = vec![];
    for action_n in north {
        let action = match action_n.action {
            MyDfAction::Kmeans(_) => DfAction::Kmeans,
            MyDfAction::RandomTree(_) => DfAction::RandomTree,
            MyDfAction::Eval(v1, v2) => {
                let mut exprs = vec![];
                for v in v2 {
                    if let Ok(expr) = Expr::from_str(v.as_str()) {
                        exprs.push(expr);
                    } else {
                        return Err(AdapterErr {
                            code: ErrCode::DffVariableErr,
                            msg: format!("报表动作Eval变量名解析错误：{v}"),
                        });
                    }
                }
                DfAction::Eval(v1, exprs)
            },
            MyDfAction::Sql(v) => DfAction::Sql(v),
            MyDfAction::Onnx(v) => DfAction::Onnx(v),
            MyDfAction::OnnxUrl(v) => DfAction::OnnxUrl(v),
            MyDfAction::Nnef(v) => DfAction::Nnef(v),
            MyDfAction::NnefUrl(v) => DfAction::NnefUrl(v),
            MyDfAction::WriteFile(v) => DfAction::WriteFile(v),
            MyDfAction::WriteSql(v1, v2) => DfAction::WriteSql(v1, v2),
            MyDfAction::None(_) => DfAction::None,
        };
        let action_s = DfActionEdge {
            flow_id: dff_id,
            name: action_n.name,
            desc: action_n.desc,
            source_node: action_n.source_node,
            target_node: action_n.target_node,
            action,
        };
        actions.push(action_s);
    }
    Ok(actions)
}
