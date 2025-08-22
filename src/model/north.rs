use std::collections::HashMap;
use derive_more::Display;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};

use crate::{AdapterErr, ErrCode};

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct MyTransports {
    pub transports: Option<Vec<MyTransport>>,
    pub add: Option<Vec<MyTransport>>,
    pub edit: Option<Vec<MyTransport>>,
    pub delete: Option<Vec<String>>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct MyMqttTransport {
    pub dev_id: String,
    /// 通道名称
    pub name: String,
    /// 遥测/遥信的测点
    pub point_ycyx_ids: Vec<String>,
    /// 遥调的测点
    pub point_yt_ids: Vec<String>,
    /// 遥控的测点
    pub point_yk_ids: Vec<String>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct MyMqttTransportJoin {
    /// 设备id及对应的测点
    pub dev_ids_map: HashMap<String, (Vec<String>, Vec<String>, Vec<String>)>,
    /// 通道名称
    pub name: String,
}

impl MyMqttTransportJoin {
    pub fn from_vec(transports: Option<Vec<MyTransport>>) -> Result<Self, AdapterErr> {
        if let Some(transports) = transports {
            if transports.is_empty() {
                return Err(AdapterErr {
                    code: ErrCode::TransportIsEmpty,
                    msg: "通道列表不能为空".to_string(),
                });
            }
            let mut name = "".to_string();
            let mut dev_ids_map = HashMap::new();
            for transport in transports {
                match transport {
                    MyTransport::Mqtt(mqtt_transport) => {
                        if name.is_empty() {
                            name = mqtt_transport.name.clone();
                        }
                        dev_ids_map.insert(
                            mqtt_transport.dev_id,
                            (
                                mqtt_transport.point_ycyx_ids,
                                mqtt_transport.point_yt_ids,
                                mqtt_transport.point_yk_ids,
                            ),
                        );
                    },
                }
            }
            Ok(Self {
                dev_ids_map,
                name,
            })
        } else {
            return Err(AdapterErr {
                code: ErrCode::TransportIsEmpty,
                msg: "通道列表不能为空".to_string(),
            });
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum MyTransport {
    Mqtt(MyMqttTransport),
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct MyPoints {
    pub points: Option<Vec<MyMeasurement>>,
    pub add: Option<Vec<MyMeasurement>>,
    pub edit: Option<Vec<MyMeasurement>>,
    pub delete: Option<Vec<String>>,
}

#[serde_as]
#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct MyMeasurement {
    /// 唯一的id
    pub point_id: String,
    /// 测点名
    pub point_name: String,
    /// 别名
    pub alias_id: String,
    /// 是否是离散量
    pub is_discrete: bool,
    /// 是否是计算点
    pub is_computing_point: bool,
    /// 如果是计算点，这是表达式
    pub expression: String,
    /// 变换公式
    pub trans_expr: String,
    /// 逆变换公式
    pub inv_trans_expr: String,
    /// 判断是否"变化"的公式，用于变化上传或储存
    pub change_expr: String,
    /// 判断是否为0值的公式
    pub zero_expr: String,
    /// 单位
    pub data_unit: String,
    /// 上限，用于坏数据辨识
    pub upper_limit: Option<f64>,
    /// 下限，用于坏数据辨识
    pub lower_limit: Option<f64>,
    /// 告警级别1的表达式
    pub alarm_level1_expr: String,
    /// 告警级别2的表达式
    pub alarm_level2_expr: String,
    /// 如是，则不判断是否"变化"，均上传
    pub is_realtime: bool,
    /// 是否是soe点
    pub is_soe: bool,
    /// 默认值存储在8个字节，需要根据is_discrete来转换成具体的值
    #[serde_as(as = "DisplayFromStr")]
    pub init_value: u64,
    /// 测点描述
    pub desc: String,
    pub param: Option<PointParam>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct PointParam {
    pub action: Option<String>,
    pub timeout: Option<String>,
    #[serde(rename = "type")]
    pub mtype: Option<String>,
    pub mode: Option<String>,
}

#[serde_as]
#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct MyAoes {
    pub aoes: Option<Vec<MyAoe>>,
    pub add: Option<Vec<MyAoe>>,
    pub edit: Option<Vec<MyAoe>>,
    #[serde_as(as = "Option<Vec<DisplayFromStr>>")]
    pub delete: Option<Vec<u64>>,
}

#[serde_as]
#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct MyAoe {
    /// aoe id
    #[serde_as(as = "DisplayFromStr")]
    pub id: u64,
    /// aoe name
    pub name: String,
    /// 节点
    pub events: Vec<MyEventNode>,
    /// 边
    pub actions: Vec<MyActionEdge>,
    /// aoe启动的方式
    pub trigger_type: MyTriggerType,
    /// 用户自定义的变量，这些变量不在计算点的范围
    pub variables: Vec<(String, String)>,
}

#[serde_as]
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct MyEventNode {
    #[serde_as(as = "DisplayFromStr")]
    pub id: u64,
    pub name: String,
    pub node_type: crate::model::south::NodeType,
    pub expr: String,
    /// 事件还未发生的等待超时时间
    #[serde_as(as = "DisplayFromStr")]
    pub timeout: u64,
}

#[serde_as]
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct MyActionEdge {
    pub name: String,
    #[serde_as(as = "DisplayFromStr")]
    pub source_node: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub target_node: u64,
    /// action失败时的处理方式
    pub failure_mode: crate::model::south::FailureMode,
    pub action: MyEigAction,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum MyEigAction {
    /// 无动作
    None(String),
    /// 设点动作
    SetPoints(MySetPoints),
    /// 设点动作
    SetPointsWithCheck(MySetPoints),
    /// 设点动作
    SetPoints2(MySetPoints),
    /// 设点动作
    SetPointsWithCheck2(MySetPoints),
    /// 求方程
    Solve(MySolver),
    /// Nlsolve
    Nlsolve(MySolver),
    /// 混合整数线性规划稀疏表示
    Milp(MyProgramming),
    /// 混合整数线性规划稠密表示
    SimpleMilp(MyProgramming),
    /// 非整数线性规划
    Nlp(MyProgramming),
    /// 调用webservice获取EigAction并执行
    Url(String),
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct MyProgramming {
    // Ax = b
    pub f: String,
    pub x: String,
    pub constraint: String,
    // 求解器参数：参数名、参数值
    pub parameters: HashMap<String, String>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct MySolver {
    // Ax = b
    pub f: String,
    pub x: String,
    // 求解器参数：参数名、参数值
    pub parameters: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct MySetPoints {
    pub discretes: HashMap<String, String>,
    pub analogs: HashMap<String, String>,
}

#[serde_as]
#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Display)]
pub enum MyTriggerType {
    // 简单固定周期触发
    SimpleRepeat(#[serde_as(as = "DisplayFromStr")] u64),
    // cron expression
    TimeDrive(String),
    // 事件驱动，AOE开始节点条件满足即触发
    EventDrive(String),
    // 事件驱动 && Simple drive
    EventRepeatMix(#[serde_as(as = "DisplayFromStr")] u64),
    // 事件驱动 && Time drive
    EventTimeMix(String),
}

impl MyTransport {
    pub fn dev_id(&self) -> String {
        match self {
            MyTransport::Mqtt(t) => t.dev_id.clone(),
        }
    }
    pub fn name(&self) -> String {
        match self {
            MyTransport::Mqtt(t) => t.name.clone(),
        }
    }
    pub fn point_ycyx_ids(&self) -> Vec<String> {
        match self {
            MyTransport::Mqtt(t) => t.point_ycyx_ids.clone(),
        }
    }
    pub fn point_yt_ids(&self) -> Vec<String> {
        match self {
            MyTransport::Mqtt(t) => t.point_yt_ids.clone(),
        }
    }
    pub fn point_yk_ids(&self) -> Vec<String> {
        match self {
            MyTransport::Mqtt(t) => t.point_yk_ids.clone(),
        }
    }
}

#[serde_as]
#[derive(Serialize, Deserialize, PartialEq, Clone, Default, Debug)]
pub struct MyPbAoeResult {
    pub aoe_id: Option<String>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub start_time: Option<u64>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub end_time: Option<u64>,
    pub event_results: Vec<MyPbEventResult>,
    pub action_results: Vec<MyPbActionResult>,
}

#[serde_as]
#[derive(Serialize, Deserialize, PartialEq, Clone, Default, Debug)]
pub struct MyPbEventResult {
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub id: Option<u64>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub start_time: Option<u64>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub end_time: Option<u64>,
    pub final_result: Option<crate::model::south::EventEvalResult>,
}

#[serde_as]
#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct MyPbActionResult {
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub source_id: Option<u64>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub target_id: Option<u64>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub start_time: Option<u64>,
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub end_time: Option<u64>,
    pub final_result: Option<crate::model::south::ActionExeResult>,
    pub fail_code: Option<u32>,
    pub yk_points: Vec<String>,
    #[serde_as(as = "Vec<DisplayFromStr>")]
    pub yk_values: Vec<i64>,
    pub yt_points: Vec<String>,
    pub yt_values: Vec<f64>,
    pub variables: Vec<String>,
    pub var_values: Vec<f64>,
}

#[test]
fn test_aoe_parse() {
    let item = MyAoes {
        aoes: Some(vec![MyAoe {
            id: 1,
            name: "a".to_string(),
            events: vec![MyEventNode {
                id: 2,
                name: "b".to_string(),
                node_type: crate::model::south::NodeType::ConditionNode,
                expr: "c".to_string(),
                timeout: 3,
            }],
            actions: vec![MyActionEdge {
                name: "d".to_string(),
                source_node: 4,
                target_node: 5,
                failure_mode: crate::model::south::FailureMode::Default,
                action: MyEigAction::None("e".to_string()),
            }],
            trigger_type: MyTriggerType::SimpleRepeat(6),
            variables: vec![("f".to_string(), "g".to_string())],
        }]),
        add: None,
        edit: None,
        delete: Some(vec![1]),
    };
    let to_str = serde_json::to_string(&item).unwrap();
    println!("to_str: {}", to_str);
    match serde_json::from_slice::<MyAoes>(to_str.as_bytes()) {
        Ok(msg) => println!("from_str: {:?}", msg),
        Err(e) => println!("err: {:?}", e),
    }
    // let to_str = to_str.replace("\"1\"", "1").replace("\"2\"", "2").replace("\"3\"", "3");
    println!("to_str2: {}", to_str);
    match serde_json::from_slice::<MyAoes>(to_str.as_bytes()) {
        Ok(msg) => println!("from_str2: {:?}", msg),
        Err(e) => println!("err2: {:?}", e),
    }
}

#[test]
fn test_aoe_result_parse() {
    let item = MyPbAoeResult {
        aoe_id: Some("a".to_string()),
        start_time: Some(2),
        end_time: Some(3),
        event_results: vec![MyPbEventResult {
            id: Some(4),
            start_time: Some(5),
            end_time: Some(6),
            final_result: Some(crate::model::south::EventEvalResult::Happen),
        }],
        action_results: vec![MyPbActionResult {
            source_id: Some(7),
            target_id: Some(8),
            start_time: Some(9),
            end_time: Some(10),
            final_result: Some(crate::model::south::ActionExeResult::NotRun),
            fail_code: Some(11),
            yk_points: vec!["b".to_string()],
            yk_values: vec![-1],
            yt_points: vec!["c".to_string()],
            yt_values: vec![1.1],
            variables: vec!["d".to_string()],
            var_values: vec![2.2],
        }],
    };
    let to_str = serde_json::to_string(&item).unwrap();
    println!("to_str: {}", to_str);
    match serde_json::from_slice::<MyAoes>(to_str.as_bytes()) {
        Ok(msg) => println!("from_str: {:?}", msg),
        Err(e) => println!("err: {:?}", e),
    }
    // let to_str = to_str.replace("\"1\"", "1").replace("\"2\"", "2").replace("\"3\"", "3");
    println!("to_str2: {}", to_str);
    match serde_json::from_slice::<MyAoes>(to_str.as_bytes()) {
        Ok(msg) => println!("from_str2: {:?}", msg),
        Err(e) => println!("err2: {:?}", e),
    }
}
