use std::collections::HashMap;
use std::time::Duration;
use std::fmt;
use std::fmt::{Display as FmtDisplay, Formatter};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use derive_more::with_trait::{Display, FromStr};
use log::{trace, warn};
use crate::utils::exprparser::*;
use Token::*;
use Operation::*;

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone, Default)]
pub struct MqttTransport {
    pub id: u64,
    /// 通道名称
    pub name: String,
    /// 服务端的ip和por
    pub mqtt_broker: (String, u16),
    /// 通道状态对应的测点号
    pub point_id: u64,
    /// 通过mqtt读写的测点
    pub point_ids: Vec<(u64, bool)>,
    /// 读测点的主题
    pub read_topic: String,
    /// 写测点的主题
    pub write_topic: String,
    /// 编码格式，默认是protobuf
    pub is_json: bool,
    /// 是否转发通道
    pub is_transfer: bool,
    /// 心跳时间
    pub keep_alive: Option<u16>,
    /// 用户名，可选
    pub user_name: Option<String>,
    /// 用户密码，可选
    pub user_password: Option<String>,
    // 总的提取器，有些情况测量数据作为一个数组放在json中
    pub array_filter: Option<String>,
    /// json格式过滤器
    pub filter_keys: Option<Vec<Vec<String>>>,
    pub filter_values: Option<Vec<Option<Vec<Value>>>>,
    /// json测点对应的数据标识, key是过滤器对应Array的json字符串，value是标识以及测点的索引
    pub json_tags: Option<HashMap<String, HashMap<String, usize>>>,
    /// json写测点模板
    pub json_write_template: Option<HashMap<u64, String>>,
    /// json写测点模板
    pub json_write_tag: Option<HashMap<u64, String>>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Measurement {
    /// 唯一的id
    pub point_id: u64,
    /// 测点名
    pub point_name: String,
    /// 字符串id
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
    #[serde(skip)]
    pub unit: DataUnit,
    /// 上限，用于坏数据辨识
    pub upper_limit: f64,
    /// 下限，用于坏数据辨识
    pub lower_limit: f64,
    /// 告警级别1的表达式
    pub alarm_level1_expr: String,
    #[serde(skip)]
    pub alarm_level1: Option<Expr>,
    /// 告警级别2的表达式
    pub alarm_level2_expr: String,
    #[serde(skip)]
    pub alarm_level2: Option<Expr>,
    /// 如是，则不判断是否"变化"，均上传
    pub is_realtime: bool,
    /// 是否是soe点
    pub is_soe: bool,
    /// 默认值存储在8个字节，需要根据is_discrete来转换成具体的值
    pub init_value: u64,
    /// Description
    pub desc: String,
    /// 标识该测点是否是采集点，在运行时根据测点是否属于通道来判断
    #[serde(skip)]
    pub is_remote: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AoeModel {
    /// aoe id
    pub id: u64,
    /// aoe name
    pub name: String,
    /// 节点
    pub events: Vec<EventNode>,
    /// 边
    pub actions: Vec<ActionEdge>,
    /// aoe启动的方式
    pub trigger_type: TriggerType,
    /// 用户自定义的变量，这些变量不在计算点的范围
    pub variables: Vec<(String, Expr)>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct EventNode {
    pub id: u64,
    pub aoe_id: u64,
    pub name: String,
    pub node_type: NodeType,
    pub expr: Expr,
    /// 事件还未发生的等待超时时间
    pub timeout: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct ActionEdge {
    pub aoe_id: u64,
    pub name: String,
    pub source_node: u64,
    pub target_node: u64,
    /// action失败时的处理方式
    pub failure_mode: FailureMode,
    pub action: EigAction,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum EigAction {
    /// 无动作
    None,
    /// 设点动作
    SetPoints(SetPoints),
    /// 设点动作
    SetPointsWithCheck(SetPoints),
    /// 设点动作
    SetPoints2(SetPoints2),
    /// 设点动作
    SetPointsWithCheck2(SetPoints2),
    /// 求方程
    Solve(SparseSolver),
    /// Nlsolve
    Nlsolve(NewtonSolver),
    /// 混合整数线性规划稀疏表示
    Milp(SparseMILP),
    /// 混合整数线性规划稠密表示
    SimpleMilp(MILP),
    /// 非整数线性规划
    Nlp(NLP),
    /// 调用webservice获取EigAction并执行
    Url(String),
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct NLP {
    // 目标函数表达式 min obj
    pub obj_expr: Expr,
    // 变量名称
    pub x_name: Vec<String>,
    // 整数变量在x中的位置
    pub x_lower: Vec<Expr>,
    pub x_upper: Vec<Expr>,
    // 等式约束式 g(x) == b
    pub g: Vec<Expr>,
    // 不等式约束式 g(x) <= b
    pub g_lower: Vec<Expr>,
    // 不等式约束式 g(x) >= b
    pub g_upper: Vec<Expr>,
    // 变量初始值x0
    pub x_init: Vec<Expr>,
    // min: true, max: false
    pub min_or_max: bool,
    // 求解器参数：参数名、参数值
    pub parameters: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SetPoints2 {
    pub discretes: Vec<PointsToExp>,
    pub analogs: Vec<PointsToExp>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct SetPoints {
    pub discrete_id: Vec<String>,
    pub discrete_v: Vec<Expr>,
    pub analog_id: Vec<String>,
    pub analog_v: Vec<Expr>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct MILP {
    // 变量名称
    pub(crate) x_name: Vec<String>,
    // 变量的下界、上界约束：变量位置、约束表达式
    pub x_lower: Vec<(usize, Expr)>,
    pub x_upper: Vec<(usize, Expr)>,
    // 整数变量在x中的位置
    pub binary_int_float: Vec<u8>,
    // Ax >=/<= b
    pub a: Mat,
    pub b: Vec<Expr>,
    pub constraint_type: Vec<Operation>,
    // min/max c^T*x
    pub c: Vec<Expr>,
    // min: true, max: false
    pub min_or_max: bool,
    // 求解器参数：参数名、参数值
    pub parameters: HashMap<String, String>,
}

/// 由表达式组成的矩阵
#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct Mat {
    pub(crate) m: usize,
    pub(crate) n: usize,
    pub(crate) v: Vec<Expr>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct SparseMILP {
    // 变量名称
    pub x_name: Vec<String>,
    // 变量的下界、上界约束：变量位置、约束表达式
    pub x_lower: Vec<(usize, Expr)>,
    pub x_upper: Vec<(usize, Expr)>,
    // 整数变量在x中的位置
    pub binary_int_float: Vec<u8>,
    // Ax >=/<= b
    pub a: SparseMat,
    pub b: Vec<Expr>,
    pub constraint_type: Vec<Operation>,
    // min/max c^T*x
    pub c: Vec<(usize, Expr)>,
    // min: true, max: false
    pub min_or_max: bool,
    // 求解器参数：参数名、参数值
    pub parameters: HashMap<String, String>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct NewtonSolver {
    pub f: Vec<Expr>,
    pub x_name: Vec<String>,
    pub x_init: Vec<Expr>,
    pub x_init_cx: Vec<Expr>,
    pub parameters: HashMap<String, String>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct SparseSolver {
    // Ax = b
    pub a: SparseMat,
    pub b: Vec<Expr>,
    // 变量名称
    pub x_name: Vec<String>,
    // 变量初始值
    pub x_init: Vec<Expr>,
    // 求解器参数：参数名、参数值
    pub parameters: HashMap<String, String>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct SparseMat {
    pub m: usize,
    pub n: usize,
    pub v: Vec<(usize, usize, Expr)>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct PointsToExp {
    pub ids: Vec<String>,
    pub expr: Expr,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum FailureMode {
    // 如果存在指向该节点的动作运行成功(可以理解为有路径到达该事件),则后续动作继续进行
    Default,
    // 忽略，不影响其他action
    Ignore,
    // 停止整个aoe
    StopAll,
    // 只停止受影响的节点
    StopFailed,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum TriggerType {
    // 简单固定周期触发
    SimpleRepeat(Duration),
    // cron expression
    TimeDrive(String),
    // 事件驱动，AOE开始节点条件满足即触发
    EventDrive,
    // 事件驱动 && Simple drive
    EventRepeatMix(Duration),
    // 事件驱动 && Time drive
    EventTimeMix(String),
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum NodeType {
    // 带表达式的节点，表达式结果>0说明事件发生，进入后续事件
    ConditionNode,
    // 带表达式的节点，表达式结果>0进入第一条支路，否则进入第二条支路
    SwitchNode,
    // 不带表达式的节点，前序Action运行成功进入第一条支路，否则进入第二条支路
    SwitchOfActionResult,
}

#[allow(non_camel_case_types)]
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Copy, Clone, Hash, Default, Display)]
#[display("{_variant}")]
pub enum DataUnit {
    /// switch on of off
    #[display("on_or_off")]
    OnOrOff,
    /// 安培
    A,
    /// 伏特
    V,
    /// 千伏
    kV,
    /// 瓦特
    W,
    /// 千瓦
    kW,
    /// 兆瓦
    MW,
    /// Var
    Var,
    /// kVar
    kVar,
    /// MVar
    MVar,
    // VA
    VA,
    // kVA
    kVA,
    // MVA
    MVA,
    /// 亨利
    H,
    /// 毫亨
    mH,
    /// 安时
    Ah,
    /// 毫安时
    mAh,
    /// 千瓦时
    kWh,
    /// 摄氏度
    #[display("°C")]
    Celsius,
    /// feet
    #[display("ft")]
    feet,
    /// kilometer
    km,
    /// meter
    meter,
    /// Square millimeter
    mm2,
    /// 无单位
    UnitOne,
    /// 百分比
    #[display("%")]
    Percent,
    /// 大小单位
    bit,
    /// Byte
    B,
    /// KB
    kB,
    /// MB
    MB,
    /// GB
    GB,
    /// TB
    TB,
    /// PB
    PB,
    /// 其他未知单位
    #[default]
    #[display("")]
    Unknown,
}

impl DataUnit {
    /// 用于遍历所有数据单位列表
    pub const DATA_UNIT: [DataUnit; 33] = [
        DataUnit::OnOrOff,
        DataUnit::A,
        DataUnit::V,
        DataUnit::kV,
        DataUnit::W,
        DataUnit::kW,
        DataUnit::MW,
        DataUnit::Var,
        DataUnit::kVar,
        DataUnit::MVar,
        DataUnit::VA,
        DataUnit::kVA,
        DataUnit::MVA,
        DataUnit::H,
        DataUnit::mH,
        DataUnit::Ah,
        DataUnit::mAh,
        DataUnit::kWh,
        DataUnit::Celsius,
        DataUnit::feet,
        DataUnit::km,
        DataUnit::meter,
        DataUnit::mm2,
        DataUnit::UnitOne,
        DataUnit::Percent,
        DataUnit::bit,
        DataUnit::B,
        DataUnit::kB,
        DataUnit::MB,
        DataUnit::GB,
        DataUnit::TB,
        DataUnit::PB,
        DataUnit::Unknown,
    ];
}


impl FromStr for DataUnit {
    type Err = ();

    fn from_str(s: &str) -> Result<DataUnit, Self::Err> {
        match s.trim().to_uppercase().as_str() {
            "ON_OR_OFF" => Ok(DataUnit::OnOrOff),
            "ONOROFF" => Ok(DataUnit::OnOrOff),
            "ONOFF" => Ok(DataUnit::OnOrOff),
            "ON/OFF" => Ok(DataUnit::OnOrOff),
            "A" => Ok(DataUnit::A),
            "安" => Ok(DataUnit::A),
            "安培" => Ok(DataUnit::A),
            "V" => Ok(DataUnit::V),
            "伏" => Ok(DataUnit::V),
            "伏特" => Ok(DataUnit::kV),
            "KV" => Ok(DataUnit::kV),
            "千伏" => Ok(DataUnit::kV),
            "W" => Ok(DataUnit::W),
            "瓦" => Ok(DataUnit::W),
            "瓦特" => Ok(DataUnit::W),
            "KW" => Ok(DataUnit::kW),
            "千瓦" => Ok(DataUnit::kW),
            "MW" => Ok(DataUnit::MW),
            "兆瓦" => Ok(DataUnit::MW),
            "VA" => Ok(DataUnit::VA),
            "伏安" => Ok(DataUnit::VA),
            "KVA" => Ok(DataUnit::kVA),
            "千伏安" => Ok(DataUnit::kVA),
            "MVA" => Ok(DataUnit::MVA),
            "兆伏安" => Ok(DataUnit::MVA),
            "VAR" => Ok(DataUnit::Var),
            "乏" => Ok(DataUnit::Var),
            "KVAR" => Ok(DataUnit::kVar),
            "千乏" => Ok(DataUnit::kVar),
            "MVAR" => Ok(DataUnit::MVar),
            "兆乏" => Ok(DataUnit::MVar),
            "H" => Ok(DataUnit::H),
            "亨" => Ok(DataUnit::H),
            "亨利" => Ok(DataUnit::H),
            "MH" => Ok(DataUnit::mH),
            "毫亨" => Ok(DataUnit::mH),
            "AH" => Ok(DataUnit::Ah),
            "安时" => Ok(DataUnit::Ah),
            "MAH" => Ok(DataUnit::mAh),
            "毫安时" => Ok(DataUnit::mAh),
            "KWH" => Ok(DataUnit::kWh),
            "千瓦时" => Ok(DataUnit::kWh),
            "度" => Ok(DataUnit::kWh),
            "℃" => Ok(DataUnit::Celsius), //℃ 字符代码 2103 中文摄氏度 一个字符
            "°C" => Ok(DataUnit::Celsius), // °C 英文摄氏度  两个字符 字符代码00B0 +大写字母C
            "CELSIUS" => Ok(DataUnit::Celsius),
            "摄氏度" => Ok(DataUnit::Celsius),
            "FEET" => Ok(DataUnit::feet),
            "KM" => Ok(DataUnit::km),
            "M" => Ok(DataUnit::meter),
            "METER" => Ok(DataUnit::meter),
            "MM2" => Ok(DataUnit::mm2),
            "%" => Ok(DataUnit::Percent),
            "PERCENT" => Ok(DataUnit::Percent),
            "BIT" => Ok(DataUnit::bit),
            "BYTE" => Ok(DataUnit::B),
            "B" => Ok(DataUnit::B),
            "KB" => Ok(DataUnit::kB),
            "MB" => Ok(DataUnit::MB),
            "GB" => Ok(DataUnit::GB),
            "TB" => Ok(DataUnit::TB),
            "PB" => Ok(DataUnit::PB),
            "UNITONE" => Ok(DataUnit::UnitOne),
            _ => Ok(DataUnit::Unknown),
        }
    }
}


#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct Expr {
    pub rpn: Vec<Token>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub enum Token {
    /// Binary operation.
    Binary(Operation),
    /// Unary operation.
    Unary(Operation),

    /// Left parenthesis.   (
    LParen,
    /// Right parenthesis.  )
    RParen,
    /// Big Left parenthesis.  {
    BigLParen,
    /// Big Right parenthesis. }
    BigRParen,
    /// Right brackets. ]
    RBracket,
    /// Comma: function argument separator
    Comma,

    /// A number.
    Number(f64),
    /// A tensor.
    Tensor(Option<usize>),
    /// A variable.
    Var(String),
    /// A function with name and number of arguments.
    Func(String, Option<usize>),
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy)]
pub enum Operation {
    // +
    Plus,
    // -
    Minus,
    // *
    Times,
    // /
    Div,
    // %
    Rem,
    // ^
    Pow,
    // !
    Fact,

    // bool操作符
    // ==
    Equal,
    // !=
    Unequal,
    // <
    LessThan,
    // >
    GreatThan,
    // <=
    LtOrEqual,
    // >=
    GtOrEqual,
    // &&
    And,
    // ||
    Or,
    // ~~
    Not,
    // 下面是位操作
    // &
    BitAnd,
    // |
    BitOr,
    // ^^
    BitXor,
    // <<
    BitShl,
    // >>
    BitShr,
    // @
    BitAt,
    // ~
    BitNot,
}

// extern crate meval;
/// An error produced during parsing or evaluation.
#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    UnknownVariable(String),
    UnknownTensor(u64),
    Function(String, FuncEvalError),
    /// An error returned by the parser.
    ParseError(ParseError),
    /// The shunting-yard algorithm returned an error.
    RPNError(RPNError),
    // A catch all for all other errors during evaluation
    EvalError(String),
    EmptyExpression,
}

// impl FromStr for Expr {
//     type Err = Error;
//     /// Constructs an expression by parsing a string.
//     fn from_str(s: &str) -> Result<Self, Error> {
//         match tokenize(s) {
//             Ok(tokens) => match to_rpn(&tokens) {
//                 Ok(rpn) => Ok(Expr { rpn }),
//                 Err(e) => Err(Error::RPNError(e)),
//             },
//             Err(e) => Err(Error::ParseError(e)),
//         }
//     }
// }

/// An error reported by the parser.
#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    /// A token that is not allowed at the given location (contains the location of the offending
    /// character in the source string).
    UnexpectedToken(usize, usize),
    /// Missing right parentheses at the end of the source string (contains the number of missing
    /// parens).
    MissingRParen(i32),
    /// Missing operator or function argument at the end of the expression.
    MissingArgument,
}

impl FmtDisplay for ParseError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            ParseError::UnexpectedToken(row, col) => write!(f, "Unexpected char at line: {row} column: {col}"),
            ParseError::MissingRParen(i) => write!(f, "Missing {i} right parenthes{}.",
                                                   if i == 1 { "is" } else { "es" }),
            ParseError::MissingArgument => write!(f, "Missing argument at the end of expression."),
        }
    }
}

impl std::error::Error for ParseError {
    fn description(&self) -> &str {
        match *self {
            ParseError::UnexpectedToken(_, _) => "unexpected token",
            ParseError::MissingRParen(_) => "missing right parenthesis",
            ParseError::MissingArgument => "missing argument",
        }
    }
}

/// An error produced by the shunting-yard algorightm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RPNError {
    /// An extra left parenthesis was found.
    MismatchedLParen(usize),
    /// An extra left brackets was found.
    MismatchedLBracket(usize),
    /// An extra right parenthesis was found.
    MismatchedRParen(usize),
    /// An extra right bracket was found.
    MismatchedRBracket(usize),
    /// Comma that is not separating function arguments.
    UnexpectedComma(usize),
    /// Too few operands for some operator.
    NotEnoughOperands(usize),
    /// Too many operands reported.
    TooManyOperands,
}

impl std::error::Error for RPNError {
    fn description(&self) -> &str {
        match *self {
            RPNError::MismatchedLParen(_) => "mismatched left parenthesis",
            RPNError::MismatchedRParen(_) => "mismatched right parenthesis",
            RPNError::MismatchedLBracket(_) => "mismatched left blackets",
            RPNError::MismatchedRBracket(_) => "mismatched right blackets",
            RPNError::UnexpectedComma(_) => "unexpected comma",
            RPNError::NotEnoughOperands(_) => "missing operands",
            RPNError::TooManyOperands => "too many operands left at the end of expression",
        }
    }
}

impl FmtDisplay for RPNError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            RPNError::MismatchedLParen(i) => {
                write!(f, "Mismatched left parenthesis at token {i}.")
            }
            RPNError::MismatchedRParen(i) => {
                write!(f, "Mismatched right parenthesis at token {i}.")
            }
            RPNError::MismatchedLBracket(i) => {
                write!(f, "Mismatched left blackets at token {i}.")
            }
            RPNError::MismatchedRBracket(i) => {
                write!(f, "Mismatched right blackets at token {i}.")
            }
            RPNError::UnexpectedComma(i) => write!(f, "Unexpected comma at token {i}"),
            RPNError::NotEnoughOperands(i) => write!(f, "Missing operands at token {i}"),
            RPNError::TooManyOperands => {
                write!(f, "Too many operands left at the end of expression.")
            }
        }
    }
}

/// Function evaluation error.
#[derive(Debug, Clone, PartialEq)]
pub enum FuncEvalError {
    TooFewArguments,
    TooManyArguments,
    NumberArgs(usize),
    CalculationError(String),
    UnknownFunction,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum Transport {
    Mqtt(MqttTransport),
}

impl Transport {
    pub fn id(&self) -> u64 {
        match self {
            Transport::Mqtt(t) => t.id,
        }
    }

    pub fn name(&self) -> String {
        match self {
            Transport::Mqtt(t) => t.name.clone(),
        }
    }
}

impl SparseSolver {
    /// 解析字符串表示的数学模型
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(expr_str: &[&str]) -> Result<Self, usize> {
        let mut expr_str = expr_str.to_vec();
        expr_str.retain(|expr| !expr.trim().is_empty());

        let s_n = expr_str.len();
        if s_n < 2 {
            warn!("!!Insufficient function num for solve Ax=b, content: {:?}",expr_str);
            return Err(s_n);
        }
        // 首先确定变量名称和类型
        let x_defines: Vec<String> = expr_str
            .pop()
            .unwrap()
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        if s_n != x_defines.len() + 1 {
            warn!("!!Insufficient function num for solve Ax=b, content: {:?}",expr_str);
            return Err(s_n);
        }
        let (x_name, x_init) = create_x_name_init(&x_defines).ok_or(s_n)?;
        let mut x_pos = HashMap::with_capacity(x_name.len());
        for (i, x_name_i) in x_name.iter().enumerate() {
            x_pos.insert(x_name_i.clone(), i);
        }
        let mut b = Vec::with_capacity(x_name.len());
        let mut a = SparseMat {
            m: x_name.len(),
            n: x_name.len(),
            v: Vec::new(),
        };
        for (i, expr_str_i) in expr_str.iter().enumerate() {
            let r = parse_linear_expr_str(expr_str_i, &x_pos).ok_or(i + 1)?;
            if r[0].0 != 0 {
                b.push(Expr::from_vec(vec![Number(0.0)]));
            }
            for (col, mut expr) in r {
                if col == 0 {
                    if expr.rpn.len() == 1 {
                        // 如果是常数参数，直接计算
                        if let Number(f) = expr.rpn[0] {
                            expr.rpn[0] = Number(-f);
                            b.push(expr);
                            continue;
                        }
                    }
                    expr.rpn.push(Unary(Minus));
                    b.push(expr);
                } else {
                    a.v.push((i, col - 1, expr));
                }
            }
        }
        let parameters = HashMap::new();
        Ok(SparseSolver { a, b, x_name, x_init, parameters })
    }
    pub fn from_str_with_parameters(
        expr_str: &[&str],
        parameters_str: &[&str],
    ) -> Result<SparseSolver, (usize, usize)> {
        match SparseSolver::from_str(expr_str) {
            Ok(mut m) => match read_parameters_from_str(parameters_str) {
                Ok(parameters) => {
                    m.parameters = parameters;
                    Ok(m)
                }
                Err(n) => Err((1, n)),
            },
            Err(n) => Err((0, n)),
        }
    }
}

impl NewtonSolver {
    // 解析字符串表示的数学模型
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(expr_str: &[&str]) -> Result<NewtonSolver, usize> {
        let mut expr_str = expr_str.to_vec();
        expr_str.retain(|expr| !expr.trim().is_empty());

        let s_n = expr_str.len();
        if s_n < 2 {
            warn!("!!Insufficient function num for solve f(x)=0, content: {:?}",expr_str);
            return Err(s_n);
        }
        // 首先确定变量名称和类型
        let x_defines: Vec<String> = expr_str
            .pop()
            .unwrap()
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();
        let n = x_defines.len();
        if n != expr_str.len() {
            warn!("!!function num is not equal to x length, content: {:?}", expr_str);
            return Err(s_n);
        }
        // column of x
        let (x_name, x_init) = create_x_name_init(&x_defines).ok_or(s_n)?;
        let mut f = Vec::with_capacity(n);
        for (i, expr_str_i) in expr_str.iter().enumerate() {
            let expr: Expr = expr_str_i.parse().map_err(|_| i + 1)?;
            // 对方程进行校验
            if !expr.check_validity() {
                return Err(i + 1);
            }
            f.push(expr);
        }
        let parameters = HashMap::new();
        Ok(NewtonSolver {
            f,
            x_name,
            x_init,
            x_init_cx: vec![],
            parameters,
        })
    }

    pub fn from_str_with_parameters(
        expr_str: &[&str],
        parameters_str: &[&str],
    ) -> Result<NewtonSolver, (usize, usize)> {
        match NewtonSolver::from_str(expr_str) {
            Ok(mut m) => match read_parameters_from_str(parameters_str) {
                Ok(parameters) => {
                    m.parameters = parameters;
                    Ok(m)
                }
                Err(n) => Err((1, n)),
            },

            Err(n) => Err((0, n)),
        }
    }
}

impl MILP {
    /// 从字符串中获取模型
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(expr_str: &[&str]) -> Result<MILP, usize> {
        let mut expr_str = expr_str.to_vec();
        expr_str.retain(|expr| !expr.trim().is_empty());

        let s_n = expr_str.len();
        if s_n <= 2 {
            return Err(s_n);
        }
        // 首先确定变量名称和类型
        let (x_name, binary_int_float, _, x_upper, x_lower) =
            get_x_info(expr_str.pop().unwrap()).map_err(|_| s_n)?;
        // 处理目标函数
        let obj_exp: Expr = expr_str[0].parse().map_err(|_| 1usize)?;
        let n = obj_exp.len();
        let f = &obj_exp.rpn[n - 1];
        let min_or_max: bool;
        match f {
            Func(name, _) => {
                if name.to_uppercase() == "MIN" {
                    min_or_max = true;
                } else if name.to_uppercase() == "MAX" {
                    min_or_max = false;
                } else {
                    warn!("!!No min or max function found in obj expression: {:?}",obj_exp);
                    return Err(1);
                }
            }
            _ => {
                warn!("!!Not a function, obj expression: {:?}", obj_exp);
                return Err(1);
            }
        }
        let c: Vec<Expr> = get_expr_from_fun(obj_exp.rpn).ok_or(1usize)?;
        let m = expr_str.len() - 1;
        let n = x_name.len();
        let mut v = Vec::new();
        let mut b = Vec::with_capacity(m);
        let mut constraint_type = Vec::with_capacity(m);
        for (i, expr_str_i) in expr_str.iter().enumerate().skip(1) {
            let s = expr_str_i;
            let constraint_expr = s.parse::<Expr>().map_err(|_| i + 1)?;
            let token = &constraint_expr.rpn[constraint_expr.len() - 1];
            if let Binary(op) = token {
                if *op == GtOrEqual || *op == LtOrEqual || *op == Equal {
                    constraint_type.push(*op)
                } else {
                    warn!("!!The {i}th constraint is wrong");
                    return Err(i + 1);
                }
            } else {
                warn!("!!The {i}th constraint is wrong");
                return Err(i + 1);
            }
            let mut left_right = get_expr_from_fun(constraint_expr.rpn).ok_or(i + 1)?;
            b.push(left_right.pop().unwrap());
            let left = get_expr_from_fun(left_right.pop().unwrap().rpn).ok_or(i + 1)?;
            for expr in left {
                v.push(expr);
            }
        }
        let a = Mat { m, n, v };
        let parameters = HashMap::new();
        Ok(MILP {
            x_name,
            x_lower,
            x_upper,
            a,
            b,
            c,
            binary_int_float,
            constraint_type,
            min_or_max,
            parameters,
        })
    }

    pub fn from_str_with_parameters(
        expr_str: &[&str],
        parameters_str: &[&str],
    ) -> Result<MILP, (usize, usize)> {
        match MILP::from_str(expr_str) {
            Ok(mut m) => match read_parameters_from_str(parameters_str) {
                Ok(parameters) => {
                    m.parameters = parameters;
                    Ok(m)
                }
                Err(n) => Err((1, n)),
            },

            Err(n) => Err((0, n)),
        }
    }
}

impl SparseMILP {
    pub fn get_c(mut obj_exp:  Expr, x_pos: &HashMap<String, usize>) -> Result<Vec<(usize, Expr)>, usize> {
        let _ = obj_exp.rpn.pop().ok_or(1usize)?;
        let mut c = split_linear_expr(obj_exp.rpn, x_pos).ok_or(1usize)?;
        if c[0].0 == 0 {
            return Err(1);
        }
        // 下标从0开始，与MILP保持一致
        for c_i in c.iter_mut() {
            c_i.0 -= 1;
        }
        Ok(c)
    }

    pub fn get_min_or_max(obj_exp: &Expr) -> Result<bool, usize>{
        // pop min or max func
        let f = obj_exp.rpn.last().ok_or(1usize)?;
        let min_or_max: bool;
        match f {
            Func(name, _) => {
                trace!("obj func is {name}");
                if name.to_uppercase() == "MIN" {
                    min_or_max = true;
                } else if name.to_uppercase() == "MAX" {
                    min_or_max = false;
                } else {
                    warn!("!!No min or max function found in obj expression: {:?}",obj_exp);
                    return Err(1);
                }
            }
            _ => {
                warn!("!!Not a function, obj expression: {:?}", obj_exp);
                return Err(1);
            }
        }
        Ok(min_or_max)
    }

    pub fn get_constraints(constraints: Vec<Expr>, x_pos: &HashMap<String, usize>) 
        -> Result<(Vec<Operation>, Vec<(usize, usize, Expr)>, Vec<Expr>), usize> {
        let m = constraints.len();       
        let mut constraint_type = Vec::with_capacity(m);
        let mut v = Vec::new();
        let mut b = Vec::with_capacity(m);
        for (i, constraint_expr) in constraints.into_iter().enumerate() {
            let token = &constraint_expr.rpn[constraint_expr.len() - 1];
            if let Binary(op) = token {
                if *op == GtOrEqual || *op == LtOrEqual || *op == Equal {
                    constraint_type.push(*op)
                } else {
                    warn!("!!The {}th constraint is wrong", i+1);
                    return Err(i + 2);
                }
            } else {
                warn!("!!The {}th constraint is wrong", i+1);
                return Err(i + 2);
            }
            let mut left_right = get_expr_from_fun(constraint_expr.rpn).ok_or(i + 2)?;
            if left_right.len() != 2 {
                return Err(i + 2);
            }
            let right = left_right.pop().unwrap();
            let left = left_right.pop().unwrap();
            let mut right = parse_linear_expr(right.rpn, x_pos).ok_or(i + 2)?;
            let left = parse_linear_expr(left.rpn, x_pos).ok_or(i + 2)?;
            if merge_expr_map(left, &mut right, Minus) {
                let r = create_linear_expr(right).ok_or(i + 2)?;
                if r[0].0 != 0 {
                    b.push(Expr::from_vec(vec![Number(0.0)]));
                }
                for (col, mut expr) in r {
                    if col == 0 {
                        if expr.rpn.len() == 1 {
                            // 如果是常数参数，直接计算
                            if let Number(f) = expr.rpn[0] {
                                expr.rpn[0] = Number(-f);
                                b.push(expr);
                                continue;
                            }
                        }
                        expr.rpn.push(Unary(Minus));
                        // check expression
                        if !expr.check_validity() {
                            return Err(i + 2);
                        }
                        b.push(expr);
                    } else {
                        // check expression
                        if !expr.check_validity() {
                            return Err(i + 2);
                        }
                        v.push((i, col - 1, expr));
                    }
                }
            }
        }
        Ok((constraint_type, v, b))
    }

    /// 从字符串中获取模型
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(expr_str: &[&str]) -> Result<SparseMILP, usize> {
        let mut expr_str = expr_str.to_vec();
        expr_str.retain(|expr| !expr.trim().is_empty());

        let s_n = expr_str.len();
        if s_n <= 2 {
            return Err(s_n);
        }
        // 首先确定变量名称和类型
        let (x_name, binary_int_float, x_pos, x_upper, x_lower) =
            get_x_info(expr_str.pop().unwrap()).map_err(|_| s_n)?;
        // 处理目标函数
        let obj_exp: Expr = expr_str[0].parse().map_err(|_| 1usize)?;
        // pop min or max func
        let min_or_max= Self::get_min_or_max(&obj_exp)?;
        let c = Self::get_c(obj_exp, &x_pos)?;

        let m = expr_str.len() - 1;
        let n = x_name.len();
        let mut constraints = Vec::with_capacity(m);
        for (i, expr_str_i) in expr_str.iter().enumerate().skip(1) {
            let s = expr_str_i;
            let constraint_expr = s.parse::<Expr>().map_err(|_| i + 1)?;
            constraints.push(constraint_expr);
        }
        let (constraint_type, v, b) = Self::get_constraints(constraints, &x_pos)?;
        let a = SparseMat { m, n, v };
        let parameters = HashMap::new();
        Ok(SparseMILP {
            x_name,
            x_lower,
            x_upper,
            binary_int_float,
            a,
            b,
            constraint_type,
            c,
            min_or_max,
            parameters,
        })
    }

    pub fn from_str_with_parameters(
        expr_str: &[&str],
        parameters_str: &[&str],
    ) -> Result<SparseMILP, (usize, usize)> {
        match SparseMILP::from_str(expr_str) {
            Ok(mut m) => match read_parameters_from_str(parameters_str) {
                Ok(parameters) => {
                    m.parameters = parameters;
                    Ok(m)
                }
                Err(n) => Err((1, n)),
            },

            Err(n) => Err((0, n)),
        }
    }
}

impl NLP {
    /// 从字符串中获取模型
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(expr_str: &[&str]) -> Result<NLP, usize> {
        let mut expr_str = expr_str.to_vec();
        expr_str.retain(|expr| !expr.trim().is_empty());
        let n = expr_str.len();
        if n < 2 {
            return Err(n);
        }
        // 首先确定变量名称和类型
        let x: Vec<&str> = expr_str.pop().unwrap().split(',').collect();
        let mut x_name = Vec::with_capacity(x.len());
        let mut x_init: Vec<Expr> = Vec::with_capacity(x.len());
        let mut x_lower: Vec<Expr> = Vec::with_capacity(x.len());
        let mut x_upper: Vec<Expr> = Vec::with_capacity(x.len());
        for x_s in x {
            let name_and_limit_and_init: Vec<&str> = x_s.split(':').collect();
            if name_and_limit_and_init.len() != 2 {
                return Err(n);
            }
            x_name.push(name_and_limit_and_init[0].trim().to_string());
            let limits_and_init = name_and_limit_and_init[1].trim();
            if limits_and_init.len() < 3 {
                return Err(n);
            }
            let tmp: Vec<&str> = limits_and_init[1..limits_and_init.len() - 1]
                .split('/')
                .collect();
            if (tmp.len() != 2) && (tmp.len() != 3) {
                return Err(n);
            }
            if tmp[0].trim().is_empty() {
                x_lower.push(Expr::from_vec(vec![Number(f64::MIN)]));
            } else if let Ok(l) = tmp[0].parse() {
                x_lower.push(l);
            }
            if tmp[1].trim().is_empty() {
                x_upper.push(Expr::from_vec(vec![Number(f64::MAX)]));
            } else if let Ok(l) = tmp[1].parse() {
                x_upper.push(l);
            }
            if tmp.len() == 3 && !tmp[2].trim().is_empty() {
                if let Ok(l) = tmp[2].parse() {
                    x_init.push(l);
                }
            } else {
                x_init.push(Expr::new());
            }
        }
        // 处理目标函数
        let mut obj_expr: Expr = expr_str[0].parse().map_err(|_| n)?;
        if !obj_expr.check_validity() {
            return Err(n);
        }
        // pop min or max func
        let f = obj_expr.rpn.pop().ok_or(1usize)?;
        let mut min_or_max = true;
        match &f {
            Func(name, _) => {
                if name.to_uppercase() == "MIN" {
                    min_or_max = true;
                } else if name.to_uppercase() == "MAX" {
                    min_or_max = false;
                } else {
                    obj_expr.rpn.push(f);
                }
            }
            _ => {
                obj_expr.rpn.push(f);
            }
        }
        let m = expr_str.len() - 1;
        let mut g = Vec::with_capacity(m);
        let mut g_lower = Vec::with_capacity(m);
        let mut g_upper = Vec::with_capacity(m);
        for (i, expr_str_i) in expr_str.iter().enumerate().skip(1) {
            let g_s = expr_str_i;
            let g_and_limit: Vec<&str> = g_s.split(':').collect();
            if g_and_limit.len() != 2 {
                return Err(i + 1);
            }
            let constraint_expr = g_and_limit[0].trim().parse::<Expr>().map_err(|_| i + 1)?;
            if !constraint_expr.check_validity() {
                return Err(i + 1);
            }
            g.push(constraint_expr);
            // find limit of constraints
            let limits = g_and_limit[1].trim();
            let tmp: Vec<&str> = limits[1..limits.len() - 1].split('/').collect();
            if tmp.len() != 2 {
                return Err(i + 1);
            }
            if tmp[0].trim().is_empty() {
                g_lower.push(Expr::from_vec(vec![Number(f64::MIN)]));
            } else if let Ok(l) = tmp[0].parse() {
                g_lower.push(l);
            }
            if tmp[1].trim().is_empty() {
                g_upper.push(Expr::from_vec(vec![Number(f64::MAX)]));
            } else if let Ok(l) = tmp[1].parse() {
                g_upper.push(l);
            }
        }
        let parameters = HashMap::new();
        Ok(NLP {
            x_name,
            x_lower,
            x_upper,
            g,
            g_upper,
            g_lower,
            obj_expr,
            x_init,
            min_or_max,
            parameters,
        })
    }

    /// 从字符串中获取模型及求解参数
    pub fn from_str_with_parameters(
        expr_str: &[&str],
        parameters_str: &[&str],
    ) -> Result<NLP, (usize, usize)> {
        match NLP::from_str(expr_str) {
            Ok(mut m) => match read_parameters_from_str(parameters_str) {
                Ok(parameters) => {
                    m.parameters = parameters;
                    Ok(m)
                }
                Err(n) => Err((1, n)),
            },

            Err(n) => Err((0, n)),
        }
    }
}

use ndarray::{Array, Array2, IxDyn};
use num_complex::Complex64;

#[derive(Debug, Clone, PartialEq)]
pub enum MyF {
    F64(f64),
    Tensor(Array<f64, IxDyn>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum MyCx {
    F64(Complex64),
    Tensor(Array<Complex64, IxDyn>),
}
pub trait ContextProvider {
    fn get_var(&self, _: &str) -> Option<f64> {
        None
    }
    fn get_var_cx(&self, _: &str) -> Option<Complex64> {
        None
    }
    fn get_tensor(&self, _: &str) -> Option<Array<f64, IxDyn>> {
        None
    }
    fn get_tensor_cx(&self, _: &str) -> Option<Array<Complex64, IxDyn>> {
        None
    }
    fn eval_func(&self, _: &str, _: &[f64]) -> Result<f64, FuncEvalError> {
        Err(FuncEvalError::UnknownFunction)
    }
    fn eval_func_cx(&self, _: &str, _: &[Complex64]) -> Result<Complex64, FuncEvalError> {
        Err(FuncEvalError::UnknownFunction)
    }
    fn eval_func_tensor(&self, _: &str, _: &[MyF]) -> Result<MyF, FuncEvalError> {
        Err(FuncEvalError::UnknownFunction)
    }
    fn eval_func_tensor_cx(&self, _: &str, _: &[MyCx]) -> Result<MyCx, FuncEvalError> {
        Err(FuncEvalError::UnknownFunction)
    }
    fn matrix_inv(&self, _: &Array2<f64>) -> Result<Array2<f64>, FuncEvalError> {
        Err(FuncEvalError::UnknownFunction)
    }
    fn matrix_inv_cx(&self, _: &Array2<Complex64>) -> Result<Array2<Complex64>, FuncEvalError> {
        Err(FuncEvalError::UnknownFunction)
    }
}

pub fn factorial(num: f64) -> Result<f64, &'static str> {
    if num.fract() != 0. || num < 0. {
        Err("Number must be non-negative with no fractional component!")
    } else if num > 170. {
        Ok(f64::INFINITY)
    } else {
        Ok(factorial_unsafe(num))
    }
}

fn factorial_unsafe(num: f64) -> f64 {
    if num == 0. || num == 1. {
        1.
    } else {
        num * factorial_unsafe(num - 1.)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Default, Debug)]
pub struct PbAoeResults {
    pub results: Vec<PbAoeResult>,
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Default, Debug)]
pub struct PbAoeResult {
    pub aoe_id: Option<u64>,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub event_results: Vec<PbEventResult>,
    pub action_results: Vec<PbActionResult>,
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Default, Debug)]
pub struct PbEventResult {
    pub id: Option<u64>,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub final_result: Option<EventEvalResult>,
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub enum EventEvalResult {
    Happen,
    NotHappen,
    Canceled,
    Error,
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct PbActionResult {
    pub source_id: Option<u64>,
    pub target_id: Option<u64>,
    pub start_time: Option<u64>,
    pub end_time: Option<u64>,
    pub final_result: Option<ActionExeResult>,
    pub fail_code: Option<u32>,
    pub yk_points: Vec<u64>,
    pub yk_values: Vec<i64>,
    pub yt_points: Vec<u64>,
    pub yt_values: Vec<f64>,
    pub variables: Vec<String>,
    pub var_values: Vec<f64>,
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub enum ActionExeResult {
    NotRun,
    Success,
    Failed,
}

/**
 * @api {AoeAction} /AoeAction AoeAction
 * @apiGroup A_Enum
 * @apiSuccess {Object} StartAoe 开始AOE，{"StartAoe": u64}
 * @apiSuccess {Object} StopAoe 停止AOE，{"StopAoe": u64}
 * @apiSuccess {Object} UpdateAoe 更新AOE，{"UpdateAoe": AoeModel}
 * @apiSuccess {Object} UpdateAoeCsv 更新AOE（字节数组），{"UpdateAoeCsv": u8[]}
 */
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum AoeAction {
    StartAoe(u64),
    StopAoe(u64),
    UpdateAoe(AoeModel),
    UpdateAoeCsv(Vec<u8>),
}

/**
 * @api {AoeControl} /AoeControl AoeControl
 * @apiGroup A_Object
 * @apiSuccess {AoeAction[]} AoeActions AOE指令列表
 */
#[allow(non_snake_case)]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AoeControl {
    pub AoeActions: Vec<AoeAction>,
}
