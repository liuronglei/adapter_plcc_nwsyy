use std::str::FromStr;

use base64::{engine, Engine};
use petgraph::graph::DiGraph;
use polars_core::frame::DataFrame;
use std::fmt;
use std::fmt::{Display, Formatter};

use crate::model::south::{DfAction, DfActionEdge, DffModel, DfNode, DfNodeType, DfSaveMode, DfSource,
                       DfTriggerType, EvalType, ImageDfFilter, Expr, StmtNode, ModelType, Token,
                       Operation, DfSqlType};

impl Display for DfSqlType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            DfSqlType::MySql(v) => write!(f, "{v}"),
            DfSqlType::Sqlite(v) => write!(f, "{v}"),
            DfSqlType::Postgres(v) => write!(f, "{v}"),
            DfSqlType::Unknown(v) => write!(f, "{v}"),
        }
    }
}

impl Display for EvalType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl DfSource {
    pub fn to_json(&self) -> String {
        let mut json = "".to_string();
        match self {
            DfSource::Data(d) => {
                if let Ok(s) = serde_json::to_string(d){
                    json += &format!("\"Data\":{s}");
                } else {
                    json += "\"Data\":\"\"";
                }
            }
            DfSource::File(s) => {
                json += &format!("\"File\":{s:?}");
            }
            DfSource::Url(s) => {
                json += &format!("\"Url\":{s:?}");
            }
            DfSource::Image(s) => {
                if let Ok(image_str) = serde_json::to_string(s) {
                    json += &format!("\"Image\":{image_str}");
                } else {
                    json += "\"Image\":\"\"";
                }
            }
            DfSource::Sql(sql_type, s) => {
                json += &format!("\"Sql\":[{:?}, {s:?}]", sql_type.to_string());
            }
            DfSource::OtherFlow(id) => {
                json += &format!("\"OtherFlow\":{id}");
            }
            DfSource::Dev(s) => {
                json += &format!("\"Dev\":{s:?}");
            }
            DfSource::Points(s) => {
                json += &format!("\"Points\":{s:?}");
            }
            DfSource::Meas(s1, s2) => {
                json += &format!("\"Meas\":[{s1:?}, {s2:?}]");
            }
            DfSource::Plan(s) => {
                json += &format!("\"Plan\":{s:?}");
            }
            DfSource::PointsEval(etype, exprs) => {
                json += "\"PointsEval\":{";
                let mut expr_json = "[".to_string();
                for expr in exprs {
                    if let Ok(s) = rpn_to_string(&expr.rpn) {
                        expr_json += &format!("{s:?},");
                    }
                }
                expr_json = expr_json.trim_end_matches(',').to_string();
                expr_json += "]";
                json += &format!("{:?}:{expr_json}", etype.to_string());
                json += "}";
            }
            DfSource::MeasEval(s,etype, exprs) => {
                json += "\"MeasEval\":{";
                json += &format!("\"Meas\":{s:?},");
                let mut expr_json = "[".to_string();
                for expr in exprs {
                    if let Ok(s) = rpn_to_string(&expr.rpn) {
                        expr_json += &format!("{s:?},");
                    }
                }
                expr_json = expr_json.trim_end_matches(',').to_string();
                expr_json += "]";
                json += "\"EvalType\":{";
                json += &format!("{:?}:{expr_json}", etype.to_string());
                json += "}}";
            }
        }

        json
    }
}

impl DfNodeType {
    pub fn to_json(&self) -> String {
        let mut json = "".to_string();
        match self {
            DfNodeType::Source(s) => {
                json += &format!("\"Source\":{{{}}}", s.to_json());
            }
            DfNodeType::Transform(expr) => {
                if let Ok(s) = rpn_to_string(&expr.rpn) {
                    json += &format!("\"Transform\":{s:?}");
                } else {
                    json += "\"Transform\":\"\"";
                }
            }
            DfNodeType::TensorEval(g, b, names) => {
                json += &format!("\"TensorEval\":{{\"para\": {b}, \"exprs\": {}", serde_json::to_string(g).unwrap());
                if let Some(n) = names {
                    if let Ok(json_str) = serde_json::to_string(n) {
                        json += &format!(",\"names\":{json_str}");
                    }
                }
                json += "}";
            }
            DfNodeType::Sql(s) => {
                json += &format!("\"Sql\":{s:?}");
            }
            DfNodeType::Solve(b) => {
                json += &format!("\"Solve\":{b}");
            }
            DfNodeType::NLSolve => {
                json += "\"NLSolve\":\"\"";
            }
            DfNodeType::MILP(s1, s2, b) => {
                json += "\"MILP\":{";
                json += &format!("\"obj_df_name\":{s1:?},");
                json += &format!("\"constraints_df_name\":{s2:?},");
                json += &format!("\"is_sparse\":{b}");
                json += "}";
            }
            DfNodeType::NLP(s1, s2) => {
                json += "\"NLP\":{";
                json += &format!("\"x_df_name\":{s1:?},");
                json += &format!("\"constraints_df_name\":{s2:?}");
                json += "}";
            }
            DfNodeType::Wasm(input, vec) => {
                let data = engine::general_purpose::STANDARD.encode(vec);
                json += "\"Wasm\":{";
                if let Ok(s) = serde_json::to_string(input) {
                    json += &format!("\"model_type\":{s},");
                } else {
                    json += "\"model_type\":\"\",";
                }
                json += &format!("\"data\":{data:?}");
                json += "}";
            }
            DfNodeType::None => {
                json += "\"None\":\"\"";
            }
        }
        json
    }
}

impl DfAction {
    pub fn to_json(&self) -> String {
        let mut json = "".to_string();
        match self {
            DfAction::Kmeans => {
                json += "\"Kmeans\":\"\"";
            }
            DfAction::RandomTree => {
                json += "\"RandomTree\":\"\"";
            }
            DfAction::Eval(etype, exprs) => {
                json += "\"Eval\":{";
                let mut expr_json = "[".to_string();
                for expr in exprs {
                    if let Ok(s) = rpn_to_string(&expr.rpn) {
                        expr_json += &format!("{s:?},");
                    }
                }
                expr_json = expr_json.trim_end_matches(',').to_string();
                expr_json += "]";
                json += &format!("{:?}:{expr_json}", etype.to_string());
                json += "}";
            }
            DfAction::Sql(s) => {
                json += &format!("\"Sql\":{s:?}");
            }
            DfAction::Onnx(vec) => {
                let data = engine::general_purpose::STANDARD.encode(vec);
                json += &format!("\"Onnx\":{:?}", data);
            }
            DfAction::OnnxUrl(s) => {
                json += &format!("\"OnnxUrl\":{s:?}");
            }
            DfAction::Nnef(vec) => {
                let data = engine::general_purpose::STANDARD.encode(vec);
                json += &format!("\"Nnef\":{data:?}");
            }
            DfAction::NnefUrl(s) => {
                json += &format!("\"NnefUrl\":{s:?}");
            }
            DfAction::None => {
                json += "\"None\":\"\"";
            }
            DfAction::WriteFile(s) => {
                json += &format!("\"WriteFile\":{s:?}");
            }
            DfAction::WriteSql(sql_type, s) => {
                json += &format!("\"WriteSql\":[{:?}, {s:?}]", sql_type.to_string());
            }
        }
        json
    }
}

impl DffModel {
    pub fn to_json(&self) -> String {
        let mut json = "{".to_string();
        json += &format!("\"id\":{},", self.id);
        json += &format!("\"is_on\":{},", self.is_on);
        json += &format!("\"name\":{:?},", self.name);
        json += &format!(
            "\"trigger_type\":{},",
            serde_json::to_string(&self.trigger_type).unwrap()
        );

        json += "\"nodes\":[";
        for n in self.nodes.iter() {
            let mut n_json = "{".to_string();
            n_json += &format!("\"id\":{},", n.id);
            n_json += &format!("\"flow_id\":{},", n.flow_id);
            n_json += &format!("\"name\":{:?},", n.name);
            n_json += &format!("\"node_type\":{{{}}}", n.node_type.to_json());
            json += &n_json;
            json += "},";
        }
        json = json.trim_end_matches(',').to_string();
        json += "],";

        json += "\"actions\":[";
        for a in self.actions.iter() {
            let mut a_json = "{".to_string();
            a_json += &format!("\"flow_id\":{},", a.flow_id);
            a_json += &format!("\"name\":{:?},", a.name);
            a_json += &format!("\"desc\":{:?},", a.desc);
            a_json += &format!("\"source_node\":{},", a.source_node);
            a_json += &format!("\"target_node\":{},", a.target_node);
            a_json += &format!("\"action\":{{{}}}", a.action.to_json());
            json += &a_json;
            json += "},";
        }
        json = json.trim_end_matches(',').to_string();
        json += "],";

        json += &format!(
            "\"save_mode\":{},",
            serde_json::to_string(&self.save_mode).unwrap()
        );

        if let Some(var) = &self.aoe_var {
            json += &format!("\"aoe_var\":[{}, {:?}]", var.0, var.1);
        } else {
            json += "\"aoe_var\":[]";
        }

        json += "}";
        json
    }
}

pub fn from_json_to_dff_model(json_str: &str) -> Result<DffModel, String> {
    match serde_json::from_str::<serde_json::Value>(json_str) {
        Ok(json_value) => {
            from_serde_value_to_dff_model(&json_value)
        }
        Err(e) => {
            let tip = format!("DffModel parse failed for serde_json: {e:?}");
            Err(tip)
        }
    }
}

pub(crate) fn from_serde_value_to_dff_model(json_value: &serde_json::Value) -> Result<DffModel, String> {
    let mut model = DffModel {
        id: 0,
        is_on: false,
        name: "".to_string(),
        trigger_type: DfTriggerType::DataSource,
        nodes: vec![],
        actions: vec![],
        save_mode: DfSaveMode::EveryTime,
        aoe_var: None,
    };
    if let Some(id) = json_value["id"].as_u64() {
        model.id = id;
    } else {
        return Err("DffModel parse failed for id".to_string());
    }
    if let Some(is_on) = json_value["is_on"].as_bool() {
        model.is_on = is_on;
    } else {
        return Err("DffModel parse failed for is_on".to_string());
    }
    model.name = json_value["name"].to_string().trim_start_matches('"').trim_end_matches('"').to_string();

    if let Ok(t) = serde_json::from_value::<DfTriggerType>(json_value["trigger_type"].clone()) {
        model.trigger_type = t;
    } else {
        return Err("DffModel parse failed for trigger_type".to_string());
    }

    if let Some(array) = json_value["nodes"].as_array() {
        for e in array {
            let node_r = from_json_to_dff_node(&e.to_string());
            match node_r {
                Ok(node) => model.nodes.push(node),
                Err(err) => return Err(err),
            }
        }
    }

    if let Some(array) = json_value["actions"].as_array() {
        for e in array {
            let edge_r = from_json_to_dff_edge(&e.to_string());
            match edge_r {
                Ok(edge) => model.actions.push(edge),
                Err(err) => return Err(err),
            }
        }
    }

    if let Ok(s) = serde_json::from_value::<DfSaveMode>(json_value["save_mode"].clone()) {
        model.save_mode = s;
    } else {
        return Err("DffModel parse failed for save_mode".to_string());
    }

    match &json_value["aoe_var"] {
        serde_json::Value::Array(arr) => {
            if arr.len() == 2 {
                match &arr[0] {
                    serde_json::Value::Number(num) => match &arr[1] {
                        serde_json::Value::String(s) => {
                            if let Some(n) = num.as_u64() {
                                model.aoe_var = Some((n, s.clone()));
                            } else {
                                return Err("DffModel parse failed for aoe_var id should be u64".to_string());
                            }
                        }
                        _ => return Err("DffModel parse failed for aoe_var name should be string".to_string()),
                    }
                    _ => return Err("DffModel parse failed for aoe_var id should be number".to_string()),
                }
            }
        }
        serde_json::Value::Null => {},
        _ => return Err("DffModel parse failed for aoe_var".to_string()),
    }

    Ok(model)
}

fn from_jsvalue_to_evalpara(v: &serde_json::Value) -> Result<(EvalType, Vec<Expr>), String> {
    if let serde_json::Value::Object(obj) = v {
        for (key, value) in obj.iter() {
            return if let Ok(expr_str) = serde_json::from_value::<Vec<String>>(value.clone()) {
                let mut exprs = vec![];
                for expr_s in expr_str {
                    if let Ok(expr) = Expr::from_str(&expr_s) {
                        exprs.push(expr);
                    }
                }
                match key.as_str() {
                    "Select" => Ok((EvalType::Select, exprs)),
                    "Filter" => Ok((EvalType::Filter, exprs)),
                    "WithColumn" => Ok((EvalType::WithColumn, exprs)),
                    "WithColumns" => Ok((EvalType::WithColumns, exprs)),
                    "GroupBy" => Ok((EvalType::GroupBy, exprs)),
                    _ => Err("DffModel Eval para parse failed for EvalType".to_string()),
                }
            } else {
                Err("DffModel Eval para parse failed for Vec<Expr>".to_string())
            }
        }
    }
    Err("DffModel Eval para parse failed for json format".to_string())
}

// json_str为键值对中的value，即Source后面的obj
fn from_json_to_dfsource(json_str: &str) -> Result<DfSource, String> {
    if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
        if let serde_json::Value::Object(obj) = json_value {
            for (key, value) in obj.iter() {
                return match key.as_str() {
                    "Data" => {
                        if let Ok(df) = serde_json::from_value::<DataFrame>(value.clone()) {
                            Ok(DfSource::Data(df))
                        } else {
                            Ok(DfSource::Data(DataFrame::empty()))
                        }
                    }
                    "File" => {
                        Ok(DfSource::File(value.to_string().trim_start_matches('"').trim_end_matches('"').to_string()))
                    }
                    "Url" => {
                        Ok(DfSource::Url(value.to_string().trim_start_matches('"').trim_end_matches('"').to_string()))
                    }
                    "Image" => {
                        if let Ok(i) = serde_json::from_value::<ImageDfFilter>(value.clone()) {
                            Ok(DfSource::Image(i))
                        } else {
                            Err("DffModel DfSource failed for Image".to_string())
                        }
                    }
                    "Sql" => {
                        if let Ok(s) = serde_json::from_value::<Vec<String>>(value.clone()) {
                            if s.len() == 2 {
                                let sql_type = get_sql_type(&s[0]);
                                Ok(DfSource::Sql(sql_type, s[1].trim_start_matches('"').trim_end_matches('"').to_string()))
                            } else {
                                Err("DffModel DfSource failed for Sql len".to_string())
                            }
                        } else {
                            Err("DffModel DfSource failed for Sql".to_string())
                        }
                    }
                    "OtherFlow" => {
                        if let Ok(id) = serde_json::from_value::<u64>(value.clone()) {
                            Ok(DfSource::OtherFlow(id))
                        } else {
                            Err("DffModel DfSource failed for OtherFlow".to_string())
                        }
                    }
                    "Dev" => {
                        Ok(DfSource::Dev(value.to_string().trim_start_matches('"').trim_end_matches('"').to_string()))
                    }
                    "Points" => {
                        Ok(DfSource::Points(value.to_string().trim_start_matches('"').trim_end_matches('"').to_string()))
                    }
                    "Meas" => {
                        if let Ok(s) = serde_json::from_value::<Vec<String>>(value.clone()) {
                            if s.len() == 2 {
                                Ok(DfSource::Meas(s[0].clone(), s[1].clone()))
                            } else {
                                Err("DffModel DfSource failed for Meas len".to_string())
                            }
                        } else {
                            Err("DffModel DfSource failed for Meas".to_string())
                        }
                    }
                    "Plan" => {
                        Ok(DfSource::Plan(value.to_string().trim_start_matches('"').trim_end_matches('"').to_string()))
                    }
                    "PointsEval" => {
                        match from_jsvalue_to_evalpara(value) {
                            Ok(a) => { Ok(DfSource::PointsEval(a.0, a.1)) }
                            Err(s) => { Err(s) }
                        }
                    }
                    "MeasEval" => {
                        if let serde_json::Value::Object(obj) = value {
                            let mut s = "".to_string();
                            let mut e: (EvalType, Vec<Expr>) = (EvalType::Filter, vec![]);
                            let mut flag1 = false;
                            let mut flag2 = false;
                            for (k, v) in obj.iter() {
                                if k == "Meas" {
                                    s = v.to_string().trim_start_matches('"').trim_end_matches('"').to_string();
                                    flag1 = true;
                                }
                                if k == "EvalType" {
                                    match from_jsvalue_to_evalpara(v) {
                                        Ok(a) => {
                                            e = a;
                                            flag2 = true;
                                        }
                                        Err(s) => { return Err(s); }
                                    }
                                }
                            }
                            if flag1 && flag2 {
                                Ok(DfSource::MeasEval(s, e.0, e.1))
                            } else {
                                Err("DffModel DfSource failed for MeasEval format".to_string())
                            }
                        } else {
                            Err("DffModel DfSource failed for MeasEval".to_string())
                        }
                    }
                    _ => Err("DffModel DfSource failed for key".to_string()),
                }
            }
        }
    }
    Err("DffModel DfSource failed for json format".to_string())
}

// json_str为键值对中的value
fn from_json_to_dff_node_type(json_str: &str, node_type: &str) -> Result<DfNodeType, String> {
    match node_type.to_uppercase().replace(['_', '-'], "").as_str() {
        "SOURCE" => {
            return match from_json_to_dfsource(json_str) {
                Ok(s) => { Ok(DfNodeType::Source(s)) }
                Err(s) => { Err(s) }
            };
        }
        "TRANSFORM" => {
            return if let Ok(expr) = Expr::from_str(json_str.trim_start_matches('"').trim_end_matches('"')) {
                Ok(DfNodeType::Transform(expr))
            } else {
                Err("DffModel DfNodeType parse failed for Transform".to_string())
            }
        }
        "TENSOREVAL" | "TENSOR_EVAL" => {
            return if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                return if let serde_json::Value::Object(mut obj) = json_value {
                    let para: u8;
                    if let Some(b_value) = obj.get("para") {
                        if let Some(b) = b_value.as_i64() {
                            para = b as u8;
                        } else {
                            return Err("DffModel DfNodeType parse failed for is_complex format".to_string());
                        }
                    } else {
                        return Err("DffModel DfNodeType parse failed for is_complex lacking".to_string());
                    }
                    let g;
                    if let Some(e_value) = obj.remove("exprs") {
                        match serde_json::from_value::<DiGraph<StmtNode, u32>>(e_value) {
                            Ok(v) => g = v,
                            Err(e) => return Err(format!("DffModel DfNodeType parse failed for exprs format, err: {e:?}")),
                        }
                    } else {
                        return Err("DffModel DfNodeType parse failed for exprs lacking".to_string());
                    }
                    let names = if let Some(e_value) = obj.get("names") {
                        if let serde_json::Value::Array(arr_vec) = e_value {
                            let v = arr_vec.iter().map(|x| x.to_string().trim_start_matches('"')
                                .trim_end_matches('"').to_string()).collect::<Vec<String>>();
                            Some(v)
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    Ok(DfNodeType::TensorEval(g, para, names))
                } else {
                    Err("DffModel DfNodeType parse failed for TensorEval json".to_string())
                }
            } else {
                Err("DffModel DfNodeType parse failed for TensorEval".to_string())
            }
        }
        "SQL" => {
            return Ok(DfNodeType::Sql(json_str.trim_start_matches('"').trim_end_matches('"').to_string()));
        }
        "SOLVE" => {
            return if let Ok(b) = serde_json::from_str::<bool>(json_str) {
                Ok(DfNodeType::Solve(b))
            } else {
                Err("DffModel DfNodeType parse failed for Solve".to_string())
            }
        }
        "NLSOLVE" => {
            return Ok(DfNodeType::NLSolve);
        }
        "MILP" => {
            return if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                let mut s1 = "".to_string();
                let mut s2 = "".to_string();
                let mut b = false;
                let mut flag1 = false;
                let mut flag2 = false;
                let mut flag3 = false;
                if let serde_json::Value::Object(obj) = json_value {
                    if let Some(v) = obj.get("obj_df_name") {
                        s1 = v.to_string().trim_start_matches('"').trim_end_matches('"').to_string();
                        flag1 = true;
                    }
                    if let Some(v) = obj.get("constraints_df_name") {
                        s2 = v.to_string().trim_start_matches('"').trim_end_matches('"').to_string();
                        flag2 = true;
                    }
                    if let Some(v) = obj.get("is_sparse") {
                        if let Ok(is_sparse) = serde_json::from_value::<bool>(v.clone()) {
                            b = is_sparse;
                            flag3 = true;
                        } else {
                            return Err("DffModel DfNodeType parse failed for MILP is_sparse".to_string());
                        }
                    }
                }
                if flag1 && flag2 && flag3 {
                    Ok(DfNodeType::MILP(s1, s2, b))
                } else {
                    Err("DffModel DfNodeType parse failed for MILP format".to_string())
                }
            } else {
                Err("DffModel DfNodeType parse failed for MILP".to_string())
            }
        }
        "NLP" => {
            return if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                let mut s1 = "".to_string();
                let mut s2 = "".to_string();
                let mut flag1 = false;
                let mut flag2 = false;
                if let serde_json::Value::Object(obj) = json_value {
                    if let Some(v) = obj.get("x_df_name") {
                        s1 = v.to_string().trim_start_matches('"').trim_end_matches('"').to_string();
                        flag1 = true;
                    }
                    if let Some(v) = obj.get("constraints_df_name") {
                        s2 = v.to_string().trim_start_matches('"').trim_end_matches('"').to_string();
                        flag2 = true;
                    }
                }
                if flag1 && flag2 {
                    Ok(DfNodeType::NLP(s1, s2))
                } else {
                    Err("DffModel DfNodeType parse failed for NLP format".to_string())
                }
            } else {
                Err("DffModel DfNodeType parse failed for NLP".to_string())
            }
        }
        "WASM" => {
            let mut w_type = vec![];
            let mut w_data = vec![];
            if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(json_str) {
                if let serde_json::Value::Object(obj) = json_value {
                    if let Some(v) = obj.get("model_type") {
                        if let Ok(t) = serde_json::from_value::<Vec<ModelType>>(v.clone()) {
                            w_type = t;
                        }
                    }
                    if let Some(v) = obj.get("data") {
                        let path = v.to_string().trim_start_matches('"').trim_end_matches('"').to_string();
                        if let Ok(bytes) = engine::general_purpose::STANDARD.decode(path) {
                            w_data = bytes;
                        }
                    }
                }
                return Ok(DfNodeType::Wasm(w_type, w_data));
            } else {
                return Err("DffModel DfNodeType parse failed for Wasm".to_string());
            }
        }
        "NONE" => return Ok(DfNodeType::None),
        _ => {}
    }
    Err("DffModel DfNodeType parse failed for Type".to_string())
}

// json_str为键值对中的value
fn from_json_to_df_action(json_str: &str, action_type: &str) -> Result<DfAction, String> {
    match action_type.to_uppercase().replace(['_', '-'], "").as_str() {
        "KMEANS" => {
            return Ok(DfAction::Kmeans);
        }
        "RANDOMTREE" => {
            return Ok(DfAction::RandomTree);
        }
        "EVAL" => {
            return if let Ok(value) = serde_json::Value::from_str(json_str) {
                match from_jsvalue_to_evalpara(&value) {
                    Ok(a) => { Ok(DfAction::Eval(a.0, a.1)) }
                    Err(s) => { Err(s) }
                }
            } else {
                Err("DffModel DfAction parse failed for Eval".to_string())
            }
        }
        "SQL" => {
            return Ok(DfAction::Sql(json_str.trim_start_matches('"').trim_end_matches('"').to_string()));
        }
        "ONNX" => {
            let path = json_str.trim_start_matches('"').trim_end_matches('"').to_string();
            return if let Ok(bytes) = engine::general_purpose::STANDARD.decode(path) {
                Ok(DfAction::Onnx(bytes))
            } else {
                Err("DffModel DfAction parse failed for Onnx".to_string())
            };
        }
        "ONNXURL" => {
            return Ok(DfAction::OnnxUrl(json_str.trim_start_matches('"').trim_end_matches('"').to_string()));
        }
        "NNEF" => {
            let path = json_str.trim_start_matches('"').trim_end_matches('"').to_string();
            if let Ok(bytes) = engine::general_purpose::STANDARD.decode(path) {
                return Ok(DfAction::Nnef(bytes));
            }
        }
        "NNEFURL" => {
            return Ok(DfAction::NnefUrl(json_str.trim_start_matches('"').trim_end_matches('"').to_string()));
        }
        "WRITEFILE" => {
            return Ok(DfAction::WriteFile(json_str.trim_start_matches('"').trim_end_matches('"').to_string()));
        }
        "NONE" => {
            return Ok(DfAction::None);
        }
        _ => {}
    }
    Err("DffModel DfAction parse failed for Type".to_string())
}

pub fn from_json_to_dff_node(e_json: &str) -> Result<DfNode, String> {
    let mut model = DfNode {
        id: 0,
        flow_id: 0,
        name: "".to_string(),
        node_type: DfNodeType::None,
    };
    match serde_json::from_str::<serde_json::Value>(e_json) {
        Ok(json_value) => {
            if let Some(id) = json_value["id"].as_u64() {
                model.id = id;
            } else {
                return Err("DfNode parse failed for id".to_string());
            }
            if let Some(flow_id) = json_value["flow_id"].as_u64() {
                model.flow_id = flow_id;
            } else {
                return Err("DfNode parse failed for flow_id".to_string());
            }
            model.name = json_value["name"].to_string().trim_start_matches('"').trim_end_matches('"').to_string();
            if let Some(obj) = json_value["node_type"].as_object() {
                for (key, value) in obj {
                    match from_json_to_dff_node_type(&value.to_string(), key) {
                        Ok(a) => { model.node_type = a; }
                        Err(s) => { return Err(s); }
                    }
                }
            } else if let Some(node_type) = json_value["node_type"].as_str() {
                match node_type.to_uppercase().replace(['_', '-'], "").as_str() {
                    "NLSOLVE" => { model.node_type = DfNodeType::NLSolve },
                    "NONE" => { model.node_type = DfNodeType::None },
                    _ => { return Err(node_type.to_string()); }
                }
            } else {
                return Err("DffModel DfNode parse failed for node_type json format".to_string());
            }
            Ok(model)
        }
        Err(e) => {
            Err(format!("DffModel DfNode parse failed for json format, err:{e:?}"))
        }
    }
}

pub fn from_json_to_dff_edge(e_json: &str) -> Result<DfActionEdge, String> {
    let mut model = DfActionEdge {
        flow_id: 0,
        name: "".to_string(),
        desc: "".to_string(),
        source_node: 0,
        target_node: 0,
        action: DfAction::None,
    };
    if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(e_json) {
        if let Some(flow_id) = json_value["flow_id"].as_u64() {
            model.flow_id = flow_id;
        } else {
            return Err("DfActionEdge parse failed for flow_id".to_string());
        }
        model.name = json_value["name"].to_string().trim_start_matches('"').trim_end_matches('"').to_string();
        model.desc = json_value["desc"].to_string().trim_start_matches('"').trim_end_matches('"').to_string();
        if let Some(source_node) = json_value["source_node"].as_u64() {
            model.source_node = source_node;
        } else {
            return Err("DfActionEdge parse failed for source_node".to_string());
        }
        if let Some(target_node) = json_value["target_node"].as_u64() {
            model.target_node = target_node;
        } else {
            return Err("DfActionEdge parse failed for target_node".to_string());
        }
        if let Some(obj) = json_value["action"].as_object() {
            for (key, value) in obj {
                match from_json_to_df_action(&value.to_string(), key) {
                    Ok(a) => { model.action = a; }
                    Err(s) => {
                        return Err(format!("DfActionEdge parse failed for action, err: {}", s));
                    }
                }
            }
        }
    }
    Ok(model)
}

pub fn rpn_to_string(input: &[Token]) -> Result<String, RPNError> {
    let mut output = String::new();
    let infix = rpn_to_infix(input)?;
    for token in infix.iter() {
        match token {
            Token::Binary(op) => match op {
                Operation::Plus => output.push('+'),
                Operation::Minus => output.push('-'),
                Operation::Times => output.push('*'),
                Operation::Div => output.push('/'),
                Operation::LeftDiv => output.push('\\'),
                Operation::Rem => output.push('%'),
                Operation::Pow => output.push('^'),
                Operation::Equal => output.push_str("=="),
                Operation::Unequal => output.push_str("!="),
                Operation::LessThan => output.push('<'),
                Operation::GreatThan => output.push('>'),
                Operation::LtOrEqual => output.push_str("<="),
                Operation::GtOrEqual => output.push_str(">="),
                Operation::And => output.push_str("&&"),
                Operation::Or => output.push_str("||"),
                Operation::BitAnd => output.push('&'),
                Operation::BitOr => output.push('|'),
                Operation::BitXor => output.push_str("^^"),
                Operation::BitShl => output.push_str("<<"),
                Operation::BitShr => output.push_str(">>"),
                Operation::BitAt => output.push('@'),
                Operation::DotTimes => output.push_str(".*"),
                Operation::DotDiv => output.push_str("./"),
                Operation::DotPow => output.push_str(".^"),
                _ => output.push_str("Unsupported"),
            }
            Token::Unary(op) => match op {
                Operation::Not => output.push_str("~~"),
                Operation::BitNot => output.push('~'),
                Operation::Fact => output.push('!'),
                Operation::Plus => output.push('+'),
                Operation::Minus => output.push('-'),
                Operation::Transpose => output.push('\''),
                _ => output.push_str("Unsupported"),
            }
            Token::LParen => output.push('('),
            Token::RParen => output.push(')'),
            Token::Comma => output.push(','),
            Token::Number(n) => output.push_str(&format!("{}", n)),
            Token::Var(v) => {
                if v.is_empty() {
                    return Err(RPNError::NotEnoughOperands(0));
                }
                let bytes = v.as_bytes();
                if matches!(bytes[0], b'a'..=b'z' | b'A'..=b'Z' | b'_' | b'$') &&
                    bytes.iter().all(|b|  matches!(b, b'a'..=b'z' | b'A'..=b'Z' | b'_' | b'0'..=b'9')) {
                    output.push_str(v.as_str())
                } else {
                    output.push_str(&format!("\"{v}\""))
                }
            }
            Token::Str(v) => output.push_str(v.as_str()),
            Token::Func(func, _) => output.push_str(&format!("{func}(")),
            Token::Tensor(_) => output.push('['),
            Token::RBracket => output.push(']'),
            Token::BigLParen => output.push('{'),
            Token::BigRParen => output.push('}'),
        }
    }
    Ok(output)
}

// 根据连接地址获得数据源类型
pub fn get_sql_type(source_url: &str) -> DfSqlType {
    if source_url.starts_with("mysql:") {
        DfSqlType::MySql(source_url.to_string())
    } else if source_url.starts_with("sqlite:") {
        DfSqlType::Sqlite(source_url.to_string())
    } else if source_url.starts_with("postgres:") {
        DfSqlType::Postgres(source_url.to_string())
    } else {
        DfSqlType::Unknown(source_url.to_string())
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

pub fn rpn_to_infix(input: &[Token]) -> Result<Vec<Token>, RPNError> {
    use crate::utils::shuntingyard::Associativity::*;
    use crate::model::south::Operation::*;
    use crate::model::south::Token::*;

    if input.is_empty() {
        return Ok(vec![]);
    }
    let mut stack = Vec::with_capacity(input.len());

    for (index, token) in input.iter().enumerate() {
        let token = token.clone();
        match token {
            Number(_) | Var(_) | Str(_) => {
                let pa = prec_assoc(&token);
                stack.push((vec![token], pa));
            }
            Tensor(nargs) => {
                let nargs = nargs.unwrap_or(0);
                let pa = prec_assoc(&token);
                let mut infix = vec![RBracket];
                for i in 0..nargs {
                    if stack.is_empty() {
                        return Err(RPNError::NotEnoughOperands(index));
                    }
                    let mut argu = stack.pop().unwrap().0;
                    if i >= 1 {
                        argu.push(Comma);
                    }
                    argu.append(&mut infix);
                    infix = argu;
                }
                infix.insert(0, token);
                stack.push((infix, pa));
            }
            Unary(_) => {
                if stack.is_empty() {
                    return Err(RPNError::NotEnoughOperands(index));
                }
                let (i, assoc) = stack.last().unwrap().1;
                let mut infix1 = stack.pop().unwrap().0;
                let pa = prec_assoc(&token);
                match assoc {
                    NA => {
                        if i < pa.0 && i != 0 {
                            infix1.insert(0, LParen);
                            infix1.push(RParen);
                        }
                    }
                    _ => {
                        if i <= pa.0 && i != 0 {
                            infix1.insert(0, LParen);
                            infix1.push(RParen);
                        }
                    }
                }
                infix1.insert(0, token);
                stack.push((infix1, pa));
            }
            Binary(op) => {
                let pa = prec_assoc(&token);
                let prec = pa.0;
                if stack.is_empty() {
                    return Err(RPNError::NotEnoughOperands(index));
                }
                let (precr, assocr) = stack.last().unwrap().1; //右边
                let mut infixr = stack.pop().unwrap().0;
                if stack.is_empty() {
                    return Err(RPNError::NotEnoughOperands(index));
                }
                let (precl, _) = stack.last().unwrap().1; //左边
                let mut infixl = stack.pop().unwrap().0;
                // let mut laddparen = false;
                let mut raddparen = false;
                match op {
                    Plus | Times => {
                        // 对于+和*，同级运算符不需要加括号
                        if precl < prec && precl != 0 {
                            infixl.insert(0, LParen);
                            infixl.push(RParen);
                            // laddparen = true;
                        }
                        if precr < prec && precr != 0 {
                            infixr.insert(0, LParen);
                            infixr.push(RParen);
                            raddparen = true;
                        }
                    }
                    Pow => {
                        // 字符串的次幂用普通括号
                        if precl <= prec && precl != 0 {
                            infixl.insert(0, LParen);
                            infixl.push(RParen);
                            // laddparen = true;
                        }
                        if precr < prec && precr != 0 {
                            infixr.insert(0, LParen);
                            infixr.push(RParen);
                            raddparen = true;
                        }
                    }
                    _ => {
                        if precl < prec && precl != 0 {
                            infixl.insert(0, LParen);
                            infixl.push(RParen);
                        }
                        if precr <= prec && precr != 0 {
                            infixr.insert(0, LParen);
                            infixr.push(RParen);
                            raddparen = true;
                        }
                    }
                }

                // 左边的单目加括号
                // if !laddparen && matches!(assocl,NA) && precl == 13 {
                //     infixl.insert(0, LParen);
                //     infixl.push(RParen);
                // }

                // 右边的单目加括号，单目的优先级较高，但在数学式子中习惯加括号，如-a+-b习惯写作-a+(-b)
                if !raddparen && matches!(assocr, NA) && precr == 13 {
                    infixr.insert(0, LParen);
                    infixr.push(RParen);
                }

                infixl.push(token);
                infixl.append(&mut infixr);
                stack.push((infixl, pa));
            }
            Func(_, nargs) => {
                let nargs = nargs.unwrap_or(0);
                let pa = prec_assoc(&token);
                let mut infix = vec![RParen];
                for i in 0..nargs {
                    if stack.is_empty() {
                        return Err(RPNError::NotEnoughOperands(index));
                    }
                    let mut argu = stack.pop().unwrap().0;
                    if i >= 1 {
                        argu.push(Comma);
                    }
                    argu.append(&mut infix);
                    infix = argu;
                }
                infix.insert(0, token);
                stack.push((infix, pa));
            }
            _ => {}
        }
    }

    if stack.len() != 1 {
        return Err(RPNError::TooManyOperands);
    }
    let output = stack.pop().unwrap().0;
    Ok(output)
}

pub fn rpn_to_infix_latex(input: &[Token]) -> Result<Vec<Token>, RPNError> {
    // 用于latex的中缀表达式，不同于rpn_to_infix，这里的pow使用{}包裹，除法均改为分式。
    use crate::utils::shuntingyard::Associativity::*;
    use crate::model::south::Operation::*;
    use crate::model::south::Token::*;

    if input.is_empty() {
        return Ok(vec![]);
    }
    let mut stack = Vec::with_capacity(input.len());

    for (index, token) in input.iter().enumerate() {
        let token = token.clone();
        match token {
            Number(_) | Var(_) | Str(_) => {
                let pa = prec_assoc(&token);
                stack.push((vec![token], pa));
            }
            Tensor(nargs) => {
                let nargs = nargs.unwrap_or(0);
                let pa = prec_assoc(&token);
                let mut infix = vec![RBracket];
                for i in 0..nargs {
                    if stack.is_empty() {
                        return Err(RPNError::NotEnoughOperands(index));
                    }
                    let mut argu = stack.pop().unwrap().0;
                    if i >= 1 {
                        argu.push(Comma);
                    }
                    argu.append(&mut infix);
                    infix = argu;
                }
                infix.insert(0, token);
                stack.push((infix, pa));
            }
            Unary(op) => {
                if stack.is_empty() {
                    return Err(RPNError::NotEnoughOperands(index));
                }
                let (i, assoc) = stack.last().unwrap().1;
                let mut infix1 = stack.pop().unwrap().0;
                let pa = prec_assoc(&token);
                match assoc {
                    NA => {
                        if i < pa.0 && i != 0 {
                            infix1.insert(0, LParen);
                            infix1.push(RParen);
                        }
                    }
                    _ => {
                        if i <= pa.0 && i != 0 {
                            infix1.insert(0, LParen);
                            infix1.push(RParen);
                        }
                    }
                }
                match op {
                    Plus | Minus | Not | BitNot => infix1.insert(0, token),
                    Fact => infix1.push(token),
                    _ => unimplemented!(),
                }
                stack.push((infix1, pa));
            }
            Binary(op) => {
                let pa = prec_assoc(&token);
                let prec = pa.0;
                if stack.is_empty() {
                    return Err(RPNError::NotEnoughOperands(index));
                }
                let (precr, assocr) = stack.last().unwrap().1; //右边
                let mut infixr = stack.pop().unwrap().0;
                if stack.is_empty() {
                    return Err(RPNError::NotEnoughOperands(index));
                }
                let (precl, _) = stack.last().unwrap().1; //左边
                let mut infixl = stack.pop().unwrap().0;
                // let mut laddparen = false;
                let mut raddparen = false;
                match op {
                    Plus | Times => {
                        // 对于+和*，同级运算符不需要加括号
                        if precl < prec && precl != 0 {
                            infixl.insert(0, LParen);
                            infixl.push(RParen);
                            // laddparen = true;
                        }
                        if precr < prec && precr != 0 {
                            infixr.insert(0, LParen);
                            infixr.push(RParen);
                            raddparen = true;
                        }
                    }
                    Div => {
                        // \frac{l}{r} 除法不加括号，直接形成分式
                        infixl.insert(0, Binary(Div)); //类似函数的逻辑，先把\frac{放进去，再把分子分母放进去
                        infixl.push(BigRParen);
                        infixr.insert(0, BigLParen);
                        infixr.push(BigRParen);
                        infixl.append(&mut infixr);
                        stack.push((infixl, (prec, assocr)));
                        continue;
                    }
                    Pow => {
                        // latex的次幂用大括号
                        if precl <= prec && precl != 0 {
                            infixl.insert(0, LParen);
                            infixl.push(RParen);
                            // laddparen = true;
                        }
                        infixr.insert(0, BigLParen);
                        infixr.push(BigRParen);
                        raddparen = true;
                    }
                    _ => {
                        if precl < prec && precl != 0 {
                            infixl.insert(0, LParen);
                            infixl.push(RParen);
                        }
                        if precr <= prec && precr != 0 {
                            infixr.insert(0, LParen);
                            infixr.push(RParen);
                            raddparen = true;
                        }
                    }
                }

                // 左边的单目加括号
                // if !laddparen && matches!(assocl,NA) && precl == 13 {
                //     infixl.insert(0, LParen);
                //     infixl.push(RParen);
                // }

                // 右边的单目加括号，单目的优先级较高，但在数学式子中习惯加括号，如-a+-b习惯写作-a+(-b)
                if !raddparen && matches!(assocr, NA) && precr == 13 {
                    infixr.insert(0, LParen);
                    infixr.push(RParen);
                }

                infixl.push(token);
                infixl.append(&mut infixr);
                stack.push((infixl, pa));
            }
            Func(_, nargs) => {
                let nargs = nargs.unwrap_or(0);
                let pa = prec_assoc(&token);
                let mut infix = vec![RParen];
                for i in 0..nargs {
                    if stack.is_empty() {
                        return Err(RPNError::NotEnoughOperands(index));
                    }
                    let mut argu = stack.pop().unwrap().0;
                    if i >= 1 {
                        argu.push(Comma);
                    }
                    argu.append(&mut infix);
                    infix = argu;
                }
                infix.insert(0, token);
                stack.push((infix, pa));
            }
            _ => {}
        }
    }

    if stack.len() != 1 {
        return Err(RPNError::TooManyOperands);
    }
    let output = stack.pop().unwrap().0;
    Ok(output)
}

/// Returns the operator precedence and associativity for a given token.
fn prec_assoc(token: &Token) -> (u32, crate::utils::shuntingyard::Associativity) {
    use crate::utils::shuntingyard::Associativity::*;
    use crate::model::south::Operation::*;
    use crate::model::south::Token::*;
    match *token {
        Binary(op) => match op {
            Or => (3, Left),
            And => (4, Left),
            BitOr => (5, Left),
            BitXor => (6, Left),
            BitAnd => (7, Left),
            Equal | Unequal => (8, Left),
            LessThan | GreatThan | LtOrEqual | GtOrEqual => (9, Left),
            BitShl | BitShr => (10, Left),
            Plus | Minus => (11, Left),
            Times | Div | LeftDiv | Rem | DotTimes | DotDiv => (12, Left),
            BitAt => (13, Left),
            Pow | DotPow  => (14, Right),
            _ => unimplemented!(),
        }
        Unary(op) => match op {
            Plus | Minus | Not | BitNot => (13, NA),
            Fact => (15, NA),
            Transpose =>  (16, NA),
            _ => unimplemented!(),
        }
        Var(_) | Str(_) | Number(_) | Func(..) | Tensor(_) | LParen | RParen | BigLParen | BigRParen
        | RBracket | Comma => (0, NA),
    }
}
