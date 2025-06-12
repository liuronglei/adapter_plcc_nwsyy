use std::str::FromStr;
use serde::{Deserialize, Serialize};

use north::MyPoints;
use south::Measurement;
use south::{DataUnit, Expr};

pub mod north;
pub mod south;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ParserResult {
    pub result: bool,
    pub value: String,
}

pub fn north_to_south(points: MyPoints) -> Vec<Measurement> {
    let mut result = vec![];
    for p in points.points {
        let point_id = p.point_id;
        let unit = DataUnit::from_str(p.data_unit.as_str()).unwrap_or(DataUnit::Unknown);
        let alarm_level1 = Expr::from_str(p.alarm_level1_expr.as_str()).ok();
        let alarm_level2 = Expr::from_str(p.alarm_level2_expr.as_str()).ok();
    }
    result
}

