use std::collections::{BTreeMap, HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};
use derive_more::with_trait::{Display, FromStr};

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct Register {
    pub token: String,
    pub time: String,
    pub body: Vec<RegisterBody>,
}

#[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
pub struct RegisterBody {
    pub model: String,
    pub port: String,
    pub addr: String,
    pub desc: String,
    pub manuID: String,
    pub manuName: String,
    pub proType: String,
    pub deviceType: String,
    pub isReport: String,
    pub nodeID: String,
    pub productID: String,
}
