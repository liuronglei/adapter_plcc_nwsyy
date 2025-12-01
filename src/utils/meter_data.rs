use std::collections::HashMap;

pub struct MeterDataResult {
    pub meter_sum_no: String,
    pub meter_nos: Vec<String>,
    pub timestamp: Vec<String>,
    pub meter_data: HashMap<(String, String), String>,
}

pub fn export_meter_csv(
    meter_sum_no: &str,
    timestamps: Vec<String>,
    meter_data: HashMap<(String, String), String>,
) -> String {
    //时间戳排序
    let mut ts_sorted = timestamps;
    ts_sorted.sort();

    //收集所有meter
    let mut all_meters = meter_data
        .keys()
        .map(|(meter, _)| meter.clone())
        .collect::<Vec<_>>();
    all_meters.sort();
    all_meters.dedup();

    // sum_no之外的meters
    let other_meters: Vec<String> = all_meters
        .iter()
        .filter(|m| m.as_str() != meter_sum_no)
        .cloned()
        .collect();

    //构建表头
    let line_count = ts_sorted.len();
    let mut header = vec![
        format!("pin_{line_count}_1"),
        format!("pout_{line_count}_1"),
    ];
    for other_meter in &other_meters {
        header.push(format!("p{other_meter}_{line_count}_1"));
    }

    let mut lines = vec![header.join(",")];

    //对每个timestamp构造一行
    for ts in ts_sorted {
        // meter_sum_no 的值
        let sum_no_val = meter_data
            .get(&(meter_sum_no.to_string(), ts.clone()))
            .cloned()
            .unwrap_or("".to_string());
        // other meter 的值
        let mut other_vals = vec![];
        let mut other_sum = 0f64;
        for m in &other_meters {
            let v = meter_data
                .get(&(m.clone(), ts.clone()))
                .cloned()
                .unwrap_or("".to_string());
            if let Ok(num) = v.parse::<f64>() {
                other_sum += num;
            }
            other_vals.push(v);
        }
        // 拼接一整行
        let line = format!(
            "{},{},{}",
            sum_no_val,
            other_sum,
            other_vals.join(",")
        );
        lines.push(line);
    }

    // 返回完整 CSV 字符串
    lines.join("\n")
}