pub mod tokenizer;
pub mod shuntingyard;
pub mod plccapi;
use regex::Regex;

pub fn get_north_tag(north_point: &str) -> Option<String> {
    let re = Regex::new(r"\$\{([^}]+)\}").unwrap();
    if let Some(captures) = re.captures(north_point) {
        let var_name = &captures[1];
        if let Some(tag) = var_name.split(".").last() {
            return Some(tag.to_string());
        }
    }
    None
}
