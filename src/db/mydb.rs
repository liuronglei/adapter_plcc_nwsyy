
static ERROR_DB_PATH: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

// todo
pub fn is_error_db_path(file_path: &str) -> bool {
    let path = file_path.to_owned();
    ERROR_DB_PATH.lock().unwrap().contains(&path)
}

pub fn add_error_db_path(file_path: &str) {
    let path = file_path.to_owned();
    ERROR_DB_PATH.lock().unwrap().push(path);
}