use std::time::{SystemTime, UNIX_EPOCH};

pub fn millis_from_epoch() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}
