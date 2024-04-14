use std::collections::HashMap;

use crate::redis::parse::RespData;

use super::lib::PersistedType;

#[derive(Debug)]
pub struct PersistedValue {
    pub data: RespData,
    pub p_type: PersistedType,
    pub timestamp: std::time::SystemTime,
    pub expiry: u128,
}

#[derive(Default)]
pub struct KeyValuePersistence(pub HashMap<String, PersistedValue>);