use std::sync::Mutex;

use super::{kv_pair::KeyValuePersistence, stream::StreamPersistence};

#[derive(Debug)]
pub enum PersistedType {
    String,
}

#[derive(Default)]
pub struct PersistenceInner {
    pub key_value: Mutex<KeyValuePersistence>,
    pub stream: Mutex<StreamPersistence>,
}