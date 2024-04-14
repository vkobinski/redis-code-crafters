use std::sync::{Arc, Mutex};

use super::{kv_pair::KeyValuePersistence, stream::StreamPersistence};

#[derive(Debug)]
pub enum PersistedType {
    String,
    Stream,
}

#[derive(Default)]
pub struct PersistenceInner {
    pub key_value: Mutex<KeyValuePersistence>,
    pub stream: Mutex<StreamPersistence>,
}

pub type Persistence = Arc<PersistenceInner>;