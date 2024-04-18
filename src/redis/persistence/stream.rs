use std::{
    collections::HashMap,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
    vec,
};

use crate::redis::parse::RespData;

#[derive(Clone, Debug)]
pub struct StreamVal {
    pub id: (u128, u32),
    pub pairs: Vec<(String, String)>,
}

#[derive(Debug)]
pub enum StreamError {
    ParseError,
    IllegalId,
    IdShouldBeHigher,
}

impl StreamVal {
    pub fn id(&self) -> String {
        format!("{}-{}", self.id.0, self.id.1)
    }

    pub fn parse_explicit_id(id: String) -> Result<(u128, u32), StreamError> {
        let (first, second) = id.split_once("-").unwrap();

        match (first.parse(), second.parse()) {
            (Ok(f), Ok(s)) => Ok((f, s)),
            _ => Err(StreamError::ParseError),
        }
    }

    fn parse_auto_generate_sequence_id(
        id: &String,
        key: &String,
        per: &Mutex<StreamPersistence>,
    ) -> Result<(u128, u32), StreamError> {
        match per.lock().unwrap().get_last(&key) {
            Some(last) => {
                let new_id: u128 = id.split("-").next().unwrap().parse().unwrap();

                if new_id == last.id.0 {
                    Ok((new_id, last.id.1 + 1))
                } else {
                    Ok((new_id, 0))
                }
            }
            None => {
                let first = id.split("-").next().unwrap();

                println!("AUTO GENERATED {} ", first);

                if first.parse::<u128>().unwrap() == 0 {
                    Ok((first.parse().unwrap(), 1))
                } else {
                    Ok((first.parse().unwrap(), 0))
                }
            }
        }
    }

    fn auto_generate_id() -> (u128, u32) {
        let cur = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        (cur, 0)
    }

    pub fn parse_id(
        id: &String,
        key: &String,
        per: &Mutex<StreamPersistence>,
    ) -> Result<(u128, u32), StreamError> {
        println!("PARSE ID {}", id);

        match &id {
            val if val.contains("-*") => StreamVal::parse_auto_generate_sequence_id(id, key, per),
            val if val.contains("*") => Ok(StreamVal::auto_generate_id()),
            val if val.contains("-") => StreamVal::parse_explicit_id(id.to_string()),
            _ => Err(StreamError::ParseError),
        }
    }
}

impl Into<RespData> for StreamVal {
    fn into(self) -> RespData {
        let mut data: Vec<RespData> = vec![];
        let mut inside_data: Vec<RespData> = vec![];

        data.push(RespData::BulkString(self.id()));
        for (key, val) in self.pairs {
            inside_data.push(RespData::BulkString(key.to_string()));
            inside_data.push(RespData::BulkString(val.to_string()));
        }

        data.push(RespData::Array(inside_data));

        RespData::Array(data)
    }
}

#[derive(Default)]
pub struct StreamPersistence(pub HashMap<String, Vec<StreamVal>>);

impl StreamPersistence {
    pub fn get_range(
        &self,
        key: String,
        start: String,
        end: String,
        to_end: Option<bool>,
    ) -> Vec<StreamVal> {
        let mut resp_range: Vec<StreamVal> = vec![];

        // TODO : The sequence number doesn't need to be included
        //        in the start and end IDs provided to the command.
        //        If not provided, XRANGE defaults to a sequence number of 0 for
        //        the start and the maximum sequence number for the end.

        let mut add = match to_end {
            Some(v) => v && true,
            None => false,
        };

        for val in self.0.get(&key).unwrap() {
            if val.id() == end {
                add = true;
            }

            if add {
                resp_range.push(val.clone());
            }

            if val.id() == start {
                add = false;
            }
        }

        resp_range.reverse();

        resp_range
    }

    pub fn get_range_to_start(&self, key: String, end: String) -> Vec<StreamVal> {
        let mut resp_range: Vec<StreamVal> = vec![];

        // TODO : The sequence number doesn't need to be included
        //        in the start and end IDs provided to the command.
        //        If not provided, XRANGE defaults to a sequence number of 0 for
        //        the start and the maximum sequence number for the end.

        let mut add = false;

        for val in self.0.get(&key).unwrap() {
            if val.id() == end {
                add = true;
            }

            if add {
                resp_range.push(val.clone());
            }
        }

        resp_range.reverse();

        resp_range
    }

    pub fn xread(&self, key: String, get_id: String) -> Vec<StreamVal> {
        let mut resp_range: Vec<StreamVal> = vec![];
        resp_range.reverse();

        let get_id = StreamVal::parse_explicit_id(get_id).unwrap();

        for val in self.0.get(&key).unwrap() {
            let id = val.id;

            if id.0 > get_id.0 || (id.0 == get_id.0 && id.1 > get_id.1) {
                resp_range.push(val.clone());
            }
        }

        resp_range
    }

    pub fn insert(&mut self, id: &String, val: StreamVal) -> Result<String, StreamError> {
        let return_id = val.id();
        if self.0.contains_key(id) {
            let values: &mut Vec<StreamVal> = self.0.get_mut(id).unwrap();
            let last = values.get(0).unwrap();

            let new_id = val.id;

            if new_id.0 == 0 && new_id.1 == 0 {
                return Err(StreamError::IdShouldBeHigher);
            }

            if new_id.0 > last.id.0 || (new_id.0 == last.id.0 && new_id.1 > last.id.1) {
                values.insert(0, val);

                let values_copy = values.to_vec();

                self.0.insert(id.to_string(), values_copy);
            } else {
                return Err(StreamError::IllegalId);
            }
        } else {
            self.0.insert(id.to_string(), vec![val]);
        }

        Ok(return_id)
    }

    pub fn get_last(&self, id: &String) -> Option<StreamVal> {
        match self.0.get(id) {
            Some(val) => val.get(0).cloned(),
            None => None,
        }
    }
}
