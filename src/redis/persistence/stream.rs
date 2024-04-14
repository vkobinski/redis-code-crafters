use std::collections::HashMap;

#[derive(Clone)]
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

    fn parse_explicit_id(id: String) -> Result<(u128, u32), StreamError> {
        let (first, second) = id.split_once("-").unwrap();

        match (first.parse(), second.parse()) {
            (Ok(f), Ok(s)) => Ok((f, s)),
            _ => Err(StreamError::ParseError),
        }
    }

    fn parse_auto_generate_sequence_id() -> Result<(u128, u32), StreamError> {
        todo!()
    }

    fn auto_generate_id() -> (u128, u32) {
        todo!()
    }

    pub fn parse_id(id: &String) -> Result<(u128, u32), StreamError> {
        match &id {
            val if val.contains("-*") => Ok(StreamVal::auto_generate_id()),
            val if val.contains("*") => StreamVal::parse_auto_generate_sequence_id(),
            val if val.contains("-") => StreamVal::parse_explicit_id(id.to_string()),
            _ => Err(StreamError::ParseError),
        }
    }
}

#[derive(Default)]
pub struct StreamPersistence(pub HashMap<String, Vec<StreamVal>>);

impl StreamPersistence {
    pub fn insert(&mut self, id: &String, val: StreamVal) -> Result<String, StreamError> {
        if self.0.contains_key(id) {
            let values: &mut Vec<StreamVal> = self.0.get_mut(id).unwrap();
            let last = values.get(0).unwrap();

            let new_id = val.id;

            println!("{:?}",new_id);

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

        Ok(id.to_string())
    }

    pub fn get_last(&self, id: &String) -> Option<StreamVal> {
        self.0.get(id).unwrap().get(0).cloned()
    }

}
