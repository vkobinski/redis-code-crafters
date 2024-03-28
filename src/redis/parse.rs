use std::fmt;

#[derive(Debug, PartialEq)]
pub enum RespType {
    SimpleString,
    Error,
    Integer,
    BulkString,
    Array,
}

impl Into<&str> for RespType {
    fn into(self) -> &'static str {
        match self {
            RespType::SimpleString => "+",
            RespType::Error => "-",
            RespType::Integer => ":",
            RespType::BulkString => "$",
            RespType::Array => "*",
        }
    }
}

impl From<char> for RespType {
    fn from(val: char) -> RespType {
        match val {
            '+' => RespType::SimpleString,
            '-' => RespType::Error,
            ':' => RespType::Integer,
            '$' => RespType::BulkString,
            '*' => RespType::Array,
            _ => panic!("Invalid RESP type"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum RespData {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<RespData>),
    RequestArray(Vec<RespData>),
}

impl fmt::Display for RespData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RespData::SimpleString(s) => write!(
                f,
                "{}{}\r\n",
                <RespType as Into<&str>>::into(RespType::SimpleString),
                s
            ),
            RespData::Error(e) => write!(
                f,
                "{}{}\r\n",
                <RespType as Into<&str>>::into(RespType::Error),
                e
            ),
            RespData::Integer(i) => write!(
                f,
                "{}{}\r\n",
                <RespType as Into<&str>>::into(RespType::Integer),
                i
            ),
            RespData::BulkString(b) => write!(
                f,
                "{}{}\r\n{}",
                <RespType as Into<&str>>::into(RespType::BulkString),
                b.len(),
                b
            ),
            RespData::Array(a) => {
                let mut result = String::new();
                result.push_str(&format!(
                    "{}{}\r\n",
                    <RespType as Into<&str>>::into(RespType::Array),
                    a.len()
                ));
                for data in a {
                    result.push_str(&format!("{}\r\n", data));
                }
                write!(f, "{}", result)
            }
            _ => panic!(),
        }
    }
}

impl RespData {
    pub fn parse(chars: &String, data_type: &RespType) -> Option<RespData> {
        match data_type {
            RespType::SimpleString => RespData::parse_simple_string(chars),
            RespType::Error => RespData::parse_error(chars),
            RespType::Integer => RespData::parse_integer(chars),
            RespType::BulkString => RespData::parse_bulk_string(chars),
            RespType::Array => RespData::parse_array(chars),
        }
    }

    pub fn parse_simple_string(chars: &String) -> Option<RespData> {
        let res = chars.split("\r\n").next().unwrap();
        Some(RespData::SimpleString(res.to_string()))
    }

    pub fn parse_error(_chars: &String) -> Option<RespData> {
        None
    }

    pub fn parse_integer(_chars: &String) -> Option<RespData> {
        None
    }

    pub fn parse_bulk_string(serialized: &String) -> Option<RespData> {
        let mut vals = serialized.split("\r\n");
        let size = vals.next().unwrap().parse::<usize>().unwrap();

        let data = vals
            .next()
            .unwrap()
            .to_string()
            .chars()
            .take(size)
            .collect();

        Some(RespData::BulkString(data))
    }

    pub fn parse_array(serialized: &String) -> Option<RespData> {
        let binding = serialized.chars().skip(1).collect::<String>();

        let mut vals = binding.split("\r\n").into_iter();

        let size = match vals.next().unwrap().parse::<u32>() {
            Ok(size) => size,
            Err(_) => return None,
        };

        let mut array: Vec<RespData> = vec![];

        for _ in 0..size {
            let mut cur = vals.next().unwrap().chars();
            let data_type = cur.next().map(RespType::from).unwrap();

            let data: RespData;

            match data_type {
                RespType::BulkString => {
                    let receive =
                        format!("{}\r\n{}", cur.collect::<String>(), vals.next().unwrap());
                    data = RespData::parse(&receive, &data_type).unwrap();
                }
                _ => {
                    data = RespData::parse(&cur.collect(), &data_type).unwrap();
                }
            }

            array.push(data);
        }

        let mut return_array: Vec<RespData> = vec![];

        let data_type = serialized.chars().next().map(RespType::from).unwrap();

        if data_type == RespType::Array {
            let serialized_string: String = vals.collect::<Vec<&str>>().join("\r\n");
            if let Some(data) = Self::parse(&serialized_string, &data_type) {
                match data {
                    RespData::RequestArray(array) => {
                        return_array.extend(array);
                    }
                    _ => {}
                }
            }
        }

        return_array.push(RespData::Array(array));

        Some(RespData::RequestArray(return_array))
    }

    pub fn inside_value(&self) -> Option<&str> {
        match self {
            RespData::SimpleString(s) => Some(s),
            RespData::Error(e) => Some(e),
            RespData::BulkString(b) => Some(b),
            RespData::Array(_) => None,
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct Resp {
    pub data_type: RespType,
    pub data: RespData,
}

impl Resp {
    pub fn parse(serialized: String) -> Option<Resp> {
        let data_type = serialized.chars().next().map(RespType::from).unwrap();
        let data = RespData::parse(&serialized, &data_type).unwrap();

        Some(Resp { data_type, data })
    }
}
