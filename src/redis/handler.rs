use std::{
    collections::HashMap,
    io::Write,
    net::TcpStream,
    num::ParseIntError,
    sync::{Arc, Mutex},
};

use super::{
    parse::{Resp, RespData},
    server::{Info, Role},
};

pub struct PersistedValue {
    pub data: RespData,
    pub timestamp: std::time::SystemTime,
    pub expiry: u128,
}

pub struct State {
    pub persisted: Persistence,
    pub info: Mutex<Info>,
}

pub type PersistenceArc = Arc<State>;
pub type Persistence = Mutex<HashMap<String, PersistedValue>>;

pub fn write_stream(stream: &mut TcpStream, content: &[u8]) {
    match stream.write(content) {
        Ok(size) => {
            println!("size: {}", size);
        }
        Err(e) => {
            println!("error: {}", e);
        }
    };
}

pub fn handle_ping(stream: &mut TcpStream) {
    write_stream(stream, b"+PONG\r\n");
}

pub fn handle_echo(stream: &mut TcpStream, data: &RespData) {
    write_stream(stream, format!("{}\r\n", data).as_bytes());
}

pub fn handle_set(persistence: &State, stream: &mut TcpStream, vals: &[RespData]) {
    let key = vals.get(1).unwrap().to_string();
    let value = vals.get(2).unwrap();

    let has_expiry = match vals.get(3) {
        Some(val) => match val {
            RespData::BulkString(v) => match v.to_lowercase().as_str() {
                "px" => match vals.get(4).unwrap() {
                    RespData::BulkString(v) => v.parse::<u128>().unwrap(),
                    _ => panic!(),
                },
                _ => 0,
            },
            _ => panic!(),
        },
        None => 0,
    };

    let insert_val = PersistedValue {
        data: value.clone(),
        timestamp: std::time::SystemTime::now(),
        expiry: has_expiry,
    };

    let mut persist = persistence.persisted.lock().unwrap();
    persist.insert(key, insert_val);

    write_stream(stream, format!("+OK\r\n").as_bytes());
}

pub fn handle_get(persistence: &State, stream: &mut TcpStream, vals: &[RespData]) {
    let key = vals.get(1).unwrap().to_string();

    let persist = persistence.persisted.lock().unwrap();
    let value = persist.get(&key).unwrap();

    let now = std::time::SystemTime::now();

    if value.expiry > 0 {
        if now.duration_since(value.timestamp).unwrap().as_millis() > value.expiry {
            write_stream(stream, b"$-1\r\n");
        }
    }

    match &value.data {
        RespData::BulkString(v) => {
            write_stream(stream, format!("+{}\r\n", v).as_bytes());
        }
        _ => panic!(),
    }
}

pub fn handle_info(persistence: &State, stream: &mut TcpStream, vals: &[RespData]) {
    match vals.get(1).unwrap() {
        RespData::BulkString(val) => match val.as_str() {
            "replication" => {
                let response = persistence.info.lock().unwrap().replication();
                write_stream(stream, format!("{}\r\n", response.to_string()).as_bytes());
            }
            _ => {}
        },
        _ => panic!(),
    }
}

pub fn handle_error(stream: &mut TcpStream, msg: &str) {
    let resp = RespData::Error(String::from(msg));
    write_stream(stream, format!("{}\r\n", resp.to_string()).as_bytes());
}

pub fn handle_replconf(_persistence: &State, stream: &mut TcpStream, _vals: &[RespData]) {
    write_stream(stream, b"+OK\r\n");
}

pub fn handle_psync(persistence: &State, stream: &mut TcpStream, _vals: &[RespData]) {
    let role = &persistence.info.lock().unwrap().role;

    let (rep_id, offset) = match role {
        Role::Master(master) => (&master.replication_id, master.offset),
        Role::Slave(_) => {
            handle_error(stream, "Slave can't handle PSYNC");
            return;
        }
    };

    write_stream(
        stream,
        format!("FULLRESYNC {} {}", rep_id, offset).as_bytes(),
    );

    let empty_rdb = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

    let hex: Result<Vec<u8>, ParseIntError> = (0..empty_rdb.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&empty_rdb[i..i + 2], 16))
        .collect();

    let res = hex.unwrap();

    let size = format!("${}\r\n", res.len());

    write_stream(stream, size.as_bytes());
    write_stream(stream, &res);
}

pub fn handle_request(persistence: &State, stream: &mut TcpStream, req: &Resp) {
    match &req.data {
        RespData::Array(vals) => match vals.get(0).unwrap() {
            RespData::BulkString(command) => match command.to_lowercase().as_str() {
                "ping" => handle_ping(stream),
                "echo" => handle_echo(stream, vals.get(1).unwrap()),
                "set" => handle_set(persistence, stream, vals),
                "get" => handle_get(persistence, stream, vals),
                "info" => handle_info(persistence, stream, vals),
                "replconf" => handle_replconf(persistence, stream, vals),
                "psync" => handle_psync(persistence, stream, vals),
                _ => handle_error(stream, "Unexpected command"),
            },
            _ => handle_error(stream, "Unexpected data type"),
        },
        _ => handle_error(stream, "Unexpected data type"),
    }
}
