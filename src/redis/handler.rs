use core::panic;
use std::{
    borrow::BorrowMut,
    io::{Read, Write},
    net::TcpStream,
    num::ParseIntError,
    sync::{Arc, Mutex, RwLock},
};

use super::{
    parse::{Resp, RespData},
    persistence::{
        kv_pair::PersistedValue,
        lib::{PersistedType, PersistenceInner},
        stream::{StreamError, StreamVal},
    },
    server::{Info, Role},
};

pub struct StateInner {
    pub persisted: PersistenceInner,
    pub info: RwLock<Info>,
}

pub type State = Arc<StateInner>;

fn write_stream(stream: &mut TcpStream, content: &[u8]) {
    match stream.write(content) {
        Ok(size) => {
            println!("size: {}", size);
        }
        Err(e) => {
            println!("error: {}", e);
        }
    };
}

fn handle_ping(stream: &mut TcpStream) {
    write_stream(stream, b"+PONG\r\n");
}

fn handle_echo(stream: &mut TcpStream, data: &RespData) {
    write_stream(
        stream,
        &RespData::new_bulk(data.inside_value().unwrap()).as_bytes(),
    );
}

fn propagate(persistence: &State, vals: &[RespData]) {
    let info = persistence.info.read().unwrap();

    match &info.role {
        Role::Master(master) => {
            for slave in &master.slave_ports {
                let mut conn = master.slave_stream.get(&slave).unwrap().lock().unwrap();
                let send = &RespData::Array(vals.to_vec()).as_bytes();
                conn.write(send).unwrap();
            }
        }
        Role::Slave(_) => return,
    };
}

fn handle_set(persistence: &State, stream: &mut TcpStream, vals: &[RespData]) {
    let key = vals.get(1).unwrap().inside_value().unwrap();
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
        data: RespData::new_simple_string(value.inside_value().unwrap()).clone(),
        p_type: PersistedType::String,
        timestamp: std::time::SystemTime::now(),
        expiry: has_expiry,
    };

    let mut persist = persistence.persisted.key_value.lock().unwrap();
    persist.0.insert(key.to_string(), insert_val);

    if persistence.info.read().unwrap().is_master() {
        write_stream(stream, format!("+OK\r\n").as_bytes());
        propagate(persistence, vals);
    }
}

fn handle_xrange(persistence: &State, stream: &mut TcpStream, vals: &[RespData]) {
    let mut vals = vals.iter().skip(1);
    let stream_key = vals.next().unwrap().inside_value().unwrap().to_string();

    let start = vals.next().unwrap().inside_value().unwrap().to_string();
    let end = vals.next().unwrap().inside_value().unwrap().to_string();

    let streams = persistence.persisted.stream.lock().unwrap();

    {
        let range: Vec<StreamVal> = match (start, end) {
            (st, en) if st == "-" => streams.get_range_to_start(stream_key, en),
            (st, en) if en == "+" => streams.get_range(stream_key, st, en, Some(true)),
            (st,en) => streams.get_range(stream_key, st, en, None),
        };

        let map = range.into_iter().map(|val| {
            let resp: RespData = val.into();
            resp
        });

        write_stream(stream, &RespData::Array(map.collect()).as_bytes());
    }
}

fn handle_xadd(persistence: &State, stream: &mut TcpStream, vals: &[RespData]) {
    let mut iter = vals.iter().skip(1);
    let stream_key = iter.next().unwrap().inside_value().unwrap();

    let id = iter.next().unwrap().inside_value().unwrap();
    let insert_id = StreamVal::parse_id(
        &id.to_string(),
        &stream_key.to_string(),
        &persistence.persisted.stream,
    );

    let mut stream_vals: Vec<(String, String)> = vec![];

    for (first, second) in iter.step_by(2).zip(vals.iter().skip(4).step_by(2)) {
        match (first, second) {
            (RespData::BulkString(key), RespData::BulkString(value)) => {
                stream_vals.push((key.to_string(), value.to_string()));
            }
            _ => panic!(),
        };
    }

    let insert_val = StreamVal {
        id: insert_id.unwrap(),
        pairs: stream_vals,
    };

    {
        match persistence
            .persisted
            .stream
            .lock()
            .unwrap()
            .insert(&stream_key.to_string(), insert_val)
        {
            Ok(new_id) => write_stream(stream, &RespData::new_bulk(&new_id).as_bytes()),
            Err(StreamError::IllegalId) => {
                write_stream(stream, "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n".as_bytes());
            }
            Err(StreamError::IdShouldBeHigher) => {
                write_stream(
                    stream,
                    "-ERR The ID specified in XADD must be greater than 0-0\r\n".as_bytes(),
                );
            }
            _ => {
                panic!();
            }
        }
    }
}

fn handle_type(persistence: &State, stream: &mut TcpStream, vals: &[RespData]) {
    let key = vals.get(1).unwrap().inside_value().unwrap();

    match persistence.persisted.stream.lock().unwrap().0.get(key) {
        Some(_) => {
            write_stream(stream, &RespData::new_simple_string("stream").as_bytes());
        }
        None => {}
    };

    match persistence.persisted.key_value.lock().unwrap().0.get(key) {
        Some(val) => {
            match &val.p_type {
                &PersistedType::String => {
                    write_stream(stream, &RespData::new_simple_string("string").as_bytes());
                }
                _ => write_stream(stream, &RespData::new_simple_string("none").as_bytes()),
            };
        }
        None => {
            write_stream(stream, &RespData::new_simple_string("none").as_bytes());
        }
    }
}

fn handle_get(persistence: &State, stream: &mut TcpStream, vals: &[RespData]) {
    let key = vals.get(1).unwrap().inside_value().unwrap();
    println!("KEY: {:?}", key);

    let persist = &persistence.persisted.key_value.lock().unwrap().0;
    let value = match persist.get(&key.to_string()) {
        Some(v) => v,
        None => return,
    };

    let now = std::time::SystemTime::now();

    if value.expiry > 0 {
        if now.duration_since(value.timestamp).unwrap().as_millis() > value.expiry {
            write_stream(stream, b"$-1\r\n");
            return;
        }
    }

    write_stream(stream, &value.data.as_bytes());
}

fn handle_info(persistence: &State, stream: &mut TcpStream, vals: &[RespData]) {
    match vals.get(1).unwrap() {
        RespData::BulkString(val) => match val.as_str() {
            "replication" => {
                let response = persistence.info.read().unwrap().replication();
                write_stream(stream, format!("{}\r\n", response.to_string()).as_bytes());
            }
            _ => {}
        },
        _ => panic!(),
    }
}

fn handle_error(stream: &mut TcpStream, msg: &str) {
    let resp = RespData::Error(String::from(msg));
    write_stream(stream, &resp.as_bytes());
}

fn handle_replconf(persistence: &State, stream: &mut TcpStream, vals: &[RespData]) {
    let port_addr = stream.peer_addr().unwrap().port();

    println!("REPLCONF: {:?}", vals);

    if let RespData::BulkString(command) = &vals[1] {
        match command.to_lowercase().as_str() {
            "getack" => {
                match persistence.info.write().unwrap().role.borrow_mut() {
                    Role::Master(master) => {
                        for slave in &master.slave_stream {
                            let response = RespData::new_bulk_array(&["REPLFCONF", "GETACK", "*"]);

                            let mut slave_stream = slave.1.lock().unwrap();

                            slave_stream.write(&response.as_bytes()).unwrap();

                            let mut buf = [0; 2024];
                            slave_stream.read(&mut buf).unwrap();

                            let received = String::from_utf8_lossy(&buf).to_string();
                            let parse = Resp::parse(received);

                            match parse {
                                Some(resp) => match resp.data {
                                    RespData::Array(vals) => match vals.get(1).unwrap() {
                                        RespData::BulkString(val) => {
                                            let val_under = val.to_lowercase();
                                            if val_under == "ack" {
                                                let _offset = vals.get(2).unwrap();
                                            }
                                        }
                                        _ => {}
                                    },
                                    _ => {}
                                },
                                None => {}
                            }
                        }
                    }
                    Role::Slave(_) => {
                        let response = RespData::new_bulk_array(&["REPLCONF", "ACK", "0"]);

                        stream.write(&response.as_bytes()).unwrap();
                    }
                };
            }
            "listening-port" => {
                if let Role::Master(master) = persistence.info.write().unwrap().role.borrow_mut() {
                    if let Some(RespData::BulkString(_v)) = vals.get(2) {
                        master.slave_ports.push(port_addr);
                        master
                            .slave_stream
                            .insert(port_addr, Arc::new(Mutex::new(stream.try_clone().unwrap())));
                        write_stream(stream, b"+OK\r\n");
                    }
                } else {
                    handle_error(stream, "Slave can't treat REPLCONF");
                }
            }
            _ => {
                write_stream(stream, b"+OK\r\n");
            }
        };
    };
}

fn handle_psync(persistence: &State, stream: &mut TcpStream, _vals: &[RespData]) {
    let role = &persistence.info.read().unwrap().role;

    let (rep_id, offset) = match role {
        Role::Master(master) => (&master.replication_id, master.offset),
        Role::Slave(_) => {
            handle_error(stream, "Slave can't handle PSYNC");
            return;
        }
    };

    write_stream(
        stream,
        &RespData::SimpleString(format!("FULLRESYNC {} {}", rep_id, offset)).as_bytes(),
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
                "xadd" => handle_xadd(persistence, stream, vals),
                "xrange" => handle_xrange(persistence, stream, vals),
                "ping" => handle_ping(stream),
                "echo" => handle_echo(stream, vals.get(1).unwrap()),
                "set" => handle_set(persistence, stream, vals),
                "get" => handle_get(persistence, stream, vals),
                "type" => handle_type(persistence, stream, vals),
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
