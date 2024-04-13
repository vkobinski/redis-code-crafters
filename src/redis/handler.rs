use std::{
    borrow::BorrowMut, collections::HashMap, io::{Read, Write}, net::TcpStream, num::ParseIntError,sync::{Arc, Mutex, RwLock}
};

use super::{
    parse::{Resp, RespData},
    server::{Info, Role},
};

#[derive(Debug)]
pub struct PersistedValue {
    pub data: RespData,
    pub timestamp: std::time::SystemTime,
    pub expiry: u128,
}

pub struct State {
    pub persisted: Persistence,
    pub info: RwLock<Info>,
}

pub type PersistenceArc = Arc<State>;
pub type Persistence = Mutex<HashMap<String, PersistedValue>>;

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
    write_stream(stream, format!("{}\r\n", data).as_bytes());
}

fn propagate(persistence: &State, vals: &[RespData]) {
    let info = persistence.info.read().unwrap();

    match &info.role {
        Role::Master(master) => {
            for slave in &master.slave_ports {
                let mut conn = master.slave_stream.get(&slave).unwrap().lock().unwrap();
                let send = RespData::Array(vals.to_vec()).to_string();
                conn.write(send.as_bytes()).unwrap();
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
        data: RespData::SimpleString(value.inside_value().unwrap().to_string()).clone(),
        timestamp: std::time::SystemTime::now(),
        expiry: has_expiry,
    };

    let mut persist = persistence.persisted.lock().unwrap();
    persist.insert(key.to_string(), insert_val);

    if persistence.info.read().unwrap().is_master() {
        write_stream(stream, format!("+OK\r\n").as_bytes());
        propagate(persistence, vals);
    }
}

fn handle_type(persistence: &State, stream: &mut TcpStream, vals: &[RespData]) {
    let key = vals.get(1).unwrap().inside_value().unwrap();

    println!("TYPE");

    match persistence.persisted.lock().unwrap().get(key) {
        Some(val) => {
            match &val.data {
                RespData::SimpleString(_) => {
                    write_stream(stream, RespData::new_simple_string("string").to_string().as_bytes());
                },
                _ => {
                    println!("{:?}", val);

                },

            };
        },
        None => {
            write_stream(stream, RespData::new_simple_string("none").to_string().as_bytes());
        },
    }
}

fn handle_get(persistence: &State, stream: &mut TcpStream, vals: &[RespData]) {
    let key = vals.get(1).unwrap().inside_value().unwrap();
    println!("KEY: {:?}", key);

    let persist = persistence.persisted.lock().unwrap();
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

    write_stream(stream, &value.data.to_string().as_bytes());
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
    write_stream(stream, format!("{}\r\n", resp.to_string()).as_bytes());
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

                            slave_stream.write(response.to_string().as_bytes()).unwrap();

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

                        stream.write(response.to_string().as_bytes()).unwrap();
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
        RespData::SimpleString(format!("FULLRESYNC {} {}", rep_id, offset))
            .to_string()
            .as_bytes(),
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
