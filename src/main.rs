mod redis;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::{env, thread};

use redis::server::Info;

use crate::redis::parse::Resp;

struct PersistedValue {
    pub data: redis::parse::RespData,
    pub timestamp: std::time::SystemTime,
    pub expiry: u128,
}

struct State {
    pub persisted: Persistence,
    pub info: Mutex<Info>,
}

type PersistenceArc = Arc<State>;
type Persistence = Mutex<HashMap<String, PersistedValue>>;

fn handle_ping(stream: &mut TcpStream) {
    match stream.write(b"+PONG\r\n") {
        Ok(size) => {
            println!("size: {}", size);
        }
        Err(e) => {
            println!("error: {}", e);
        }
    };
}

fn handle_echo(stream: &mut TcpStream, data: &redis::parse::RespData) {
    match stream.write(format!("{}\r\n", data).as_bytes()) {
        Ok(size) => {
            println!("size: {}", size);
        }
        Err(e) => {
            println!("error: {}", e);
        }
    };
}

fn handle_set(persistence: &State, stream: &mut TcpStream, vals: &[redis::parse::RespData]) {
    println!("vals_set: {:?}", vals);
    let key = vals.get(1).unwrap().to_string();
    let value = vals.get(2).unwrap();

    let has_expiry = match vals.get(3) {
        Some(val) => match val {
            redis::parse::RespData::BulkString(v) => match v.to_lowercase().as_str() {
                "px" => match vals.get(4).unwrap() {
                    redis::parse::RespData::BulkString(v) => v.parse::<u128>().unwrap(),
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

    match stream.write(format!("+OK\r\n").as_bytes()) {
        Ok(size) => {
            println!("size: {}", size);
        }
        Err(e) => {
            println!("error: {}", e);
        }
    };
}

fn handle_get(persistence: &State, stream: &mut TcpStream, vals: &[redis::parse::RespData]) {
    let key = vals.get(1).unwrap().to_string();

    let persist = persistence.persisted.lock().unwrap();
    let value = persist.get(&key).unwrap();

    let now = std::time::SystemTime::now();

    if value.expiry > 0 {
        if now.duration_since(value.timestamp).unwrap().as_millis() > value.expiry {
            match stream.write(b"$-1\r\n") {
                Ok(size) => {
                    println!("size: {}", size);
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            };
            return;
        }
    }

    match &value.data {
        redis::parse::RespData::BulkString(v) => {
            match stream.write(format!("+{}\r\n", v).as_bytes()) {
                Ok(size) => {
                    println!("size: {}", size);
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            };
        }
        _ => panic!(),
    }
}

fn handle_info(
    persistence: &State,
    stream: &mut TcpStream,
    vals: &[redis::parse::RespData],
) {
    match vals.get(1).unwrap() {
        redis::parse::RespData::BulkString(val) => match val.as_str() {
            "replication" => {
                let response = persistence.info.lock().unwrap().replication();

                match stream.write(format!("{}\r\n", response.to_string()).as_bytes()) {
                    Ok(size) => {
                        println!("size: {}", size);
                    }
                    Err(e) => {
                        println!("error: {}", e);
                    }
                };
            }
            _ => {}
        },
        _ => panic!(),
    }
}

fn handle_error(stream: &mut TcpStream, req: &Resp, msg: &str) {
    let resp = redis::parse::RespData::Error(String::from(msg));
    match stream.write(format!("{}\r\n", resp.to_string()).as_bytes()) {
        Ok(size) => {
            println!("size: {}", size);
        }
        Err(e) => {
            println!("error: {}", e);
        }
    };
}

fn handle_request(persistence: &State, stream: &mut TcpStream, req: &Resp) {
    match &req.data {
        redis::parse::RespData::Array(vals) => match vals.get(0).unwrap() {
            redis::parse::RespData::BulkString(command) => match command.to_lowercase().as_str() {
                "ping" => handle_ping(stream),
                "echo" => handle_echo(stream, vals.get(1).unwrap()),
                "set" => handle_set(persistence, stream, vals),
                "get" => handle_get(persistence, stream, vals),
                "info" => handle_info(persistence, stream, vals),
                _ => handle_error(stream, req, "Unexpected command"),
            },
            _ => handle_error(stream, req, "Unexpected data type"),
        },
        _ => handle_error(stream, req, "Unexpected data type"),
    }
}

fn handle_connection(mut persistence: &State, mut stream: TcpStream) {
    loop {
        let mut buf = [0; 1028];

        match stream.read(&mut buf) {
            Ok(size) => {
                if size <= 0 {
                    return;
                }
                println!("Received bytes: {}", size);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        };

        let received = String::from_utf8_lossy(&buf);
        let req = Resp::parse(received.to_string()).expect("Could not parse request");
        handle_request(persistence, &mut stream, &req);
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let mut server: Info = redis::server::Info::default();

    for (i, arg) in args.iter().enumerate() {
        match arg.as_str() {
            "--port" => {
                server.port = args.get(i + 1).unwrap().parse::<u16>().unwrap();
            }
            "--replicaof" => {
                let master_host = args.get(i + 1).unwrap();
                let master_port = args.get(i + 2).unwrap().parse::<u16>().unwrap();
                server.slave(master_host.to_string(), master_port);
            }
            _ => {}
        }
    }

    let persist: PersistenceArc = Arc::new(State {
        persisted: Mutex::new(HashMap::new()),
        info: Mutex::new(server),
    });

    let listener = TcpListener::bind(format!("127.0.0.1:{}", Arc::clone(&persist).info.lock().unwrap().port)).unwrap();
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                let persist = Arc::clone(&persist);
                thread::spawn(move || {
                    handle_connection(&persist, stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
