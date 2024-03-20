mod resp;

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::resp::resp::Resp;

struct PersistedValue {
    pub data: resp::resp::RespData,
    pub timestamp: std::time::SystemTime,
    pub expiry: u128,
}

type PersistenceArc = Arc<Mutex<HashMap<String, PersistedValue>>>;
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

fn handle_echo(stream: &mut TcpStream, data: &resp::resp::RespData) {
    match stream.write(format!("{}\r\n", data).as_bytes()) {
        Ok(size) => {
            println!("size: {}", size);
        }
        Err(e) => {
            println!("error: {}", e);
        }
    };
}

fn handle_set(persistence: &Persistence, stream: &mut TcpStream, vals: &[resp::resp::RespData]) {
    println!("vals_set: {:?}", vals);
    let key = vals.get(1).unwrap().to_string();
    let value = vals.get(2).unwrap();

    let has_expiry = match vals.get(3) {
        Some(val) => match val {
            resp::resp::RespData::BulkString(v) => match v.to_lowercase().as_str() {
                "px" => match vals.get(4).unwrap() {
                    resp::resp::RespData::BulkString(v) => v.parse::<u128>().unwrap(),
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

    let mut persist = persistence.lock().unwrap();
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

fn handle_get(persistence: &Persistence, stream: &mut TcpStream, vals: &[resp::resp::RespData]) {
    let key = vals.get(1).unwrap().to_string();

    let persist = persistence.lock().unwrap();
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
        resp::resp::RespData::BulkString(v) => {
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

fn handle_request(persistence: &Persistence, stream: &mut TcpStream, req: &Resp) {
    match &req.data {
        resp::resp::RespData::Array(vals) => match vals.get(0).unwrap() {
            resp::resp::RespData::BulkString(command) => match command.to_lowercase().as_str() {
                "ping" => handle_ping(stream),
                "echo" => handle_echo(stream, vals.get(1).unwrap()),
                "set" => handle_set(persistence, stream, vals),
                "get" => handle_get(persistence, stream, vals),
                _ => panic!("Unexpected command"),
            },
            _ => panic!("Unexpected data type"),
        },
        _ => panic!("Unexpected data type"),
    }
}

fn handle_connection(mut persistence: &Persistence, mut stream: TcpStream) {
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
    let persist: PersistenceArc = Arc::new(Mutex::new(HashMap::new()));

    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

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
