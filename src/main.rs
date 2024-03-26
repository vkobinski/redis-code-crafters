mod redis;

use std::collections::HashMap;
use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::{env, thread};

use redis::handler::{handle_request, PersistenceArc, State};
use redis::server::Info;

use crate::redis::parse::Resp;


fn handle_connection(persistence: &State, mut stream: TcpStream) {
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

    let port = Arc::clone(&persist).info.lock().unwrap().port;

    let listener = TcpListener::bind(format!(
        "127.0.0.1:{}",
        port
    ))
    .unwrap();

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
