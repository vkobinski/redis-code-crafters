mod redis;

use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, RwLock};
use std::{env, thread};

use redis::handler::{handle_request,State, StateInner};
use redis::persistence::lib::PersistenceInner;
use redis::server::Info;

use crate::redis::parse::{Resp, RespData};

fn handle_connection(persistence: &State, mut stream: TcpStream) {
    loop {
        let mut buf = [0; 1024];

        match stream.read(&mut buf) {
            Ok(size) => {
                if size <= 0 {
                    continue;
                }
                println!("Received bytes: {}", size);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        };

        let received = String::from_utf8_lossy(&buf);
        let req = Resp::parse(received.to_string()).expect("Could not parse request");

        match req.data {
            RespData::RequestArray(array) => {
                for req in array {
                    let resp = Resp {
                        data_type: redis::parse::RespType::Array,
                        data: req,
                    };
                    handle_request(persistence, &mut stream, &resp);
                }
            }
            _ => {
                println!("Received: {:?}", req);
                handle_request(persistence, &mut stream, &req);
            }
        }
    }
}

fn handle_connection_slave(persistence: &State, stream: Arc<Mutex<TcpStream>>) {
    println!("SLAVE HANDLING CONNECTIONS!");

    loop {
        let mut conn = stream.lock().unwrap();

        loop {
            let mut buf = [0; 1024];
            match conn.read(&mut buf) {
                Ok(size) => {
                    if size <= 0 {
                        continue;
                    }

                    println!("Handling request");
                    let received = &String::from_utf8_lossy(&buf[..size]);
                    println!("Received: {:?}", received);

                    let req = Resp::parse(received.to_string()).unwrap();

                    match req.data {
                        RespData::RequestArray(array) => {
                            for req in array {
                                let resp = Resp {
                                    data_type: redis::parse::RespType::Array,
                                    data: req,
                                };

                                let send_stream = Arc::clone(persistence);
                                let mut stream = conn.try_clone().unwrap();

                                println!("Handling request: {:?}", resp);

                                thread::spawn(move || {
                                    handle_request(&send_stream, &mut stream, &resp);
                                });
                            }
                        }
                        _ => {
                            println!("Received: {:?}", req);
                        }
                    }
                }
                Err(e) => {
                    println!("error: {}", e);
                    break;
                }
            };
        }
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

    let persist: State = Arc::new(StateInner {
        persisted: PersistenceInner::default(),
        info: RwLock::new(server),
    });

    let port = Arc::clone(&persist).info.read().unwrap().port;

    let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();

    {
        match persist.info.read().unwrap().as_slave() {
            Some(slave) => {
                let persist = Arc::clone(&persist);
                thread::spawn(move || {
                    handle_connection_slave(&persist, slave.stream);
                });
            }
            None => {}
        }
    }

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
