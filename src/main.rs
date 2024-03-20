mod resp;

use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::hash::Hash;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use crate::resp::resp::Resp;

type Persistence = Arc<Mutex<HashMap<String, String>>>;

fn handle_connection(mut persistence: &Mutex<HashMap<String,String>>, mut stream: TcpStream) {
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

        match req.data {
            resp::resp::RespData::Array(vals) => match vals.get(0).unwrap() {
                resp::resp::RespData::BulkString(command) => {
                    match command.to_lowercase().as_str() {
                        "ping" => {
                            match stream.write(b"+PONG\r\n") {
                                Ok(size) => {
                                    println!("size: {}", size);
                                }
                                Err(e) => {
                                    println!("error: {}", e);
                                }
                            };
                        }
                        "echo" => {
                            let data = vals.get(1).unwrap();

                            match stream.write(format!("{}\r\n", data).as_bytes()) {
                                Ok(size) => {
                                    println!("size: {}", size);
                                }
                                Err(e) => {
                                    println!("error: {}", e);
                                }
                            };
                        }
                        "set" => {
                            println!("vals_set: {:?}",vals);
                            let key = vals.get(1).unwrap().to_string();
                            let value = match vals.get(2).unwrap() {
                                resp::resp::RespData::BulkString(v) => v,
                                _ => panic!()
                            };

                            let mut persist = persistence.lock().unwrap();
                            persist.insert(key, value.to_string());

                            match stream.write(format!("+OK\r\n").as_bytes()) {
                                Ok(size) => {
                                    println!("size: {}", size);
                                }
                                Err(e) => {
                                    println!("error: {}", e);
                                }
                            };
                        }
                        "get" => {
                            println!("vals: {:?}",vals);
                            let key = vals.get(1).unwrap().to_string();

                            let persist = persistence.lock().unwrap();
                            let value = persist.get(&key).unwrap();

                            match stream.write(format!("+{}\r\n", value).as_bytes()) {
                                Ok(size) => {
                                    println!("size: {}", size);
                                }
                                Err(e) => {
                                    println!("error: {}", e);
                                }
                            };
                        }
                        _ => {
                            panic!("Unexpected command");
                        }
                    }
                }
                _ => {
                    panic!("Unexpected data type");
                }
            },
            _ => {
                panic!("Unexpected data type");
            }
        }

        //for pinged in ping.into_iter(){
        //    if pinged.to_lowercase().contains("ping") {
        //        match stream.write(b"+PONG\r\n") {
        //            Ok(size) => {
        //                println!("size: {size}");
        //            }
        //            Err(e) => {
        //                println!("error: {}", e);
        //            }
        //        };

        //    }
        //}
    }
}

fn main() {

    let mut persist = Arc::new(Mutex::new(HashMap::new()));

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
