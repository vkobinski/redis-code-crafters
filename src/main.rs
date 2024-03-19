use std::io::{Write, Read};
use std::net::{TcpListener, TcpStream};

fn handle_connection(mut stream: TcpStream) {

    let mut buf = [0; 1028];

    match stream.read(&mut buf) {

        Ok(size) => {
            println!("Received bytes: {}", size);
        }
        Err(e) => {
            println!("error: {}", e);
        }

    };

    let received = String::from_utf8_lossy(&buf);

    let ping = received.split("\n");

    for pinged in ping.into_iter(){
        if pinged.to_lowercase().starts_with("ping") {
            match stream.write(b"+PONG\r\n") {
                Ok(_size) => {
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            };

        }
    }

}

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                handle_connection(stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
