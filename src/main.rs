use std::io::{Write, Read};
use std::thread;
use std::net::{TcpListener, TcpStream};

fn handle_connection(mut stream: TcpStream) {

    loop {

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

        let ping = received.split("\r\n");

        for pinged in ping.into_iter(){
            if pinged.to_lowercase().contains("ping") {
                match stream.write(b"+PONG\r\n") {
                    Ok(size) => {
                        println!("size: {size}");
                    }
                    Err(e) => {
                        println!("error: {}", e);
                    }
                };

            }
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
                thread::spawn(|| {
                    handle_connection(stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
