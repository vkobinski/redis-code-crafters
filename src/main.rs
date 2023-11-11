use std::net::{TcpListener, TcpStream};
use std::io::Write;

use tokio::stream;

fn handle_connection(mut stream : TcpStream) {

    match stream.write_all(b"+PONG\r\n") {

        Ok(()) => {

        } Err(e) => {
            println!("error: {}", e);

        }
    };
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
