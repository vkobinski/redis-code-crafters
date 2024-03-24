use core::panic;
use std::{fmt::format, io::{Read, Write}, net::TcpStream};

use super::parse::RespData;

#[derive(Clone)]
struct Master {
    pub replication_id: String,
    pub offset: u64,
    pub slaves_ports: Vec<u16>,
}

#[derive(Clone)]
struct Slave {
    pub master_host: String,
    pub master_port: u16,
}

#[derive(Clone)]
pub enum Role {
    Master(Master),
    Slave(Slave),
}

impl Role {
    fn master_replication(&self) -> (RespData, RespData) {
        match self {
            Role::Master(master) => (
                RespData::BulkString(format!(
                    "master_replid:{}",
                    master.replication_id.to_string()
                )),
                RespData::BulkString(format!("master_repl_offset:{}", master.offset.to_string())),
            ),
            _ => (
                RespData::Error("Slaves don't have a ReplicationId or Offset".to_string()),
                RespData::Error("".to_string()),
            ),
        }
    }
}

impl Default for Role {
    fn default() -> Self {
        Role::Master(Master {
            replication_id: String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"),
            offset: 0,
            slaves_ports: vec![],
        })
    }
}

impl Into<String> for Role {
    fn into(self) -> String {
        match self {
            Role::Master(_) => String::from("role:master"),
            Role::Slave(_) => String::from("role:slave"),
        }
    }
}

pub struct Info {
    pub role: Role,
    pub port: u16,
}

impl Default for Info {
    fn default() -> Self {
        Self {
            role: Default::default(),
            port: 6379,
        }
    }
}

impl Info {
    pub fn slave(&mut self, host: String, port: u16) {

        let mut connection = TcpStream::connect(format!("{}:{}", host.to_string(), port)).unwrap();

        self.role = Role::Slave(Slave {
            master_host: host,
            master_port: port,
        });


        self.ping(&mut connection).unwrap();

        self.replconf(&mut connection, vec!("listening-port", &self.port.to_string())).unwrap();
        self.replconf(&mut connection, vec!("capa", "psync2")).unwrap();

    }

    fn replconf(&self, connection: &mut TcpStream, args: Vec<&str>) -> Result<usize, std::io::Error> {

        let mut fields: Vec<RespData> = vec!();
        fields.push(RespData::BulkString("REPLCONF".to_string()));

        for arg in args.into_iter() {
            fields.push(RespData::BulkString(arg.to_string()));
        };

        let send = RespData::Array(fields);
        println!("{}",send.to_string());

        let mut buf = [0; 1028];

        match connection.read(&mut buf) {
            Ok(size) => {
                if size <= 0 {
                    panic!();
                }
                println!("Received bytes: {}", size);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        };

        let received = String::from_utf8_lossy(&buf);

        connection.write(send.to_string().as_bytes())
    }

    fn ping(&self, connection: &mut TcpStream) -> Result<String, String> {
        match &self.role {
            Role::Slave(slave) => {
                let data = RespData::Array(vec![RespData::BulkString("ping".to_string())]);
                match connection.write(format!("{}", data.to_string()).as_bytes()) {
                    Ok(_) => Ok("Server online".to_string()),
                    Err(_) => Err("Could not ping server!".to_string()),
                }
            }
            Role::Master(_) => Err("Master can't ping!".to_string()),
        }
    }

    pub fn replication(&self) -> RespData {
        let mut fields: Vec<RespData> = vec![];

        fields.push(RespData::BulkString(self.role.clone().into()));

        match self.role {
            Role::Master(_) => {
                let rep = self.role.master_replication();
                fields.push(rep.0);
                fields.push(rep.1);
                let joined_fields = fields
                    .iter()
                    .map(|field| field.to_string())
                    .collect::<Vec<String>>()
                    .join("\r\n");

                RespData::BulkString(joined_fields)
            }
            Role::Slave(_) => self.get_role(),
        }
    }

    pub fn get_role(&self) -> RespData {
        RespData::BulkString(self.role.clone().into())
    }
}
