use core::panic;
use std::{
    collections::HashMap,
    io::{Read, Write},
    net::TcpStream,
    sync::{Arc, Mutex},
    vec,
};

use super::parse::{Resp, RespData};

#[derive(Clone, Debug)]
pub struct Master {
    pub replication_id: String,
    pub offset: u64,
    pub slave_ports: Vec<u16>,
    pub slave_stream: HashMap<u16, Arc<Mutex<TcpStream>>>,
}

#[derive(Clone, Debug)]
pub struct Slave {
    pub master_host: String,
    pub master_port: u16,
    pub stream: Arc<Mutex<TcpStream>>,
    pub is_live: bool,
}

#[derive(Clone, Debug)]
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
            slave_ports: vec![],
            slave_stream: HashMap::new(),
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

#[derive(Debug)]
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
            stream: Arc::new(Mutex::new(connection.try_clone().unwrap())),
            is_live: false,
        });

        self.ping(&mut connection).unwrap();

        self.replconf(
            &mut connection,
            vec!["REPLCONF", "listening-port", &self.port.to_string()],
        )
        .unwrap();
        self.replconf(&mut connection, vec!["REPLCONF", "capa", "psync2"])
            .unwrap();
        self.psync(&mut connection, vec!["PSYNC", "?", "-1"])
            .unwrap();
    }

    pub fn is_master(&self) -> bool {
        match self.role {
            Role::Master(_) => true,
            Role::Slave(_) => false,
        }
    }

    pub fn as_slave(&self) -> Option<Slave> {
        match self.role {
            Role::Slave(ref slave) => Some(slave.clone()),
            _ => None,
        }
    }

    fn read_from_stream(connection: &mut TcpStream) -> Result<String, std::io::Error> {
        let mut buf = [0; 2048];
        loop {
            match connection.read(&mut buf) {
                Ok(size) => {
                    if size > 0 {
                        let received = String::from_utf8_lossy(&buf).to_string();
                        return Ok(received);
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn psync(&self, connection: &mut TcpStream, args: Vec<&str>) -> Result<usize, std::io::Error> {
        let mut fields: Vec<RespData> = vec![];

        for arg in args.into_iter() {
            fields.push(RespData::BulkString(arg.to_string()));
        }

        let send = RespData::Array(fields);
        let _ = connection.write(send.to_string().as_bytes());

        match Self::read_from_stream(connection) {
            Ok(received) => {
                let resp = Resp::parse(received).unwrap();

                let mut is_working = false;

                println!("{:?}", resp.data);

                match resp.data {
                    RespData::RequestArray(mut array) => {
                        for i in 0..2 {
                            let req = array.get(i);
                            match req {
                                Some(req) => match req {
                                    RespData::SimpleString(s) => {
                                        if s.contains(&"FULLRESYNC") {
                                            is_working = true;
                                        }
                                    }
                                    RespData::BulkString(s) => {
                                        if s.contains(&"REDIS0011") && is_working {
                                            return Ok(1);
                                        }
                                        is_working = false;
                                    }
                                    RespData::Array(arr) => {
                                        match (arr.get(0), arr.get(1), arr.get(2)) {
                                            (Some(class), Some(comm), Some(idx)) => {
                                                if class
                                                    == &RespData::BulkString("REPLCONF".to_string())
                                                    && comm
                                                        == &RespData::BulkString(
                                                            "GETACK".to_string(),
                                                        )
                                                    && idx == &RespData::BulkString("*".to_string())
                                                {
                                                    let response = RespData::Array(vec![
                                                        RespData::BulkString(
                                                            "REPLCONF".to_string(),
                                                        ),
                                                        RespData::BulkString("ACK".to_string()),
                                                        RespData::BulkString("0".to_string()),
                                                    ]);
                                                    connection
                                                        .write(response.to_string().as_bytes())
                                                        .unwrap();
                                                    is_working = true;
                                                } else {
                                                    is_working = false;
                                                }
                                            }
                                            _ => {}
                                        }
                                    }
                                    _ => {}
                                },
                                None => {
                                    let par =
                                        Resp::parse(Self::read_from_stream(connection).unwrap())
                                            .unwrap();
                                    println!("{:?}", par.data);
                                    array.push(par.data);
                                }
                            }
                        }
                    }
                    _ => panic!("PSYNC received wrong answer"),
                };

                if is_working {
                    return Ok(1);
                }

                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Could not PSYNC",
                ))
            }
            Err(e) => return Err(e),
        }
    }

    fn replconf(
        &self,
        connection: &mut TcpStream,
        args: Vec<&str>,
    ) -> Result<usize, std::io::Error> {
        let mut fields: Vec<RespData> = vec![];

        for arg in args.into_iter() {
            fields.push(RespData::BulkString(arg.to_string()));
        }

        let send = RespData::Array(fields);
        let _ = connection.write(send.to_string().as_bytes());

        Self::read_from_stream(connection).unwrap();
        Ok(1)
    }

    fn ping(&self, connection: &mut TcpStream) -> Result<String, String> {
        match &self.role {
            Role::Slave(_slave) => {
                let data = RespData::Array(vec![RespData::BulkString("ping".to_string())]);
                match connection.write(format!("{}", data.to_string()).as_bytes()) {
                    Ok(_) => {
                        let mut buf = [0; 2024];

                        match connection.read(&mut buf) {
                            Ok(_size) => {
                                let _received = String::from_utf8_lossy(&buf).to_string();
                            }
                            _ => {}
                        }
                        Ok("Server online".to_string())
                    }
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
