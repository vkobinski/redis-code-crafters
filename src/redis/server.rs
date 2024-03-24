use std::{fmt::format, io::Write, net::TcpStream};

use super::parse::RespData;

#[derive(Clone)]
pub enum Role {
    Master(String, u64),
    Slave(String, u16),

}

impl Role {

    fn master_replication(&self) -> (RespData, RespData) {
        match self {
            Role::Master(replid, offset) => {
                (RespData::BulkString(format!("master_replid:{}", replid.to_string())), RespData::BulkString(format!("master_repl_offset:{}",offset.to_string())))
            }
            _ => (RespData::Error("Slaves don't have a ReplicationId or Offset".to_string()), RespData::Error("".to_string()))

        }

    }

}

impl Default for Role {
    fn default() -> Self {
        Role::Master(String::from("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"), 0)
    }
}

impl Into<String> for Role {
    fn into(self) -> String {
        match self {
            Role::Master(_,_) => String::from("role:master"),
            Role::Slave(_,_) => String::from("role:slave"),
        }
    }
}

pub struct Info {
    pub role: Role,
    pub port: u16,
}

impl Default for Info {
    fn default() -> Self {
        Self { role: Default::default(), port: 6379 }
    }
}

impl Info {

    pub fn slave(&mut self, host: String, port: u16) {

        println!("{}:{}", host, port);

        self.role = Role::Slave(host, port);
        self.ping().unwrap();

    }

   fn ping(&self) -> Result<String, String> {

        match &self.role {
            Role::Slave(host, port) => {
                let mut connection = TcpStream::connect(format!("{}:{}", host, port)).unwrap();
                let test = RespData::BulkString("ping".to_string());
                let data = RespData::Array(vec!(RespData::BulkString("ping".to_string())));
                println!("{}", data);
                match connection.write(format!("{}", data.to_string()).as_bytes()) {
                    Ok(_) => Ok("Server online".to_string()),
                    Err(_) => Err("Could not ping server!".to_string()),
                }

            },
            Role::Master(_, _) => Err("Master can't ping!".to_string()),
        }


   }

    pub fn replication(&self) -> RespData {
        let mut fields: Vec<RespData> = vec!();

        fields.push(RespData::BulkString(self.role.clone().into()));

        match self.role {
            Role::Master(_, _) => {
                let rep = self.role.master_replication();
                fields.push(rep.0);
                fields.push(rep.1);
                let joined_fields = fields
                    .iter()
                    .map(|field| field.to_string())
                    .collect::<Vec<String>>()
                    .join("\r\n");

                RespData::BulkString(joined_fields)
            },
            Role::Slave(_, _) => self.get_role(),
        }
    }

    pub fn get_role(&self) -> RespData {
        RespData::BulkString(self.role.clone().into())
    }

}