use super::parse::{Resp, RespData};

#[derive(Default, Clone)]
pub enum Role {
    #[default]
    Master,
    Slave(String, u16),
}

impl Into<String> for Role {
    fn into(self) -> String {
        match self {
            Role::Master => String::from("role:master"),
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

    pub fn get_info(&self) -> RespData {
        let mut fields: Vec<RespData> = vec!();

        fields.push(RespData::BulkString(self.role.clone().into()));
        fields.push(RespData::BulkString(self.port.to_string()));

        RespData::Array(fields)

    }

    pub fn get_role(&self) -> RespData {
        RespData::BulkString(self.role.clone().into())
    }

}