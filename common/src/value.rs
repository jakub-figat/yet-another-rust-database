use crate::value::Value::*;
use get_size::GetSize;
use std::fmt::Display;

#[derive(Clone, Debug)]
pub enum Value {
    Varchar(String),
    Int32(i32),
    Int64(i64),
    Unsigned32(u32),
    Unsigned64(u64),
    Float32(f32),
    Float64(f64),
    Boolean(bool),
    Null,
}

impl Value {
    pub fn to_bytes(self) -> Vec<u8> {
        match self {
            Varchar(value) => value.into_bytes(),
            Int32(value) => value.to_be_bytes().to_vec(),
            Int64(value) => value.to_be_bytes().to_vec(),
            Unsigned32(value) => value.to_be_bytes().to_vec(),
            Unsigned64(value) => value.to_be_bytes().to_vec(),
            Float32(value) => value.to_be_bytes().to_vec(),
            Float64(value) => value.to_be_bytes().to_vec(),
            Boolean(value) => {
                if value {
                    return Vec::from([1u8]);
                }
                Vec::from([0u8])
            }
            Null => Vec::with_capacity(0),
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Varchar(val) => val.clone(),
            Int32(val) => val.to_string(),
            Int64(val) => val.to_string(),
            Unsigned32(val) => val.to_string(),
            Unsigned64(val) => val.to_string(),
            Float32(val) => val.to_string(),
            Float64(val) => val.to_string(),
            Boolean(val) => val.to_string(),
            Null => "".to_string(),
        };
        write!(f, "{}", str)
    }
}

impl GetSize for Value {
    fn get_size(&self) -> usize {
        match self {
            Varchar(val) => val.get_size(),
            Int32(val) => val.get_size(),
            Int64(val) => val.get_size(),
            Unsigned32(val) => val.get_size(),
            Unsigned64(val) => val.get_size(),
            Float32(val) => val.get_size(),
            Float64(val) => val.get_size(),
            Boolean(val) => val.get_size(),
            Null => 0,
        }
    }
}
