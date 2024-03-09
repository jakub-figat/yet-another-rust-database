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
    Decimal(String),
    Datetime(String),
    Boolean(bool),
    Null,
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
            Decimal(val) => val.clone(),
            Datetime(val) => val.clone(),
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
            Decimal(val) => val.get_size(),
            Datetime(val) => val.get_size(),
            Boolean(val) => val.get_size(),
            Null => 0,
        }
    }
}
