use crate::row::Value::*;
use get_size::GetSize;
use std::cmp::Ordering;
use std::fmt::Display;
use std::mem::size_of;

#[derive(Clone, Debug)]
pub struct Row {
    pub hash_key: String,
    pub sort_key: Value,
    pub primary_key: String,
    pub values: Vec<Value>,
}

impl Row {
    pub fn new(hash_key: String, sort_key: Value, values: Vec<Value>) -> Row {
        let sort_key_string = sort_key.to_string();
        let mut primary_key = String::with_capacity(hash_key.len() + sort_key_string.len() + 1);

        primary_key.push_str(&hash_key);
        primary_key.push_str(":");
        primary_key.push_str(&sort_key_string);

        Row {
            hash_key,
            sort_key,
            primary_key,
            values,
        }
    }
}

impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        self.primary_key == other.primary_key
    }
}

impl PartialOrd for Row {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.primary_key.partial_cmp(&other.primary_key)
    }
}

impl Default for Row {
    fn default() -> Self {
        Row {
            hash_key: "".to_string(),
            sort_key: Varchar("".to_string(), 0),
            primary_key: "".to_string(),
            values: Vec::new(),
        }
    }
}

impl GetSize for Row {
    fn get_size(&self) -> usize {
        self.hash_key.get_size()
            + self.sort_key.get_size()
            + self.primary_key.get_size()
            + size_of::<Vec<Value>>()
            + self.values.iter().map(|val| val.get_size()).sum::<usize>()
    }
}

#[derive(Clone, Debug)]
pub enum Value {
    Varchar(String, usize),
    Text(String),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Unsigned32(u32),
    Unsigned64(u64),
    Float32(f32),
    Float64(f64),
    Decimal(String, usize, usize),
    Datetime(String),
    Boolean(bool),
    Null,
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            Varchar(val, _) => val.clone(),
            Text(val) => val.clone(),
            Int16(val) => val.to_string(),
            Int32(val) => val.to_string(),
            Int64(val) => val.to_string(),
            Unsigned32(val) => val.to_string(),
            Unsigned64(val) => val.to_string(),
            Float32(val) => val.to_string(),
            Float64(val) => val.to_string(),
            Decimal(val, _, _) => val.clone(),
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
            Varchar(val, _) => val.get_size(),
            Text(val) => val.get_size(),
            Int16(val) => val.get_size(),
            Int32(val) => val.get_size(),
            Int64(val) => val.get_size(),
            Unsigned32(val) => val.get_size(),
            Unsigned64(val) => val.get_size(),
            Float32(val) => val.get_size(),
            Float64(val) => val.get_size(),
            Decimal(val, _, _) => val.get_size(),
            Datetime(val) => val.get_size(),
            Boolean(val) => val.get_size(),
            Null => 0,
        }
    }
}
