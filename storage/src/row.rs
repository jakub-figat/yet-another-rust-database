use common::value::Value;
use common::value::Value::Varchar;
use get_size::GetSize;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::mem::size_of;

#[derive(Clone, Debug)]
pub struct Row {
    pub hash_key: String,
    pub sort_key: Value,
    pub primary_key: String,
    pub values: HashMap<String, Value>,
    pub table: String,
}

impl Row {
    pub fn new(
        hash_key: String,
        sort_key: Value,
        values: HashMap<String, Value>,
        table: String,
    ) -> Row {
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
            table,
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
            values: HashMap::new(),
            table: String::new(),
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
