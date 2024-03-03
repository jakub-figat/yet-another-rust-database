use client::Model;
use common::value::Value::Varchar;
use macros::DatabaseModel;
use protos::util::{
    parse_message_field_from_value, parse_proto_from_value, parse_value_from_message_field,
    parse_value_from_proto,
};
use protos::{DeleteRequest, GetResponse, InsertRequest};

fn main() {
    let user = User {
        hash_key: "1".to_string(),
        sort_key: "2".to_string(),
        name: "3".to_string(),
        last_name: "4".to_string(),
    };
}

#[derive(DatabaseModel)]
pub struct User {
    pub hash_key: String,
    pub sort_key: String,
    pub name: String,
    pub last_name: String,
}
