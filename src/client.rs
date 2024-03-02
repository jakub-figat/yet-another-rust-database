use client::Model;
use common::value::Value::Varchar;
use macros::Model as DeriveModel;
use protos::util::{parse_message_field_from_value, parse_proto_from_value};
use protos::InsertRequest;

fn main() {}

#[derive(DeriveModel)]
pub struct User {
    pub hash_key: String,
    pub sort_key: String,
    pub name: String,
    pub last_name: String,
}
