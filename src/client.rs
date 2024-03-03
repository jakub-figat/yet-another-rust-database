use client::Model;
use common::value::Value::Varchar;
use macros::Model as DeriveModel;
use protos::util::{
    parse_message_field_from_value, parse_proto_from_value, parse_value_from_message_field,
    parse_value_from_proto,
};
use protos::{InsertRequest, ProtoResponse, ProtoResponseData};

fn main() {}

#[derive(DeriveModel)]
pub struct User {
    pub hash_key: String,
    pub sort_key: String,
    pub name: String,
    pub last_name: String,
}

// impl User {
//     pub fn from_get_response(proto_response: ProtoResponse) -> Self {
//         let get_response = match proto_response.data.unwrap() {
//             ProtoResponseData::Get(get_response) => get_response,
//             _ => panic!("Invalid proto response type"),
//         };
//         let hash_key = get_response.hash_key.clone();
//
//         let sort_key = match parse_value_from_message_field(get_response.sort_key) {
//             Varchar(sort_key, _) => sort_key,
//             _ => panic!("Invalid value type"),
//         };
//         let name = match parse_value_from_proto(
//             get_response
//                 .values
//                 .get(0)
//                 .expect("No value for field 'name' on index 0")
//                 .clone(),
//         ) {
//             Varchar(name, _) => name,
//             _ => panic!("Invalid value type"),
//         };
//
//         User {
//             hash_key,
//             sort_key,
//             name,
//         }
//     }
// }
