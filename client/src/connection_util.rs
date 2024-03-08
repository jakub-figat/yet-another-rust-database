use crate::Model;
use common::value::Value;
use protos::util::parse_message_field_from_value;
use protos::{DeleteRequest, GetRequest};

pub fn create_get_request<T: Model>(hash_key: String, sort_key: Value) -> GetRequest {
    let mut get_request = GetRequest::new();
    get_request.hash_key = hash_key;
    get_request.sort_key = parse_message_field_from_value(sort_key);
    get_request.table = T::table_name();

    get_request
}

pub fn create_delete_request(hash_key: String, sort_key: Value, table_name: &str) -> DeleteRequest {
    let mut delete_request = DeleteRequest::new();
    delete_request.hash_key = hash_key;
    delete_request.sort_key = parse_message_field_from_value(sort_key);
    delete_request.table = table_name.to_string();

    delete_request
}
