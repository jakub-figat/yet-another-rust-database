use common::value::Value;
use protos::util::parse_message_field_from_value;
use protos::{DeleteRequest, GetRequest};

pub fn create_get_request(hash_key: String, sort_key: Value) -> GetRequest {
    let mut get_request = GetRequest::new();
    get_request.hash_key = hash_key;
    get_request.sort_key = parse_message_field_from_value(sort_key);

    get_request
}

pub fn create_delete_request(hash_key: String, sort_key: Value) -> DeleteRequest {
    let mut delete_request = DeleteRequest::new();
    delete_request.hash_key = hash_key;
    delete_request.sort_key = parse_message_field_from_value(sort_key);

    delete_request
}
