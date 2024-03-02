use protos::InsertRequest;

// macro: table name, required hash key, optional sort_key?

// generate serialization/deserialization code and table name from the guy above
// methods on trait
// then generate impl with macro?

// migration tools in the future?

pub trait Model {
    fn to_insert_request(&self) -> InsertRequest;
    // fn to_get_request(&self) -> GetRequest;
    // fn to_delete_request(&self) -> DeleteRequest;
    // fn to_batch_request_item(&self) -> BatchItem;
}

// impl Model for User {
//     fn to_insert_request(&self) -> InsertRequest {
//         let mut insert_request = InsertRequest::new();
//         insert_request.hash_key = self.hash_key.clone();
//         insert_request.sort_key = parse_message_field_from_value(Unsigned32(self.sort_key));
//
//         let mut values = Vec::new();
//         values.push(parse_proto_from_value(Varchar(self.username.clone(), 1)));
//         values.push(parse_proto_from_value(Unsigned32(self.age)));
//         insert_request.values = values;
//
//         insert_request.table = "users".to_string();
//         insert_request
//     }
// }
