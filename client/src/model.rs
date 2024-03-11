use protos::{DeleteRequest, GetResponse, InsertRequest};
use storage::table::TableSchema;

pub trait Model {
    fn from_get_response(get_response: GetResponse) -> Self;
    fn to_insert_request(&self) -> InsertRequest;
    fn to_delete_request(&self) -> DeleteRequest;
    fn hash_key(&self) -> String;
    fn table_name() -> String;
    fn table_schema() -> TableSchema;
}
