use crate::connection_util::{create_delete_request, create_get_request};
use crate::Model;
use common::value::Value;
use protos::{BatchItem, BatchItemData, GetRequest};
use std::marker::PhantomData;

pub struct Batch {
    pub items: Vec<BatchItem>,
}

impl Batch {
    pub fn insert<T: Model>(&mut self, instance: T) {
        let insert_request = instance.to_insert_request();
        let mut batch_item = BatchItem::new();
        batch_item.item = Some(BatchItemData::Insert(insert_request));
        self.items.push(batch_item);
    }

    pub fn delete(&mut self, hash_key: String, sort_key: Value, table_name: &str) {
        let delete_request = create_delete_request(hash_key, sort_key, table_name);
        let mut batch_item = BatchItem::new();
        batch_item.item = Some(BatchItemData::Delete(delete_request));
        self.items.push(batch_item);
    }
}

pub fn get_batch_item_hash_key(batch_item: &BatchItem) -> String {
    match batch_item.item.as_ref().unwrap() {
        BatchItemData::Insert(insert) => insert.hash_key.clone(),
        BatchItemData::Delete(delete) => delete.hash_key.clone(),
        _ => panic!("Invalid batch response data type"),
    }
}

pub struct GetMany<T: Model> {
    pub items: Vec<GetRequest>,
    _phantom_data: PhantomData<T>,
}

impl<T: Model> GetMany<T> {
    pub fn add(&mut self, hash_key: String, sort_key: Value) {
        self.items.push(create_get_request::<T>(hash_key, sort_key))
    }
}
