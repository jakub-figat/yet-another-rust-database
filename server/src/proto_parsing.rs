use crate::thread_channels::{Command, Operation};
use protobuf::Message;
use protos::util::parse_value_from_proto;
use protos::{BatchItemData, ProtoRequest, ProtoRequestData};
use std::collections::HashMap;

pub fn parse_request_from_bytes(buffer: &mut Vec<u8>) -> Result<ProtoRequest, String> {
    let request = ProtoRequest::parse_from_bytes(&buffer).map_err(|err| err.to_string());
    buffer.clear();

    request
}

pub fn parse_command_from_request(request: ProtoRequest) -> Result<Command, String> {
    let request_data = request.data.ok_or("Invalid request data".to_string())?;

    match request_data {
        ProtoRequestData::Get(get) => {
            let sort_key = parse_value_from_proto(get.sort_key.unwrap());
            Ok(Command::Single(
                Operation::Get(get.hash_key, sort_key),
                request.table,
            ))
        }
        ProtoRequestData::Insert(insert) => {
            let sort_key = parse_value_from_proto(insert.sort_key.unwrap());
            let values: HashMap<_, _> = insert
                .values
                .into_iter()
                .map(|(key, value)| (key, parse_value_from_proto(value)))
                .collect();
            Ok(Command::Single(
                Operation::Insert(insert.hash_key, sort_key, values),
                request.table,
            ))
        }
        ProtoRequestData::Delete(delete) => {
            let sort_key = parse_value_from_proto(delete.sort_key.unwrap());
            Ok(Command::Single(
                Operation::Delete(delete.hash_key, sort_key),
                request.table,
            ))
        }
        ProtoRequestData::GetMany(get_many) => {
            let operations: Vec<_> = get_many
                .items
                .into_iter()
                .map(|get| {
                    let sort_key = parse_value_from_proto(get.sort_key.unwrap());
                    Operation::Get(get.hash_key, sort_key)
                })
                .collect();
            Ok(Command::GetMany(operations, request.table))
        }
        ProtoRequestData::Batch(batch) => {
            let mut operations = Vec::with_capacity(batch.items.len());
            for item in batch.items {
                let operation = match item.item.unwrap() {
                    BatchItemData::Insert(insert) => {
                        let sort_key = parse_value_from_proto(insert.sort_key.unwrap());
                        let values: HashMap<_, _> = insert
                            .values
                            .into_iter()
                            .map(|(key, value)| (key, parse_value_from_proto(value)))
                            .collect();
                        Ok::<Operation, String>(Operation::Insert(
                            insert.hash_key,
                            sort_key,
                            values,
                        ))
                    }
                    BatchItemData::Delete(delete) => {
                        let sort_key = parse_value_from_proto(delete.sort_key.unwrap());
                        Ok(Operation::Delete(delete.hash_key, sort_key))
                    }
                    _ => panic!("Invalid batch item type"),
                }?;
                operations.push(operation);
            }
            Ok(Command::Batch(operations, request.table))
        }
        ProtoRequestData::BeginTransaction(_) => Ok(Command::BeginTransaction),
        ProtoRequestData::CommitTransaction(_) => Ok(Command::CommitTransaction),
        ProtoRequestData::AbortTransaction(_) => Ok(Command::AbortTransaction),
        _ => panic!("Invalid proto request data type"),
    }
}
