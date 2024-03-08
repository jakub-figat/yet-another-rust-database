use crate::thread_channels::{Command, Operation};
use protobuf::Message;
use protos::util::parse_value_from_proto;
use protos::{BatchItemData, ProtoRequest, ProtoRequestData};

pub fn parse_command_from_bytes(buffer: &mut Vec<u8>) -> Result<Command, String> {
    let request = ProtoRequest::parse_from_bytes(&buffer).map_err(|err| err.to_string());
    buffer.clear();

    let parsed_request = request?;
    let request_data = parsed_request
        .data
        .ok_or("Invalid request data".to_string())?;
    match request_data {
        ProtoRequestData::Get(get) => {
            let sort_key = parse_value_from_proto(get.sort_key.unwrap());
            Ok(Command::Single(Operation::Get(
                get.hash_key,
                sort_key,
                get.table,
            )))
        }
        ProtoRequestData::Insert(insert) => {
            let sort_key = parse_value_from_proto(insert.sort_key.unwrap());
            let values: Vec<_> = insert
                .values
                .into_iter()
                .map(|value| parse_value_from_proto(value))
                .collect();
            Ok(Command::Single(Operation::Insert(
                insert.hash_key,
                sort_key,
                values,
                insert.table,
            )))
        }
        ProtoRequestData::Delete(delete) => {
            let sort_key = parse_value_from_proto(delete.sort_key.unwrap());
            Ok(Command::Single(Operation::Delete(
                delete.hash_key,
                sort_key,
                delete.table,
            )))
        }
        ProtoRequestData::Batch(batch) => {
            let mut operations = Vec::with_capacity(batch.items.len());
            for item in batch.items {
                let operation = match item.item.unwrap() {
                    BatchItemData::Insert(insert) => {
                        let sort_key = parse_value_from_proto(insert.sort_key.unwrap());
                        let values: Vec<_> = insert
                            .values
                            .into_iter()
                            .map(|value| parse_value_from_proto(value))
                            .collect();
                        Ok::<Operation, String>(Operation::Insert(
                            insert.hash_key,
                            sort_key,
                            values,
                            insert.table,
                        ))
                    }
                    BatchItemData::Delete(delete) => {
                        let sort_key = parse_value_from_proto(delete.sort_key.unwrap());
                        Ok(Operation::Delete(delete.hash_key, sort_key, delete.table))
                    }
                    _ => panic!("Invalid batch item type"),
                }?;
                operations.push(operation);
            }
            Ok(Command::Batch(operations))
        }
        _ => panic!("Invalid proto request data type"),
    }
}
