use crate::thread_channels::ThreadCommand;
use protobuf::Message;
use protos::util::parse_value_from_proto;
use protos::{BatchItemData, ProtoRequest, ProtoRequestData};

#[derive(Debug)]
pub enum Request {
    Command(ThreadCommand),
    Batch(Vec<ThreadCommand>),
}

pub fn parse_request_from_bytes(buffer: &mut Vec<u8>) -> Result<Request, String> {
    let request = ProtoRequest::parse_from_bytes(&buffer).map_err(|err| err.to_string());
    buffer.clear();

    let parsed_request = request?;
    let request_data = parsed_request
        .data
        .ok_or("Invalid request data".to_string())?;
    match request_data {
        ProtoRequestData::Get(get) => {
            let sort_key = parse_value_from_proto(get.sort_key.unwrap());
            Ok(Request::Command(ThreadCommand::Get(
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
            Ok(Request::Command(ThreadCommand::Insert(
                insert.hash_key,
                sort_key,
                values,
                insert.table,
            )))
        }
        ProtoRequestData::Delete(delete) => {
            let sort_key = parse_value_from_proto(delete.sort_key.unwrap());
            Ok(Request::Command(ThreadCommand::Delete(
                delete.hash_key,
                sort_key,
                delete.table,
            )))
        }
        ProtoRequestData::Batch(batch) => {
            let mut commands = Vec::with_capacity(batch.items.len());
            for item in batch.items {
                let command = match item.item.unwrap() {
                    BatchItemData::Get(get) => {
                        let sort_key = parse_value_from_proto(get.sort_key.unwrap());
                        Ok::<ThreadCommand, String>(ThreadCommand::Get(
                            get.hash_key,
                            sort_key,
                            get.table,
                        ))
                    }
                    BatchItemData::Insert(insert) => {
                        let sort_key = parse_value_from_proto(insert.sort_key.unwrap());
                        let values: Vec<_> = insert
                            .values
                            .into_iter()
                            .map(|value| parse_value_from_proto(value))
                            .collect();
                        Ok(ThreadCommand::Insert(
                            insert.hash_key,
                            sort_key,
                            values,
                            insert.table,
                        ))
                    }
                    BatchItemData::Delete(delete) => {
                        let sort_key = parse_value_from_proto(delete.sort_key.unwrap());
                        Ok(ThreadCommand::Delete(
                            delete.hash_key,
                            sort_key,
                            delete.table,
                        ))
                    }
                    _ => panic!("Invalid batch item type"),
                }?;
                commands.push(command);
            }
            Ok(Request::Batch(commands))
        }
        _ => panic!("Invalid proto request data type"),
    }
}
