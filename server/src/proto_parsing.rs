use crate::thread_channels::ThreadCommand;
use protobuf::{Message, MessageField};
use protos::{
    BatchItemData, ClientError, ProtoRequest, ProtoRequestData, ProtoResponse, ProtoResponseData,
    ProtoValue, ProtoValueData,
};
use storage::Value::{
    self, Boolean, Datetime, Decimal, Float32, Float64, Int32, Int64, Null, Unsigned32, Unsigned64,
    Varchar,
};

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
            Ok(Request::Command(ThreadCommand::Get(get.hash_key, sort_key)))
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
            )))
        }
        ProtoRequestData::Delete(delete) => {
            let sort_key = parse_value_from_proto(delete.sort_key.unwrap());
            Ok(Request::Command(ThreadCommand::Delete(
                delete.hash_key,
                sort_key,
            )))
        }
        ProtoRequestData::Batch(batch) => {
            let mut commands = Vec::with_capacity(batch.items.len());
            for item in batch.items {
                let command = match item.item.unwrap() {
                    BatchItemData::Get(get) => {
                        let sort_key = parse_value_from_proto(get.sort_key.unwrap());
                        Ok::<ThreadCommand, String>(ThreadCommand::Get(get.hash_key, sort_key))
                    }
                    BatchItemData::Insert(insert) => {
                        let sort_key = parse_value_from_proto(insert.sort_key.unwrap());
                        let values: Vec<_> = insert
                            .values
                            .into_iter()
                            .map(|value| parse_value_from_proto(value))
                            .collect();
                        Ok(ThreadCommand::Insert(insert.hash_key, sort_key, values))
                    }
                    BatchItemData::Delete(delete) => {
                        let sort_key = parse_value_from_proto(delete.sort_key.unwrap());
                        Ok(ThreadCommand::Delete(delete.hash_key, sort_key))
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

fn parse_value_from_proto(value: ProtoValue) -> Value {
    match value.data.unwrap() {
        ProtoValueData::Varchar(data) => Varchar(data, 1), // TODO
        ProtoValueData::Int32(data) => Int32(data),
        ProtoValueData::Int64(data) => Int64(data),
        ProtoValueData::Unsigned32(data) => Unsigned32(data),
        ProtoValueData::Unsigned64(data) => Unsigned64(data),
        ProtoValueData::Float32(data) => Float32(data),
        ProtoValueData::Float64(data) => Float64(data),
        ProtoValueData::Boolean(data) => Boolean(data),
        ProtoValueData::Decimal(data) => Decimal(data, 1, 1), // TODO
        ProtoValueData::Datetime(data) => Datetime(data),     // TODO
        ProtoValueData::Null(_) => Null,
        _ => panic!("Invalid proto value type"),
    }
}

pub fn parse_proto_from_value(value: Value) -> ProtoValue {
    let proto_enum_value = match value {
        Varchar(data, _) => ProtoValueData::Varchar(data), // TODO
        Int32(data) => ProtoValueData::Int32(data),
        Int64(data) => ProtoValueData::Int64(data),
        Unsigned32(data) => ProtoValueData::Unsigned32(data),
        Unsigned64(data) => ProtoValueData::Unsigned64(data),
        Float32(data) => ProtoValueData::Float32(data),
        Float64(data) => ProtoValueData::Float64(data),
        Boolean(data) => ProtoValueData::Boolean(data),
        Decimal(data, _, _) => ProtoValueData::Decimal(data), // TODO
        Datetime(data) => ProtoValueData::Datetime(data),     // TODO
        Null => ProtoValueData::Null(vec![0u8; 0]),
    };

    let mut proto_value = ProtoValue::new();
    proto_value.data = Some(proto_enum_value);
    proto_value
}

pub fn parse_message_field_from_value(value: Value) -> MessageField<ProtoValue> {
    let proto_value = parse_proto_from_value(value);
    MessageField(Some(Box::new(proto_value)))
}

pub fn client_error_to_proto_response(error: String) -> ProtoResponse {
    let mut client_response = ClientError::new();
    client_response.detail = error;

    let mut proto_response = ProtoResponse::new();
    proto_response.data = Some(ProtoResponseData::ClientError(client_response));
    proto_response
}
