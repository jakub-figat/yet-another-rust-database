use crate::protos::common::{value::Data as ValueData, Value as ProtoValue};
use crate::protos::request::{
    batch_item::Item as BatchItem, request::Data as RequestData, Request as ProtoRequest,
};
use crate::protos::response::{
    response::Data as ProtoResponseData, ClientError, Response as ProtoResponse,
};
use crate::thread_channels::ThreadCommand;
use protobuf::{Message, MessageField};
use storage::Value::{
    self, Boolean, Datetime, Decimal, Float32, Float64, Int32, Int64, Null, Unsigned32, Unsigned64,
    Varchar,
};

pub enum Request {
    Command(ThreadCommand),
    Batch(Vec<ThreadCommand>),
}

pub fn parse_request_from_bytes(buffer: &mut Vec<u8>) -> Result<Request, String> {
    let request = ProtoRequest::parse_from_bytes(&buffer).map_err(|err| err.to_string());
    buffer.truncate(0);

    let parsed_request = request?;
    let request_data = parsed_request
        .data
        .ok_or("Invalid request data".to_string())?;
    match request_data {
        RequestData::Get(get) => {
            let sort_key = parse_value_from_proto(get.sort_key.unwrap());
            Ok(Request::Command(ThreadCommand::Get(get.hash_key, sort_key)))
        }
        RequestData::Insert(insert) => {
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
        RequestData::Delete(delete) => {
            let sort_key = parse_value_from_proto(delete.sort_key.unwrap());
            Ok(Request::Command(ThreadCommand::Delete(
                delete.hash_key,
                sort_key,
            )))
        }
        RequestData::Batch(batch) => {
            let mut commands = Vec::with_capacity(batch.items.len());
            for item in batch.items {
                let command = match item.item.unwrap() {
                    BatchItem::Get(get) => {
                        let sort_key = parse_value_from_proto(get.sort_key.unwrap());
                        Ok::<ThreadCommand, String>(ThreadCommand::Get(get.hash_key, sort_key))
                    }
                    BatchItem::Insert(insert) => {
                        let sort_key = parse_value_from_proto(insert.sort_key.unwrap());
                        let values: Vec<_> = insert
                            .values
                            .into_iter()
                            .map(|value| parse_value_from_proto(value))
                            .collect();
                        Ok(ThreadCommand::Insert(insert.hash_key, sort_key, values))
                    }
                    BatchItem::Delete(delete) => {
                        let sort_key = parse_value_from_proto(delete.sort_key.unwrap());
                        Ok(ThreadCommand::Delete(delete.hash_key, sort_key))
                    }
                }?;
                commands.push(command);
            }
            Ok(Request::Batch(commands))
        }
    }
}

fn parse_value_from_proto(value: ProtoValue) -> Value {
    match value.data.unwrap() {
        ValueData::Varchar(data) => Varchar(data, 1), // TODO
        ValueData::Int32(data) => Int32(data),
        ValueData::Int64(data) => Int64(data),
        ValueData::Unsigned32(data) => Unsigned32(data),
        ValueData::Unsigned64(data) => Unsigned64(data),
        ValueData::Float32(data) => Float32(data),
        ValueData::Float64(data) => Float64(data),
        ValueData::Boolean(data) => Boolean(data),
        ValueData::Decimal(data) => Decimal(data, 1, 1), // TODO
        ValueData::Datetime(data) => Datetime(data),     // TODO
        ValueData::Null(_) => Null,
    }
}

pub fn parse_proto_from_value(value: Value) -> ProtoValue {
    let proto_enum_value = match value {
        Varchar(data, _) => ValueData::Varchar(data), // TODO
        Int32(data) => ValueData::Int32(data),
        Int64(data) => ValueData::Int64(data),
        Unsigned32(data) => ValueData::Unsigned32(data),
        Unsigned64(data) => ValueData::Unsigned64(data),
        Float32(data) => ValueData::Float32(data),
        Float64(data) => ValueData::Float64(data),
        Boolean(data) => ValueData::Boolean(data),
        Decimal(data, _, _) => ValueData::Decimal(data), // TODO
        Datetime(data) => ValueData::Datetime(data),     // TODO
        Null => ValueData::Null(vec![0u8; 0]),
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
