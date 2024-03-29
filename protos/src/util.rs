use crate::{ClientError, ProtoResponse, ProtoResponseData, ProtoValue, ProtoValueData};
use common::value::Value;
use common::value::Value::*;
use protobuf::MessageField;

pub fn parse_value_from_proto(value: ProtoValue) -> Value {
    match value.data {
        Some(data) => match data {
            ProtoValueData::Varchar(data) => Varchar(data),
            ProtoValueData::Int32(data) => Int32(data),
            ProtoValueData::Int64(data) => Int64(data),
            ProtoValueData::Unsigned32(data) => Unsigned32(data),
            ProtoValueData::Unsigned64(data) => Unsigned64(data),
            ProtoValueData::Float32(data) => Float32(data),
            ProtoValueData::Float64(data) => Float64(data),
            ProtoValueData::Boolean(data) => Boolean(data),
        },
        None => Null,
    }
}

pub fn parse_proto_from_value(value: Value) -> ProtoValue {
    let proto_enum_value = match value {
        Varchar(data) => Some(ProtoValueData::Varchar(data)),
        Int32(data) => Some(ProtoValueData::Int32(data)),
        Int64(data) => Some(ProtoValueData::Int64(data)),
        Unsigned32(data) => Some(ProtoValueData::Unsigned32(data)),
        Unsigned64(data) => Some(ProtoValueData::Unsigned64(data)),
        Float32(data) => Some(ProtoValueData::Float32(data)),
        Float64(data) => Some(ProtoValueData::Float64(data)),
        Boolean(data) => Some(ProtoValueData::Boolean(data)),
        Null => None,
    };

    let mut proto_value = ProtoValue::new();
    proto_value.data = proto_enum_value;
    proto_value
}

pub fn parse_message_field_from_value(value: Value) -> MessageField<ProtoValue> {
    let proto_value = parse_proto_from_value(value);
    MessageField(Some(Box::new(proto_value)))
}

pub fn parse_value_from_message_field(message_field: MessageField<ProtoValue>) -> Value {
    parse_value_from_proto(message_field.unwrap())
}

pub fn client_error_to_proto_response(error: String) -> ProtoResponse {
    let mut client_response = ClientError::new();
    client_response.detail = error;

    let mut proto_response = ProtoResponse::new();
    proto_response.data = Some(ProtoResponseData::ClientError(client_response));
    proto_response
}
