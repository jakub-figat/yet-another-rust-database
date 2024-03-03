use crate::model::Model;
use common::value::Value;
use protobuf::Message;
use protos::util::parse_message_field_from_value;
use protos::{
    DeleteRequest, GetRequest, ProtoRequest, ProtoRequestData, ProtoResponse, ProtoResponseData,
};
use std::net::SocketAddrV4;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

static PROTO_RESPONSE_INITIAL_BUFFER_SIZE: usize = 8 * 1024;

pub struct Session {
    stream: TcpStream,
    address: SocketAddrV4,
}

impl Session {
    pub async fn new(address: SocketAddrV4) -> Session {
        let stream = TcpStream::connect(address).await.unwrap();
        Session { address, stream }
    }

    pub async fn get<T: Model>(
        &mut self,
        hash_key: String,
        sort_key: Value,
    ) -> Result<Option<T>, String> {
        let mut get_request = GetRequest::new();
        get_request.hash_key = hash_key;
        get_request.sort_key = parse_message_field_from_value(sort_key);
        get_request.table = T::table_name();

        let mut request = ProtoRequest::new();
        request.data = Some(ProtoRequestData::Get(get_request));

        let proto_response = self.send_request(request).await?;

        // TODO: null handling?
        Ok(proto_response
            .data
            .map(|proto_response_data| match proto_response_data {
                ProtoResponseData::Get(get_response) => T::from_get_response(get_response),
                _ => panic!("Invalid proto response type"),
            }))
    }

    pub async fn insert<T: Model>(&mut self, instance: T) -> Result<(), String> {
        let insert_request = instance.to_insert_request();

        let mut request = ProtoRequest::new();
        request.data = Some(ProtoRequestData::Insert(insert_request));

        let proto_response = self.send_request(request).await?;

        // TODO, error handling?
        match proto_response.data.unwrap() {
            ProtoResponseData::Insert(insert_response) => match insert_response.okay {
                true => Ok(()),
                false => Err(String::from("Insert failed")),
            },
            _ => panic!("Invalid proto response type"),
        }
    }

    pub async fn delete(
        &mut self,
        hash_key: String,
        sort_key: Value,
        table_name: String,
    ) -> Result<Option<()>, String> {
        let mut delete_request = DeleteRequest::new();
        delete_request.hash_key = hash_key;
        delete_request.sort_key = parse_message_field_from_value(sort_key);
        delete_request.table = table_name;

        let mut request = ProtoRequest::new();
        request.data = Some(ProtoRequestData::Delete(delete_request));
        let proto_response = self.send_request(request).await?;

        // TODO, error handling?
        match proto_response.data {
            Some(proto_response_data) => match proto_response_data {
                ProtoResponseData::Delete(delete_response) => match delete_response.okay {
                    true => Ok(Some(())),
                    false => Err(String::from("Insert failed")),
                },
                _ => panic!("Invalid proto response type"),
            },
            None => Ok(None),
        }
    }

    async fn send_request(&mut self, proto_request: ProtoRequest) -> Result<ProtoResponse, String> {
        let request_bytes = proto_request.write_to_bytes().unwrap();
        self.stream
            .write_all(&request_bytes)
            .await
            .map_err(|e| e.to_string())?;

        let mut buffer = vec![0u8; PROTO_RESPONSE_INITIAL_BUFFER_SIZE];
        let mut offset = 0usize;

        loop {
            let bytes_read = self
                .stream
                .read(&mut buffer[offset..])
                .await
                .map_err(|e| e.to_string())?;
            offset += bytes_read;

            if bytes_read == 0 {
                break;
            }
        }

        ProtoResponse::parse_from_bytes(&buffer[..offset]).map_err(|e| e.to_string())
    }
}
