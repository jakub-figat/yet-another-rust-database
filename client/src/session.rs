use crate::model::Model;
use common::partition::get_hash_key_target_partition;
use common::value::Value;
use protobuf::Message;
use protos::util::parse_message_field_from_value;
use protos::{
    DeleteRequest, GetRequest, ProtoRequest, ProtoRequestData, ProtoResponse, ProtoResponseData,
};
use std::collections::HashMap;
use std::net::SocketAddrV4;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

pub struct Session {
    connections: HashMap<usize, Mutex<TcpStream>>,
}

impl Session {
    pub async fn new(address: SocketAddrV4) -> Result<Session, String> {
        let mut stream = TcpStream::connect(address)
            .await
            .map_err(|e| format!("Failed to connect to server: {}", e.to_string()))?;

        let mut buffer = [0u8; 1];
        stream
            .read_exact(&mut buffer)
            .await
            .map_err(|e| e.to_string())?;

        let num_of_threads = buffer[0] as u16;
        let mut session = Session {
            connections: HashMap::from([(0, Mutex::new(stream))]),
        };

        let starting_port = address.port() + 1;
        let last_port = starting_port + num_of_threads;

        for (partition, port) in (starting_port..last_port).enumerate() {
            let new_address = SocketAddrV4::new(address.ip().clone(), port);
            let stream = TcpStream::connect(new_address)
                .await
                .map_err(|e| format!("Failed to connect to server: {}", e.to_string()))?;
            session.connections.insert(partition, Mutex::new(stream));
        }

        Ok(session)
    }

    pub async fn get<T: Model>(
        &mut self,
        hash_key: String,
        sort_key: Value,
    ) -> Result<Option<T>, String> {
        let partition = get_hash_key_target_partition(&hash_key, self.connections.len());

        let mut get_request = GetRequest::new();
        get_request.hash_key = hash_key;
        get_request.sort_key = parse_message_field_from_value(sort_key);
        get_request.table = T::table_name();

        let mut request = ProtoRequest::new();
        request.data = Some(ProtoRequestData::Get(get_request));

        let proto_response = self.send_request(request, partition).await?;

        // TODO: null handling?
        Ok(proto_response
            .data
            .map(|proto_response_data| match proto_response_data {
                ProtoResponseData::Get(get_response) => T::from_get_response(get_response),
                _ => panic!("Invalid proto response type"),
            }))
    }

    pub async fn insert<T: Model>(&mut self, instance: T) -> Result<(), String> {
        let partition = get_hash_key_target_partition(&instance.hash_key(), self.connections.len());
        let insert_request = instance.to_insert_request();

        let mut request = ProtoRequest::new();
        request.data = Some(ProtoRequestData::Insert(insert_request));

        let proto_response = self.send_request(request, partition).await?;

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
        let partition = get_hash_key_target_partition(&hash_key, self.connections.len());

        let mut delete_request = DeleteRequest::new();
        delete_request.hash_key = hash_key;
        delete_request.sort_key = parse_message_field_from_value(sort_key);
        delete_request.table = table_name;

        let mut request = ProtoRequest::new();
        request.data = Some(ProtoRequestData::Delete(delete_request));
        let proto_response = self.send_request(request, partition).await?;

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

    async fn send_request(
        &mut self,
        proto_request: ProtoRequest,
        partition: usize,
    ) -> Result<ProtoResponse, String> {
        let request_bytes = proto_request.write_to_bytes().unwrap();
        let request_size_prefix = (request_bytes.len() as u32).to_be_bytes();

        let mut stream = self.connections[&partition].lock().await;

        stream
            .write_all(&request_size_prefix)
            .await
            .map_err(|e| e.to_string())?;
        stream
            .write_all(&request_bytes)
            .await
            .map_err(|e| e.to_string())?;

        let response_size = stream.read_u32().await.map_err(|e| e.to_string())?;
        let mut buffer = vec![0u8; response_size as usize];
        // TODO: timeout
        stream
            .read_exact(&mut buffer)
            .await
            .map_err(|e| e.to_string())?;

        ProtoResponse::parse_from_bytes(&buffer).map_err(|e| e.to_string())
    }
}
