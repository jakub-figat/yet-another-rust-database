use crate::model::Model;
use crate::pool::ConnectionPool;
use common::partition::get_hash_key_target_partition;
use common::value::Value;
use protobuf::Message;
use protos::util::parse_message_field_from_value;
use protos::{
    DeleteRequest, GetRequest, ProtoRequest, ProtoRequestData, ProtoResponse, ProtoResponseData,
};
use std::collections::HashMap;
use std::net::SocketAddrV4;
use std::sync::Weak;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct Connection {
    pub inner: Option<ConnectionInner>,
    pub pool: Option<Weak<ConnectionPool>>,
}

impl Connection {
    pub async fn new(addr: SocketAddrV4) -> Result<Connection, ConnectionError> {
        Ok(Connection {
            inner: Some(ConnectionInner::new(addr).await?),
            pool: None,
        })
    }

    pub fn new_for_pool(inner: ConnectionInner, pool: Weak<ConnectionPool>) -> Connection {
        Connection {
            inner: Some(inner),
            pool: Some(pool),
        }
    }

    pub async fn get<T: Model>(
        &mut self,
        hash_key: String,
        sort_key: Value,
    ) -> Result<Option<T>, ConnectionError> {
        self.inner.as_mut().unwrap().get(hash_key, sort_key).await
    }

    pub async fn insert<T: Model>(&mut self, instance: T) -> Result<(), ConnectionError> {
        self.inner.as_mut().unwrap().insert(instance).await
    }

    pub async fn delete(
        &mut self,
        hash_key: String,
        sort_key: Value,
        table_name: String,
    ) -> Result<Option<()>, ConnectionError> {
        self.inner
            .as_mut()
            .unwrap()
            .delete(hash_key, sort_key, table_name)
            .await
    }
}

pub struct ConnectionInner {
    streams: HashMap<usize, TcpStream>,
}

#[derive(Debug)]
pub enum ConnectionError {
    Client(String),
    Server(String),
}

impl ConnectionInner {
    pub async fn new(address: SocketAddrV4) -> Result<ConnectionInner, ConnectionError> {
        let mut stream = TcpStream::connect(address).await.map_err(|e| {
            ConnectionError::Client(format!("Failed to connect to server: {}", e.to_string()))
        })?;

        let num_of_threads = stream
            .read_u32()
            .await
            .map_err(|e| ConnectionError::Server(e.to_string()))?;

        let mut session = ConnectionInner {
            streams: HashMap::from([(0, stream)]),
        };

        let starting_port = address.port();
        let last_port = starting_port + num_of_threads as u16;
        for (partition, port) in (starting_port..last_port).enumerate().skip(1) {
            let new_address = SocketAddrV4::new(address.ip().clone(), port);
            let mut stream = TcpStream::connect(new_address).await.map_err(|e| {
                ConnectionError::Client(format!("Failed to connect to server: {}", e.to_string()))
            })?;

            stream
                .read_u32()
                .await
                .map_err(|e| ConnectionError::Server(e.to_string()))?;
            session.streams.insert(partition, stream);
        }

        Ok(session)
    }

    async fn get<T: Model>(
        &mut self,
        hash_key: String,
        sort_key: Value,
    ) -> Result<Option<T>, ConnectionError> {
        let partition = get_hash_key_target_partition(&hash_key, self.streams.len());

        let mut get_request = GetRequest::new();
        get_request.hash_key = hash_key;
        get_request.sort_key = parse_message_field_from_value(sort_key);
        get_request.table = T::table_name();

        let mut request = ProtoRequest::new();
        request.data = Some(ProtoRequestData::Get(get_request));

        let proto_response = self.send_request(request, partition).await?;

        match proto_response.data {
            None => Ok(None),
            Some(proto_response_data) => match proto_response_data {
                ProtoResponseData::Get(get_response) => {
                    Ok(Some(T::from_get_response(get_response)))
                }
                ProtoResponseData::ClientError(client_error) => {
                    Err(ConnectionError::Client(client_error.detail))
                }
                ProtoResponseData::ServerError(server_error) => {
                    Err(ConnectionError::Server(server_error.detail))
                }
                _ => panic!("Invalid proto response type"),
            },
        }
    }

    async fn insert<T: Model>(&mut self, instance: T) -> Result<(), ConnectionError> {
        let partition = get_hash_key_target_partition(&instance.hash_key(), self.streams.len());
        let insert_request = instance.to_insert_request();

        let mut request = ProtoRequest::new();
        request.data = Some(ProtoRequestData::Insert(insert_request));

        let proto_response = self.send_request(request, partition).await?;

        match proto_response.data.unwrap() {
            ProtoResponseData::Insert(insert_response) => match insert_response.okay {
                true => Ok(()),
                false => Err(ConnectionError::Server(String::from("Insert failed"))),
            },
            ProtoResponseData::ClientError(client_error) => {
                Err(ConnectionError::Client(client_error.detail))
            }
            ProtoResponseData::ServerError(server_error) => {
                Err(ConnectionError::Server(server_error.detail))
            }
            _ => panic!("Invalid proto response type"),
        }
    }

    async fn delete(
        &mut self,
        hash_key: String,
        sort_key: Value,
        table_name: String,
    ) -> Result<Option<()>, ConnectionError> {
        let partition = get_hash_key_target_partition(&hash_key, self.streams.len());

        let mut delete_request = DeleteRequest::new();
        delete_request.hash_key = hash_key;
        delete_request.sort_key = parse_message_field_from_value(sort_key);
        delete_request.table = table_name;

        let mut request = ProtoRequest::new();
        request.data = Some(ProtoRequestData::Delete(delete_request));
        let proto_response = self.send_request(request, partition).await?;

        match proto_response.data {
            Some(proto_response_data) => match proto_response_data {
                ProtoResponseData::Delete(delete_response) => match delete_response.okay {
                    true => Ok(Some(())),
                    false => Err(ConnectionError::Client(String::from("Insert failed"))),
                },
                ProtoResponseData::ClientError(client_error) => {
                    Err(ConnectionError::Client(client_error.detail))
                }
                ProtoResponseData::ServerError(server_error) => {
                    Err(ConnectionError::Server(server_error.detail))
                }
                _ => panic!("Invalid proto response type"),
            },
            None => Ok(None),
        }
    }

    async fn send_request(
        &mut self,
        proto_request: ProtoRequest,
        partition: usize,
    ) -> Result<ProtoResponse, ConnectionError> {
        let request_bytes = proto_request.write_to_bytes().unwrap();
        let request_size_prefix = (request_bytes.len() as u32).to_be_bytes();

        let stream = self.streams.get_mut(&partition).unwrap();

        stream
            .write_all(&request_size_prefix)
            .await
            .map_err(|e| ConnectionError::Client(e.to_string()))?;
        stream
            .write_all(&request_bytes)
            .await
            .map_err(|e| ConnectionError::Client(e.to_string()))?;

        let response_size = stream
            .read_u32()
            .await
            .map_err(|e| ConnectionError::Client(e.to_string()))?;
        let mut buffer = vec![0u8; response_size as usize];
        stream
            .read_exact(&mut buffer)
            .await
            .map_err(|e| ConnectionError::Client(e.to_string()))?;

        ProtoResponse::parse_from_bytes(&buffer).map_err(|e| ConnectionError::Client(e.to_string()))
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(pool_ref) = self.pool.take() {
            if let Some(pool) = pool_ref.upgrade() {
                pool.put_back(self.inner.take().unwrap());
            }
        }
    }
}
