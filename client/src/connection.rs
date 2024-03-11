use crate::batch::{get_batch_item_hash_key, Batch, GetMany};
use crate::connection_util::{create_delete_request, create_get_request};
use crate::model::Model;
use crate::pool::ConnectionPool;
use crate::transaction::Transaction;
use common::partition::get_hash_key_target_partition;
use common::value::Value;
use protobuf::Message;
use protos::{
    AbortTransaction, BatchRequest, BeginTransaction, CommitTransaction, GetManyRequest,
    ProtoRequest, ProtoRequestData, ProtoResponse, ProtoResponseData,
};
use rand::{thread_rng, Rng};
use std::collections::HashMap;
use std::net::SocketAddrV4;
use std::sync::{Arc, Weak};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::task::JoinSet;

pub struct Connection {
    pub(crate) inner: Arc<Mutex<ConnectionInner>>,
    pub(crate) pool: Option<Weak<ConnectionPool>>,
}

impl Connection {
    pub async fn new(addr: SocketAddrV4) -> Result<Connection, ConnectionError> {
        Ok(Connection {
            inner: Arc::new(Mutex::new(ConnectionInner::new(addr).await?)),
            pool: None,
        })
    }

    pub(crate) fn new_for_pool(
        inner: Arc<Mutex<ConnectionInner>>,
        pool: Weak<ConnectionPool>,
    ) -> Connection {
        Connection {
            inner,
            pool: Some(pool),
        }
    }

    pub async fn get<T: Model>(
        &self,
        hash_key: String,
        sort_key: Value,
    ) -> Result<Option<T>, ConnectionError> {
        self.inner.lock().await.get(hash_key, sort_key, None).await
    }

    pub async fn insert<T: Model>(&self, instance: T) -> Result<(), ConnectionError> {
        self.inner.lock().await.insert(instance, None).await
    }

    pub async fn delete(
        &self,
        hash_key: String,
        sort_key: Value,
        table_name: &str,
    ) -> Result<bool, ConnectionError> {
        self.inner
            .lock()
            .await
            .delete(hash_key, sort_key, table_name, None)
            .await
    }

    pub async fn get_many<T: Model>(
        &self,
        get_many: GetMany<T>,
    ) -> Result<Vec<T>, ConnectionError> {
        self.inner.lock().await.get_many(get_many, None).await
    }

    pub async fn batch<T: Model>(&self, batch: Batch<T>) -> Result<bool, ConnectionError> {
        self.inner.lock().await.batch(batch, None).await
    }

    pub async fn begin_transaction(&self) -> Result<Transaction, ConnectionError> {
        let (transaction_id, coordinator_partition) =
            self.inner.lock().await.begin_transaction().await?;
        Ok(Transaction::new(
            transaction_id,
            coordinator_partition,
            self.inner.clone(),
        ))
    }

    // pub async fn sync_table<T: Model>(&self) -> Result<(), ConnectionError> {
    //
    // }
}

pub(crate) struct ConnectionInner {
    streams: HashMap<usize, Arc<Mutex<TcpStream>>>,
}

#[derive(Debug)]
pub enum ConnectionError {
    Client(String),
    Server(String),
}

impl ConnectionInner {
    pub(crate) async fn new(address: SocketAddrV4) -> Result<ConnectionInner, ConnectionError> {
        let mut stream = TcpStream::connect(address).await.map_err(|e| {
            ConnectionError::Client(format!("Failed to connect to server: {}", e.to_string()))
        })?;

        let num_of_threads = stream
            .read_u32()
            .await
            .map_err(|e| ConnectionError::Server(e.to_string()))?;

        let mut streams = HashMap::from([(0, Arc::new(Mutex::new(stream)))]);

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
            streams.insert(partition, Arc::new(Mutex::new(stream)));
        }

        Ok(ConnectionInner { streams })
    }

    pub(crate) async fn get<T: Model>(
        &self,
        hash_key: String,
        sort_key: Value,
        transaction_id: Option<u64>,
    ) -> Result<Option<T>, ConnectionError> {
        let partition = get_hash_key_target_partition(&hash_key, self.streams.len());
        let get_request = create_get_request(hash_key, sort_key);

        let mut request = ProtoRequest::new();
        request.table = T::table_name();
        request.data = Some(ProtoRequestData::Get(get_request));
        request.transaction_id = transaction_id;

        let proto_response = send_request(self.streams[&partition].clone(), request).await?;

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

    pub(crate) async fn insert<T: Model>(
        &self,
        instance: T,
        transaction_id: Option<u64>,
    ) -> Result<(), ConnectionError> {
        let partition = get_hash_key_target_partition(&instance.hash_key(), self.streams.len());
        let insert_request = instance.to_insert_request();

        let mut request = ProtoRequest::new();
        request.table = T::table_name();
        request.transaction_id = transaction_id;

        request.data = Some(ProtoRequestData::Insert(insert_request));

        let proto_response = send_request(self.streams[&partition].clone(), request).await?;

        match proto_response.data.unwrap() {
            ProtoResponseData::Insert(_) => Ok(()),
            ProtoResponseData::ClientError(client_error) => {
                Err(ConnectionError::Client(client_error.detail))
            }
            ProtoResponseData::ServerError(server_error) => {
                Err(ConnectionError::Server(server_error.detail))
            }
            _ => panic!("Invalid proto response type"),
        }
    }

    pub(crate) async fn delete(
        &self,
        hash_key: String,
        sort_key: Value,
        table_name: &str,
        transaction_id: Option<u64>,
    ) -> Result<bool, ConnectionError> {
        let partition = get_hash_key_target_partition(&hash_key, self.streams.len());

        let mut request = ProtoRequest::new();
        request.table = table_name.to_string();
        request.transaction_id = transaction_id;

        let delete_request = create_delete_request(hash_key, sort_key);
        request.data = Some(ProtoRequestData::Delete(delete_request));

        let proto_response = send_request(self.streams[&partition].clone(), request).await?;

        match proto_response.data.unwrap() {
            ProtoResponseData::Delete(delete_response) => Ok(delete_response.okay),
            ProtoResponseData::ClientError(client_error) => {
                Err(ConnectionError::Client(client_error.detail))
            }
            ProtoResponseData::ServerError(server_error) => {
                Err(ConnectionError::Server(server_error.detail))
            }
            _ => panic!("Invalid proto response type"),
        }
    }

    pub(crate) async fn get_many<T: Model>(
        &self,
        get_many: GetMany<T>,
        transaction_id: Option<u64>,
    ) -> Result<Vec<T>, ConnectionError> {
        if get_many.items.is_empty() {
            return Ok(vec![]);
        }

        let mut item_batches: Vec<_> = (0..self.streams.len()).map(|_| Vec::new()).collect();
        let mut responses = Vec::with_capacity(get_many.items.len());

        for item in get_many.items {
            let partition = get_hash_key_target_partition(&item.hash_key, self.streams.len());
            item_batches[partition].push(item);
        }

        let mut join_set = JoinSet::new();
        for (partition, item_batch) in item_batches
            .into_iter()
            .enumerate()
            .filter(|(_, batch)| !batch.is_empty())
        {
            let mut get_many_request = GetManyRequest::new();
            get_many_request.items = item_batch;

            let mut proto_request = ProtoRequest::new();
            proto_request.table = T::table_name();
            proto_request.transaction_id = transaction_id;

            proto_request.data = Some(ProtoRequestData::GetMany(get_many_request));

            join_set.spawn(send_request(
                self.streams[&partition].clone(),
                proto_request,
            ));
        }

        while let Some(result) = join_set.join_next().await {
            let response = result.unwrap()?;
            match response.data.unwrap() {
                ProtoResponseData::GetMany(get_many_response) => {
                    for response in get_many_response.items {
                        responses.push(T::from_get_response(response));
                    }
                }
                ProtoResponseData::ClientError(client_error) => {
                    Err(ConnectionError::Client(client_error.detail))?
                }
                ProtoResponseData::ServerError(server_error) => {
                    Err(ConnectionError::Server(server_error.detail))?
                }
                _ => panic!("Invalid proto response type"),
            }
        }

        Ok(responses)
    }

    pub(crate) async fn batch<T: Model>(
        &self,
        batch: Batch<T>,
        transaction_id: Option<u64>,
    ) -> Result<bool, ConnectionError> {
        if batch.items.is_empty() {
            return Ok(true);
        }

        let mut item_batches: Vec<_> = (0..self.streams.len()).map(|_| Vec::new()).collect();

        for item in batch.items {
            let hash_key = get_batch_item_hash_key(&item);
            let partition = get_hash_key_target_partition(&hash_key, self.streams.len());
            item_batches[partition].push(item);
        }

        let mut join_set = JoinSet::new();
        for (partition, item_batch) in item_batches
            .into_iter()
            .enumerate()
            .filter(|(_, batch)| !batch.is_empty())
        {
            let mut batch_request = BatchRequest::new();
            batch_request.items = item_batch;

            let mut proto_request = ProtoRequest::new();
            proto_request.table = T::table_name();
            proto_request.transaction_id = transaction_id;

            proto_request.data = Some(ProtoRequestData::Batch(batch_request));

            join_set.spawn(send_request(
                self.streams[&partition].clone(),
                proto_request,
            ));
        }

        while let Some(result) = join_set.join_next().await {
            let response = result.unwrap()?;
            match response.data.unwrap() {
                ProtoResponseData::Batch(batch_response) => {
                    if !batch_response.okay {
                        return Ok(false);
                    }
                }
                ProtoResponseData::ClientError(client_error) => {
                    Err(ConnectionError::Client(client_error.detail))?
                }
                ProtoResponseData::ServerError(server_error) => {
                    Err(ConnectionError::Server(server_error.detail))?
                }
                _ => panic!("Invalid proto response type"),
            }
        }

        Ok(true)
    }

    async fn begin_transaction(&self) -> Result<(u64, usize), ConnectionError> {
        let mut rng = thread_rng();
        let coordinator_partition = rng.gen_range(0..self.streams.len());

        let mut proto_request = ProtoRequest::new();
        proto_request.data = Some(ProtoRequestData::BeginTransaction(BeginTransaction::new()));

        let coordinator_stream = self.streams[&coordinator_partition].clone();
        let proto_response = send_request(coordinator_stream.clone(), proto_request).await?;
        match proto_response.data.unwrap() {
            ProtoResponseData::Transaction(transaction) => {
                Ok((transaction.transaction_id, coordinator_partition))
            }
            ProtoResponseData::ClientError(client_error) => {
                Err(ConnectionError::Client(client_error.detail))
            }
            ProtoResponseData::ServerError(server_error) => {
                Err(ConnectionError::Server(server_error.detail))
            }
            _ => panic!("Invalid proto response type"),
        }
    }

    pub(crate) async fn commit_transaction(
        &self,
        transaction_id: u64,
        coordinator_partition: usize,
    ) -> Result<(), ConnectionError> {
        let mut proto_request = ProtoRequest::new();
        proto_request.transaction_id = Some(transaction_id);
        proto_request.data = Some(ProtoRequestData::CommitTransaction(CommitTransaction::new()));

        let coordinator_stream = self.streams[&coordinator_partition].clone();
        let proto_response = send_request(coordinator_stream.clone(), proto_request).await?;
        handle_transaction_response(proto_response)
    }

    pub(crate) async fn abort_transaction(
        &self,
        transaction_id: u64,
        coordinator_partition: usize,
    ) -> Result<(), ConnectionError> {
        let mut proto_request = ProtoRequest::new();
        proto_request.transaction_id = Some(transaction_id);
        proto_request.data = Some(ProtoRequestData::AbortTransaction(AbortTransaction::new()));

        let coordinator_stream = self.streams[&coordinator_partition].clone();
        let proto_response = send_request(coordinator_stream.clone(), proto_request).await?;
        handle_transaction_response(proto_response)
    }
}

async fn send_request(
    stream: Arc<Mutex<TcpStream>>,
    proto_request: ProtoRequest,
) -> Result<ProtoResponse, ConnectionError> {
    let request_bytes = proto_request.write_to_bytes().unwrap();
    let request_size_prefix = (request_bytes.len() as u32).to_be_bytes();

    let mut stream = stream.try_lock().unwrap();

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

fn handle_transaction_response(proto_response: ProtoResponse) -> Result<(), ConnectionError> {
    match proto_response.data.unwrap() {
        ProtoResponseData::Transaction(_) => Ok(()),
        ProtoResponseData::ClientError(client_error) => {
            Err(ConnectionError::Client(client_error.detail))
        }
        ProtoResponseData::ServerError(server_error) => {
            Err(ConnectionError::Server(server_error.detail))
        }
        _ => panic!("Invalid proto response type"),
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(pool_ref) = self.pool.take() {
            if let Some(pool) = pool_ref.upgrade() {
                pool.put_back(self.inner.clone());
            }
        }
    }
}
