use crate::proto_parsing::parse_command_from_bytes;
use crate::thread_channels::Operation::{Delete, Get, Insert};
use crate::thread_channels::{
    send_operations, Command, Operation, OperationResponse, OperationSender, Response,
};
use common::partition::get_hash_key_target_partition;
use futures::channel::oneshot;
use futures::lock::Mutex;
use futures::SinkExt;
use monoio::io::{AsyncReadRentExt, AsyncWriteRentExt};
use monoio::net::TcpStream;
use protobuf::Message;
use protos::util::client_error_to_proto_response;
use protos::{ProtoResponse, ProtoResponseData, ServerError};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::Arc;
use storage::table::Table;
use storage::Row;

pub enum HandlerError {
    Client(String),
    Server(String),
    Disconnected,
}

pub async fn handle_tcp_stream(
    mut stream: TcpStream,
    partition: usize,
    num_of_threads: usize,
    senders: Vec<OperationSender>,
    tables: Arc<HashMap<String, Mutex<Table>>>,
) {
    tracing::info!("Accepting connection on thread {}", partition);

    let num_of_threads_bytes = (num_of_threads as u32).to_be_bytes().to_vec();

    if let (Err(error), _) = stream.write_all(num_of_threads_bytes).await {
        tracing::error!("Failed to send num_of_threads: {}", error.to_string());
        return;
    }

    loop {
        let response_bytes =
            match listen_for_tcp_request(&mut stream, partition, senders.clone(), tables.clone())
                .await
            {
                Ok(proto_response) => proto_response.write_to_bytes().unwrap(),
                Err(handler_error) => match handler_error {
                    HandlerError::Client(client_error) => {
                        tracing::warn!("Invalid request on thread {}", partition);

                        let proto_response = client_error_to_proto_response(client_error);
                        proto_response.write_to_bytes().unwrap()
                    }
                    HandlerError::Server(server_error) => {
                        tracing::error!("Internal server error: {}", server_error);

                        let mut server_error = ServerError::new();
                        server_error.detail = "Internal server error".to_string();

                        let mut proto_response = ProtoResponse::new();
                        proto_response.data = Some(ProtoResponseData::ServerError(server_error));
                        proto_response.write_to_bytes().unwrap()
                    }
                    HandlerError::Disconnected => {
                        tracing::warn!("Client disconnected on thread {}", partition);
                        return;
                    }
                },
            };
        write_to_tcp(&mut stream, response_bytes).await;
    }
}

async fn listen_for_tcp_request(
    stream: &mut TcpStream,
    current_partition: usize,
    mut senders: Vec<OperationSender>,
    tables: Arc<HashMap<String, Mutex<Table>>>,
) -> Result<ProtoResponse, HandlerError> {
    let request_size = stream
        .read_u32()
        .await
        .map_err(|error| match error.kind() {
            ErrorKind::UnexpectedEof => HandlerError::Disconnected,
            ErrorKind::ConnectionReset => HandlerError::Disconnected,
            _ => HandlerError::Server(format!(
                "Failed to parse command size: {}",
                error.to_string()
            )),
        })?;
    tracing::info!("Incoming command on thread {}", current_partition);

    let buffer = vec![0u8; request_size as usize];
    let (result, mut buffer) = stream.read_exact(buffer).await;
    result.map_err(|e| HandlerError::Server(e.to_string()))?;

    let command = parse_command_from_bytes(&mut buffer).map_err(|e| {
        tracing::warn!(
            "Invalid command on thread {}: {}",
            current_partition,
            e.to_string()
        );
        HandlerError::Client(format!("Invalid command: {}", e.to_string()))
    })?;

    let proto_response = match command {
        Command::Single(operation, table_name) => {
            let target_partition =
                get_hash_key_target_partition(&operation.hash_key(), senders.len());
            let operation_response = match target_partition == current_partition {
                true => {
                    let mut table = tables
                        .get(&table_name)
                        .ok_or(HandlerError::Client(format!(
                            "Table named '{}' not found",
                            table_name
                        )))?
                        .lock()
                        .await;

                    handle_operation(operation, &mut table)
                }
                false => {
                    tracing::info!(
                        "Sending from thread {} to thread {}",
                        current_partition,
                        target_partition
                    );
                    let (response_sender, response_receiver) = oneshot::channel();
                    let sender = senders.get_mut(target_partition).unwrap();
                    sender
                        .send((Vec::from([operation]), table_name, response_sender))
                        .await
                        .unwrap();
                    response_receiver.await.unwrap().pop().unwrap()
                }
            };
            Response::Single(operation_response).to_proto_response()
        }
        Command::GetMany(operations, table_name) => {
            if !tables.contains_key(&table_name) {
                return Err(HandlerError::Client(format!(
                    "Table named '{}' not found",
                    table_name
                )));
            }

            tracing::info!("Sending get requests from thread {}", current_partition);
            let operation_responses =
                send_operations(operations, senders.clone(), &table_name).await;
            Response::GetMany(operation_responses).to_proto_response()
        }
        Command::Batch(operations, table_name) => {
            if !tables.contains_key(&table_name) {
                return Err(HandlerError::Client(format!(
                    "Table named '{}' not found",
                    table_name
                )));
            }

            tracing::info!("Sending batches from thread {}", current_partition);
            let operation_responses =
                send_operations(operations, senders.clone(), &table_name).await;
            Response::Batch(operation_responses).to_proto_response()
        }
    };

    tracing::info!("Request on thread {} handled", current_partition);
    Ok(proto_response)
}

pub fn handle_operation(operation: Operation, table: &mut Table) -> OperationResponse {
    match operation {
        Get(hash_key, sort_key) => {
            let primary_key = format!("{}:{}", hash_key, sort_key);
            let val = table.memtable.get(&primary_key).cloned();
            OperationResponse::Get(val)
        }
        Insert(hash_key, sort_key, values) => {
            let val = table.memtable.insert(Row::new(hash_key, sort_key, values));
            OperationResponse::Insert(val)
        }
        Delete(hash_key, sort_key) => {
            let primary_key = format!("{}:{}", hash_key, sort_key);

            let val = table.memtable.delete(&primary_key);
            OperationResponse::Delete(val)
        }
    }
}

async fn write_to_tcp(stream: &mut TcpStream, bytes: Vec<u8>) {
    let response_size_prefix = (bytes.len() as u32).to_be_bytes().to_vec();
    if let (Err(error), _) = stream.write_all(response_size_prefix).await {
        tracing::error!("Couldn't write response to tcp, {}", error);
    }

    if let (Err(error), _) = stream.write_all(bytes).await {
        tracing::error!("Couldn't write response to tcp, {}", error);
    }
}
