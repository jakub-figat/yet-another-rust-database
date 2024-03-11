use crate::proto_parsing::{parse_command_from_request, parse_request_from_bytes};
use crate::thread_channels::{
    send_operations, send_transaction_aborted, send_transaction_begun, Command, OperationSender,
    Response, ThreadMessage, ThreadOperations,
};
use crate::transaction_manager::TransactionManager;
use crate::util::{handle_operation, write_to_tcp};
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

pub async fn handle_tcp_stream(
    mut stream: TcpStream,
    partition: usize,
    num_of_threads: usize,
    mut senders: Vec<OperationSender>,
    tables: Arc<HashMap<String, Mutex<Table>>>,
    transaction_manager: Arc<Mutex<TransactionManager>>,
) {
    tracing::info!("Accepting connection on thread {}", partition);

    let num_of_threads_bytes = (num_of_threads as u32).to_be_bytes().to_vec();

    if let (Err(error), _) = stream.write_all(num_of_threads_bytes).await {
        tracing::error!("Failed to send num_of_threads: {}", error.to_string());
        return;
    }

    loop {
        let response_bytes = match handle_tcp_request(
            &mut stream,
            partition,
            &mut senders,
            tables.clone(),
            transaction_manager.clone(),
        )
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

async fn handle_tcp_request(
    stream: &mut TcpStream,
    current_partition: usize,
    senders: &mut Vec<OperationSender>,
    tables: Arc<HashMap<String, Mutex<Table>>>,
    transaction_manager: Arc<Mutex<TransactionManager>>,
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

    let buffer = vec![0u8; request_size as usize];
    let (result, mut buffer) = stream.read_exact(buffer).await;
    result.map_err(|e| HandlerError::Server(e.to_string()))?;

    let request = parse_request_from_bytes(&mut buffer)
        .map_err(|e| client_error_from_string(&e, current_partition))?;

    let transaction_id = request.transaction_id;
    let command = parse_command_from_request(request)
        .map_err(|e| client_error_from_string(&e, current_partition))?;

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

                    handle_operation(
                        operation,
                        &mut table,
                        transaction_id,
                        transaction_manager.clone(),
                    )
                    .await
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
                        .send(ThreadMessage::Operations(ThreadOperations::new(
                            Vec::from([operation]),
                            table_name,
                            transaction_id,
                            response_sender,
                        )))
                        .await
                        .unwrap();
                    response_receiver.await.unwrap().pop().unwrap()
                }
            }?;
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
                send_operations(operations, &table_name, transaction_id, senders).await;
            let mut responses = Vec::with_capacity(operation_responses.len());
            for operation_response in operation_responses {
                responses.push(operation_response?);
            }

            Response::GetMany(responses).to_proto_response()
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
                send_operations(operations, &table_name, transaction_id, senders).await;

            let mut responses = Vec::with_capacity(operation_responses.len());
            for operation_response in operation_responses {
                responses.push(operation_response?);
            }
            Response::Batch(responses).to_proto_response()
        }
        Command::BeginTransaction => {
            let mut manager = transaction_manager.lock().await;
            let transaction_id = manager.begin();
            send_transaction_begun(transaction_id, senders).await;
            Response::Transaction(transaction_id).to_proto_response()
        }
        Command::CommitTransaction => {
            let mut manager = transaction_manager.lock().await;
            let transaction_id = transaction_id.ok_or(HandlerError::Client(
                "Transaction id cannot be null".to_string(),
            ))?;
            manager.commit(transaction_id, tables.clone()).await?;

            Response::Transaction(transaction_id).to_proto_response()
        }
        Command::AbortTransaction => {
            let mut manager = transaction_manager.lock().await;
            let transaction_id = transaction_id.ok_or(HandlerError::Client(
                "Transaction id cannot be null".to_string(),
            ))?;
            if !manager.abort(transaction_id) {
                return Err(HandlerError::Client(format!(
                    "Cannot abort non existing transaction with id '{}'",
                    transaction_id
                )));
            }

            send_transaction_aborted(transaction_id, senders).await;

            Response::Transaction(transaction_id).to_proto_response()
        }
    };

    tracing::info!("Request on thread {} handled", current_partition);
    Ok(proto_response)
}

fn client_error_from_string(error: &str, partition: usize) -> HandlerError {
    tracing::warn!("Invalid request on thread {}: {}", partition, error);
    HandlerError::Client(format!("Invalid request: {}", error))
}

#[derive(Debug, Clone)]
pub enum HandlerError {
    Client(String),
    Server(String),
    Disconnected,
}
