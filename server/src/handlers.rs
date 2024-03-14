use crate::proto_parsing::{parse_command_from_request, parse_request_from_bytes};
use crate::thread_channels::Operation::{Delete, Get, Insert};
use crate::thread_channels::{
    send_drop_table, send_sync_model, send_transaction_aborted, send_transaction_begun,
    send_transaction_committed, send_transaction_prepare, Command, Operation, OperationResponse,
    OperationSender, Response,
};
use crate::transaction_manager::TransactionManager;
use common::partition::get_hash_key_target_partition;
use futures::lock::Mutex;
use monoio::io::{AsyncReadRentExt, AsyncWriteRentExt};
use monoio::net::TcpStream;
use protobuf::Message;
use protos::util::client_error_to_proto_response;
use protos::{ProtoResponse, ProtoResponseData, ServerError};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::Arc;
use storage::sstable::read_row_from_sstable;
use storage::table::{drop_table, sync_model, Table};
use storage::transaction::Transaction;
use storage::validation::validate_values_against_schema;
use storage::Row;

pub async fn handle_tcp_stream(
    mut stream: TcpStream,
    partition: usize,
    num_of_threads: usize,
    mut senders: Vec<OperationSender>,
    tables: Arc<Mutex<HashMap<String, Table>>>,
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
    tables: Arc<Mutex<HashMap<String, Table>>>,
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
            validate_hash_key_partition(&operation.hash_key(), current_partition, senders.len())?;

            let operation_response = handle_operation(
                operation,
                table_name,
                tables.clone(),
                transaction_id,
                transaction_manager.clone(),
                current_partition,
                senders.len(),
            )
            .await?;
            Response::Single(operation_response).to_proto_response()
        }
        Command::GetMany(operations, table_name) => {
            let responses = handle_operations(
                operations,
                table_name,
                tables.clone(),
                transaction_id,
                transaction_manager.clone(),
                current_partition,
                senders.len(),
            )
            .await?;
            Response::GetMany(responses).to_proto_response()
        }
        Command::Batch(operations, table_name) => {
            let responses = handle_operations(
                operations,
                table_name,
                tables.clone(),
                transaction_id,
                transaction_manager.clone(),
                current_partition,
                senders.len(),
            )
            .await?;
            Response::Batch(responses).to_proto_response()
        }
        Command::BeginTransaction => {
            let mut manager = transaction_manager.lock().await;

            let transaction_id = manager.add_coordinated();
            manager.add(transaction_id);

            send_transaction_begun(transaction_id, senders, current_partition).await;
            Response::Transaction(transaction_id).to_proto_response()
        }
        Command::CommitTransaction => {
            let mut manager = transaction_manager.lock().await;
            let transaction_id = transaction_id.ok_or(HandlerError::Client(
                "Transaction id cannot be null".to_string(),
            ))?;

            manager.remove_coordinated(transaction_id)?;
            let transaction = manager.remove(transaction_id).unwrap();

            if !(send_transaction_prepare(transaction_id, senders, current_partition).await
                && transaction.can_commit(tables.clone()).await)
            {
                send_transaction_aborted(transaction_id, senders, current_partition).await;

                return Err(HandlerError::Server(format!(
                    "Transaction with id '{}' failed to commit and got aborted",
                    transaction_id
                )));
            }

            send_transaction_committed(transaction_id, senders, current_partition).await;
            Response::Transaction(transaction_id).to_proto_response()
        }
        Command::AbortTransaction => {
            let mut manager = transaction_manager.lock().await;
            let transaction_id = transaction_id.ok_or(HandlerError::Client(
                "Transaction id cannot be null".to_string(),
            ))?;

            manager.remove_coordinated(transaction_id)?;
            manager.remove(transaction_id);

            send_transaction_aborted(transaction_id, senders, current_partition).await;

            Response::Transaction(transaction_id).to_proto_response()
        }
        Command::SyncModel(schema_string) => {
            sync_model(schema_string.clone(), tables.clone(), current_partition)
                .await
                .map_err(|e| HandlerError::Client(e))?;
            send_sync_model(schema_string, senders, current_partition).await;
            Response::SyncModel.to_proto_response()
        }
        Command::DropTable(table_name) => {
            drop_table(
                table_name.clone(),
                tables.clone(),
                current_partition,
                senders.len(),
            )
            .await
            .map_err(|e| HandlerError::Client(e))?;
            send_drop_table(table_name, senders, current_partition).await;
            Response::DropTable.to_proto_response()
        }
    };

    tracing::info!("Request on thread {} handled", current_partition);
    Ok(proto_response)
}

fn validate_hash_key_partition(
    hash_key: &str,
    current_partition: usize,
    num_of_partitions: usize,
) -> Result<(), HandlerError> {
    if get_hash_key_target_partition(hash_key, num_of_partitions) != current_partition {
        return Err(HandlerError::Client("Invalid partition".to_string()));
    }

    Ok(())
}

#[derive(Debug, Clone)]
pub enum HandlerError {
    Client(String),
    Server(String),
    Disconnected,
}

fn client_error_from_string(error: &str, partition: usize) -> HandlerError {
    tracing::warn!("Invalid request on thread {}: {}", partition, error);
    HandlerError::Client(format!("Invalid request: {}", error))
}

async fn handle_operation(
    operation: Operation,
    table_name: String,
    tables: Arc<Mutex<HashMap<String, Table>>>,
    transaction_id: Option<u64>,
    transaction_manager: Arc<Mutex<TransactionManager>>,
    current_partition: usize,
    num_of_partitions: usize,
) -> Result<OperationResponse, HandlerError> {
    let mut manager = transaction_manager.lock().await;
    let mut transaction = get_transaction_by_id(transaction_id, &mut manager)?;

    execute_operation(
        operation,
        table_name,
        tables.clone(),
        &mut transaction,
        current_partition,
        num_of_partitions,
    )
    .await
}

async fn handle_operations(
    operations: Vec<Operation>,
    table_name: String,
    tables: Arc<Mutex<HashMap<String, Table>>>,
    transaction_id: Option<u64>,
    transaction_manager: Arc<Mutex<TransactionManager>>,
    current_partition: usize,
    num_of_partitions: usize,
) -> Result<Vec<OperationResponse>, HandlerError> {
    let mut manager = transaction_manager.lock().await;
    let mut transaction = get_transaction_by_id(transaction_id, &mut manager)?;

    let mut responses = Vec::with_capacity(operations.len());

    for operation in operations {
        validate_hash_key_partition(&operation.hash_key(), current_partition, num_of_partitions)?;
        responses.push(
            execute_operation(
                operation,
                table_name.clone(),
                tables.clone(),
                &mut transaction,
                current_partition,
                num_of_partitions,
            )
            .await?,
        );
    }

    Ok(responses)
}

async fn execute_operation(
    operation: Operation,
    table_name: String,
    tables: Arc<Mutex<HashMap<String, Table>>>,
    transaction: &mut Option<&mut Transaction>,
    current_partition: usize,
    num_of_partitions: usize,
) -> Result<OperationResponse, HandlerError> {
    let mut tables = tables.lock().await;
    let table = tables
        .get_mut(&table_name)
        .ok_or(HandlerError::Client(format!(
            "Table named '{}' not found",
            table_name
        )))?;

    match operation {
        Get(hash_key, sort_key) => {
            let primary_key = format!("{}:{}", hash_key, sort_key);
            let mut val = table.memtable.get(&primary_key).cloned();

            if val.is_none() {
                val = read_row_from_sstable(
                    &primary_key,
                    &table,
                    current_partition,
                    num_of_partitions,
                )
                .await;
            }

            if let Some(transaction) = transaction.as_mut() {
                transaction.get_for_update(val.as_ref(), table.table_schema.name.clone());
            }

            Ok(OperationResponse::Get(val))
        }
        Insert(hash_key, sort_key, values) => {
            validate_values_against_schema(&sort_key, &values, &table.table_schema)
                .map_err(|e| HandlerError::Client(e))?;

            let row = Row::new(hash_key, sort_key, values);

            match transaction {
                Some(transaction) => transaction.insert(row, &table),
                None => {
                    {
                        let mut commit_log = table.commit_log.lock().await;
                        commit_log.write_insert(&row).await;
                    }

                    table.memtable.insert(row, false);
                    if table.memtable.max_size_reached() {
                        table.flush_memtable_to_disk(current_partition).await;
                    }
                }
            }

            Ok(OperationResponse::Insert)
        }
        Delete(hash_key, sort_key) => {
            let primary_key = format!("{}:{}", hash_key, sort_key);

            let val = match transaction {
                Some(transaction) => transaction.delete(primary_key, &table),
                None => table.memtable.delete(&primary_key, None),
            };

            Ok(OperationResponse::Delete(val))
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

fn get_transaction_by_id(
    transaction_id: Option<u64>,
    manager: &mut TransactionManager,
) -> Result<Option<&mut Transaction>, HandlerError> {
    match transaction_id {
        Some(id) => match manager.transactions.get_mut(&id) {
            Some(transaction) => Ok(Some(transaction)),
            None => Err(HandlerError::Client(format!(
                "Transaction with id '{}' does not exist",
                id
            ))),
        },
        None => Ok(None),
    }
}
