use crate::handlers::HandlerError;
use crate::thread_channels::Operation::{Delete, Get, Insert};
use crate::thread_channels::{Operation, OperationResponse};
use crate::transaction_manager::TransactionManager;
use futures::lock::Mutex;
use monoio::io::AsyncWriteRentExt;
use monoio::net::TcpStream;
use std::sync::Arc;
use storage::table::Table;
use storage::transaction::Transaction;
use storage::validation::validate_values_against_schema;
use storage::Row;

pub async fn handle_operation(
    operation: Operation,
    table: &mut Table,
    transaction_id: Option<u64>,
    transaction_manager: Arc<Mutex<TransactionManager>>,
) -> Result<OperationResponse, HandlerError> {
    let mut manager = transaction_manager.lock().await;
    let transaction = get_transaction_by_id(transaction_id, &mut manager)?;

    match operation {
        Get(hash_key, sort_key) => {
            let primary_key = format!("{}:{}", hash_key, sort_key);
            let val = table.memtable.get(&primary_key).cloned();

            if let Some(transaction) = transaction {
                transaction.get_for_update(val.as_ref(), table.table_schema.name.clone());
            }

            Ok(OperationResponse::Get(val))
        }
        Insert(hash_key, sort_key, values) => {
            validate_values_against_schema(&values, &table.table_schema)
                .map_err(|e| HandlerError::Client(e))?;

            let row = Row::new(hash_key, sort_key, values);

            match transaction {
                Some(transaction) => transaction.insert(row, &table),
                None => table.memtable.insert(row),
            }

            Ok(OperationResponse::Insert)
        }
        Delete(hash_key, sort_key) => {
            let primary_key = format!("{}:{}", hash_key, sort_key);

            let val = match transaction {
                Some(transaction) => transaction.delete(primary_key, &table),
                None => table.memtable.delete(&primary_key),
            };

            Ok(OperationResponse::Delete(val))
        }
    }
}

pub async fn write_to_tcp(stream: &mut TcpStream, bytes: Vec<u8>) {
    let response_size_prefix = (bytes.len() as u32).to_be_bytes().to_vec();
    if let (Err(error), _) = stream.write_all(response_size_prefix).await {
        tracing::error!("Couldn't write response to tcp, {}", error);
    }

    if let (Err(error), _) = stream.write_all(bytes).await {
        tracing::error!("Couldn't write response to tcp, {}", error);
    }
}

pub fn get_transaction_by_id(
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
