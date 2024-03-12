use common::value::Value;
use futures::channel::{mpsc, oneshot};
use futures::SinkExt;
use protos::util::{parse_message_field_from_value, parse_proto_from_value};
use protos::{
    BatchResponse, DeleteResponse, DropTableResponse, GetManyResponse, GetResponse, InsertResponse,
    ProtoResponse, ProtoResponseData, SyncModelResponse, TransactionResponse,
};
use std::collections::HashMap;
use storage::Row;

pub enum ThreadMessage {
    TransactionBegun(u64),
    TransactionPrepare(u64, oneshot::Sender<bool>),
    TransactionCommit(u64),
    TransactionAborted(u64),
    SyncModel(String),
    DropTable(String),
}

pub type OperationSender = mpsc::UnboundedSender<ThreadMessage>;
pub type OperationReceiver = mpsc::UnboundedReceiver<ThreadMessage>;

#[derive(Debug)]
pub enum Command {
    Single(Operation, String),
    GetMany(Vec<Operation>, String),
    Batch(Vec<Operation>, String),
    BeginTransaction,
    CommitTransaction,
    AbortTransaction,
    SyncModel(String),
    DropTable(String),
}

#[derive(Debug, Clone)]
pub enum Operation {
    Get(String, Value),
    Insert(String, Value, HashMap<String, Value>),
    Delete(String, Value),
}

impl Operation {
    pub fn hash_key(&self) -> String {
        match self {
            Operation::Get(hash_key, _) => hash_key.clone(),
            Operation::Insert(hash_key, _, _) => hash_key.clone(),
            Operation::Delete(hash_key, _) => hash_key.clone(),
        }
    }
}

#[derive(Debug)]
pub enum Response {
    Single(OperationResponse),
    GetMany(Vec<OperationResponse>),
    Batch(Vec<OperationResponse>),
    Transaction(u64),
    SyncModel,
    DropTable,
}

#[derive(Debug)]
pub enum OperationResponse {
    Get(Option<Row>),
    Insert,
    Delete(bool),
}

impl Response {
    pub fn to_proto_response(self) -> ProtoResponse {
        let mut proto_response = ProtoResponse::new();
        let proto_response_data = match self {
            Response::Single(operation_response) => match operation_response {
                OperationResponse::Get(result) => result.map(|row| {
                    let get_response = row_to_get_response(row);
                    ProtoResponseData::Get(get_response)
                }),
                OperationResponse::Insert => Some(ProtoResponseData::Insert(InsertResponse::new())),
                OperationResponse::Delete(result) => {
                    let mut delete_response = DeleteResponse::new();
                    delete_response.okay = result;
                    Some(ProtoResponseData::Delete(delete_response))
                }
            },
            Response::GetMany(operation_responses) => {
                let mut get_many_response = GetManyResponse::new();
                for operation_response in operation_responses {
                    match operation_response {
                        OperationResponse::Get(row) => {
                            if let Some(row) = row {
                                get_many_response.items.push(row_to_get_response(row));
                            }
                        }
                        _ => panic!("Invalid operation response type"),
                    }
                }

                Some(ProtoResponseData::GetMany(get_many_response))
            }
            Response::Batch(operation_responses) => {
                let mut batch_response = BatchResponse::new();
                batch_response.okay = operation_responses.into_iter().all(|operation_response| {
                    match operation_response {
                        OperationResponse::Insert => true,
                        OperationResponse::Delete(delete) => delete,
                        _ => panic!("Invalid operation response type"),
                    }
                });
                Some(ProtoResponseData::Batch(batch_response))
            }
            Response::Transaction(transaction_id) => {
                let mut transaction_response = TransactionResponse::new();
                transaction_response.transaction_id = transaction_id;
                Some(ProtoResponseData::Transaction(transaction_response))
            }
            Response::SyncModel => Some(ProtoResponseData::Model(SyncModelResponse::new())),
            Response::DropTable => Some(ProtoResponseData::DropTable(DropTableResponse::new())),
        };

        proto_response.data = proto_response_data;
        proto_response
    }
}

pub async fn send_transaction_begun(
    transaction_id: u64,
    senders: &mut Vec<OperationSender>,
    current_partition: usize,
) {
    for (partition, sender) in senders.iter_mut().enumerate() {
        if current_partition == partition {
            continue;
        }
        sender
            .send(ThreadMessage::TransactionBegun(transaction_id))
            .await
            .unwrap();
    }
}

pub async fn send_transaction_prepare(
    transaction_id: u64,
    senders: &mut Vec<OperationSender>,
    current_partition: usize,
) -> bool {
    for (partition, sender) in senders.iter_mut().enumerate() {
        if current_partition == partition {
            continue;
        }
        let (prepare_sender, receiver) = oneshot::channel();
        sender
            .send(ThreadMessage::TransactionPrepare(
                transaction_id,
                prepare_sender,
            ))
            .await
            .unwrap();
        if !receiver.await.unwrap() {
            return false;
        }
    }

    true
}

pub async fn send_transaction_committed(
    transaction_id: u64,
    senders: &mut Vec<OperationSender>,
    current_partition: usize,
) {
    for (partition, sender) in senders.iter_mut().enumerate() {
        if current_partition == partition {
            continue;
        }
        sender
            .send(ThreadMessage::TransactionCommit(transaction_id))
            .await
            .unwrap();
    }
}

pub async fn send_transaction_aborted(
    transaction_id: u64,
    senders: &mut Vec<OperationSender>,
    current_partition: usize,
) {
    for (partition, sender) in senders.iter_mut().enumerate() {
        if current_partition == partition {
            continue;
        }
        sender
            .send(ThreadMessage::TransactionAborted(transaction_id))
            .await
            .unwrap();
    }
}

pub async fn send_sync_model(
    table_schema: String,
    senders: &mut Vec<OperationSender>,
    current_partition: usize,
) {
    for (partition, sender) in senders.iter_mut().enumerate() {
        if current_partition == partition {
            continue;
        }
        sender
            .send(ThreadMessage::SyncModel(table_schema.clone()))
            .await
            .unwrap();
    }
}

pub async fn send_drop_table(
    table_name: String,
    senders: &mut Vec<OperationSender>,
    current_partition: usize,
) {
    for (partition, sender) in senders.iter_mut().enumerate() {
        if current_partition == partition {
            continue;
        }
        sender
            .send(ThreadMessage::DropTable(table_name.clone()))
            .await
            .unwrap();
    }
}

fn row_to_get_response(row: Row) -> GetResponse {
    let mut get_response = GetResponse::new();
    get_response.hash_key = row.hash_key;
    get_response.sort_key = parse_message_field_from_value(row.sort_key);
    get_response.values = row
        .values
        .into_iter()
        .map(|(key, value)| (key, parse_proto_from_value(value)))
        .collect();

    get_response
}
