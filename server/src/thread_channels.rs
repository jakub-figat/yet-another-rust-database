use common::partition::get_hash_key_target_partition;
use common::value::Value;
use futures::channel::{mpsc, oneshot};
use futures::SinkExt;
use protos::util::{parse_message_field_from_value, parse_proto_from_value};
use protos::{
    BatchResponse, DeleteResponse, GetManyResponse, GetResponse, InsertResponse, ProtoResponse,
    ProtoResponseData,
};
use std::collections::HashMap;
use storage::Row;

pub type OperationSender = mpsc::UnboundedSender<(Vec<Operation>, String, OperationResponseSender)>;
pub type OperationReceiver =
    mpsc::UnboundedReceiver<(Vec<Operation>, String, OperationResponseSender)>;

pub type OperationResponseSender = oneshot::Sender<Vec<OperationResponse>>;

#[derive(Debug)]
pub enum Command {
    Single(Operation, String),
    GetMany(Vec<Operation>, String),
    Batch(Vec<Operation>, String),
}

#[derive(Debug)]
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
}

#[derive(Debug)]
pub enum OperationResponse {
    Get(Option<Row>),
    Insert(Result<(), String>),
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
                OperationResponse::Insert(result) => {
                    let mut insert_response = InsertResponse::new();
                    insert_response.okay = result.is_ok();
                    Some(ProtoResponseData::Insert(insert_response))
                }
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
                        OperationResponse::Insert(insert) => insert.is_ok(),
                        OperationResponse::Delete(delete) => delete,
                        _ => panic!("Invalid operation response type"),
                    }
                });
                Some(ProtoResponseData::Batch(batch_response))
            }
        };

        proto_response.data = proto_response_data;
        proto_response
    }
}

pub async fn send_operations(
    operations: Vec<Operation>,
    mut senders: Vec<OperationSender>,
    table_name: &String,
) -> Vec<OperationResponse> {
    let mut batches: Vec<_> = (0..senders.len()).map(|_| Vec::new()).collect();
    let mut responses = Vec::with_capacity(operations.len());

    for operation in operations {
        let hash_key = operation.hash_key();
        let partition = get_hash_key_target_partition(&hash_key, senders.len());
        batches[partition].push(operation);
    }

    for (partition, batch) in batches
        .into_iter()
        .enumerate()
        .filter(|(_, batch)| !batch.is_empty())
    {
        let (response_sender, response_receiver) = oneshot::channel();
        let sender = senders.get_mut(partition).unwrap();

        tracing::info!("Sending batch to thread {}", partition);
        sender
            .send((batch, table_name.clone(), response_sender))
            .await
            .unwrap();
        for operation_response in response_receiver.await.unwrap() {
            responses.push(operation_response);
        }
    }

    responses
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
