use crate::proto_parsing::{parse_message_field_from_value, parse_proto_from_value};
use crate::protos::response::{
    batch_response_item::Item as BatchItem, response::Data as ProtoResponseData, BatchResponseItem,
    DeleteResponse, GetResponse, InsertResponse, Response as ProtoResponse,
};
use futures::channel::mpsc;
use futures::lock::Mutex;
use futures::{SinkExt, StreamExt};
use murmur3::murmur3_32;
use std::io::Cursor;
use std::sync::Arc;
use storage::{Row, Value};

static MURMUR3_SEED: u32 = 1119284470;

pub type CommandSender = mpsc::UnboundedSender<ThreadCommand>;
pub type CommandReceiver = mpsc::UnboundedReceiver<ThreadCommand>;

pub type ResponseSender = mpsc::UnboundedSender<ThreadResponse>;
pub type ResponseReceiver = mpsc::UnboundedReceiver<ThreadResponse>;

pub struct ThreadChannel {
    pub sender: CommandSender,
    pub receiver: ResponseReceiver,
}

pub enum ThreadCommand {
    Get(String, Value),
    Insert(String, Value, Vec<Value>),
    Delete(String, Value),
}

impl ThreadCommand {
    pub fn hash_key(&self) -> String {
        match self {
            ThreadCommand::Get(hash_key, _) => hash_key.clone(),
            ThreadCommand::Insert(hash_key, _, _) => hash_key.clone(),
            ThreadCommand::Delete(hash_key, _) => hash_key.clone(),
        }
    }
}

#[derive(Debug)]
pub enum ThreadResponse {
    Get(Option<Row>),
    Insert(Result<(), String>),
    Delete(Option<Row>),
}

impl ThreadResponse {
    pub fn to_proto_response(self) -> ProtoResponse {
        let mut proto_response = ProtoResponse::new();
        let proto_response_data = match self {
            ThreadResponse::Get(result) => result.map(|row| {
                let mut get_response = GetResponse::new();
                get_response.hash_key = row.hash_key;
                get_response.sort_key = parse_message_field_from_value(row.sort_key);
                get_response.values = row
                    .values
                    .into_iter()
                    .map(|value| parse_proto_from_value(value))
                    .collect();
                ProtoResponseData::Get(get_response)
            }),
            ThreadResponse::Insert(result) => {
                let mut insert_response = InsertResponse::new();
                insert_response.okay = result.is_ok();
                Some(ProtoResponseData::Insert(insert_response))
            }
            ThreadResponse::Delete(result) => {
                let mut delete_response = DeleteResponse::new();
                delete_response.okay = result.is_some();
                Some(ProtoResponseData::Delete(delete_response))
            }
        };

        proto_response.data = proto_response_data;
        proto_response
    }

    pub fn to_batch_response_item(self) -> BatchResponseItem {
        let mut batch_response_item = BatchResponseItem::new();
        let item_data = match self {
            ThreadResponse::Get(result) => result.map(|row| {
                let mut get_response = GetResponse::new();
                get_response.hash_key = row.hash_key;
                get_response.sort_key = parse_message_field_from_value(row.sort_key);
                get_response.values = row
                    .values
                    .into_iter()
                    .map(|value| parse_proto_from_value(value))
                    .collect();
                BatchItem::Get(get_response)
            }),
            ThreadResponse::Insert(result) => {
                let mut insert_response = InsertResponse::new();
                insert_response.okay = result.is_ok();
                Some(BatchItem::Insert(insert_response))
            }
            ThreadResponse::Delete(result) => {
                let mut delete_response = DeleteResponse::new();
                delete_response.okay = result.is_some();
                Some(BatchItem::Delete(delete_response))
            }
        };

        batch_response_item.item = item_data;
        batch_response_item
    }
}

pub fn get_command_target_partition(command: &ThreadCommand, num_of_threads: usize) -> usize {
    let hash_key = command.hash_key();
    let hash = murmur3_32(&mut Cursor::new(&hash_key), MURMUR3_SEED).unwrap();
    (hash % (num_of_threads as u32)) as usize
}

pub async fn send_batches(
    commands: Vec<ThreadCommand>,
    channels: Vec<Arc<Mutex<ThreadChannel>>>,
) -> Vec<ThreadResponse> {
    let mut batches: Vec<_> = (0..channels.len()).map(|_| Vec::new()).collect();
    let mut responses_vec: Vec<Option<ThreadResponse>> =
        (0..commands.len()).map(|_| None).collect();
    let mut response_index = 0usize;

    for command in commands {
        let hash_key = command.hash_key();
        let hash = murmur3_32(&mut Cursor::new(&hash_key), MURMUR3_SEED).unwrap();
        let partition = (hash % (channels.len() as u32)) as usize;
        batches[partition].push((response_index, command));
        response_index += 1;
    }

    let mut response_batches: Vec<_> = (0..batches.len()).map(|_| Vec::new()).collect();
    for (partition, batch) in batches
        .into_iter()
        .enumerate()
        .filter(|(_, batch)| !batch.is_empty())
    {
        let mut channel = channels[partition].lock().await;
        for (response_index, command) in batch {
            response_batches[partition].push(response_index);
            channel.sender.send(command).await.unwrap();
        }

        for response_index in &response_batches[partition] {
            let response = channel.receiver.next().await.unwrap();
            responses_vec[response_index.clone()] = Some(response);
        }
    }

    responses_vec
        .into_iter()
        .map(|response| response.unwrap())
        .collect()
}
