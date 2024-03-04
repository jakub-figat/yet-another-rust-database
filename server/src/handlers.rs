use crate::proto_parsing::{parse_request_from_bytes, Request};
use crate::thread_channels::ThreadCommand::{Delete, Get, Insert};
use crate::thread_channels::{
    get_command_target_partition, send_batches, ThreadChannel, ThreadCommand, ThreadResponse,
};
use futures::lock::Mutex;
use futures::{SinkExt, StreamExt};
use monoio::io::AsyncReadRentExt;
use monoio::net::TcpStream;
use protos::{BatchResponse, ProtoResponse, ProtoResponseData};
use std::sync::Arc;
use storage::{Row, SkipList};

pub enum HandlerError {
    Client(String),
    Server(String),
}

pub async fn receive_from_tcp(
    stream: &mut TcpStream,
    current_partition: usize,
    channels: Vec<Arc<Mutex<ThreadChannel>>>,
    memtable: Arc<Mutex<SkipList<Row>>>,
) -> Result<ProtoResponse, HandlerError> {
    tracing::info!("Incoming request on thread {}", current_partition);

    let request_size = stream.read_u32().await.map_err(|e| {
        HandlerError::Server(format!("Failed to parse request size: {}", e.to_string()))
    })?;

    let buffer = vec![0u8; request_size as usize];
    let (result, mut buffer) = stream.read_exact(buffer).await;
    result.map_err(|e| HandlerError::Server(e.to_string()))?;

    let request = parse_request_from_bytes(&mut buffer).map_err(|e| {
        HandlerError::Client(format!(
            "Invalid request on thread {}: {}",
            current_partition,
            e.to_string()
        ))
    })?;

    let proto_response = match request {
        Request::Command(command) => {
            let target_partition = get_command_target_partition(&command, channels.len());
            let response = match target_partition == current_partition {
                true => handle_command(command, memtable.clone()).await,
                false => {
                    tracing::info!(
                        "Sending from thread {} to thread {}",
                        current_partition,
                        target_partition
                    );
                    let mut channel = channels[target_partition].lock().await;
                    channel.sender.send(command).await.unwrap();
                    channel.receiver.next().await.unwrap()
                }
            };
            response.to_proto_response()
        }
        Request::Batch(commands) => {
            let responses = send_batches(commands, channels.clone()).await;
            let mut batch_response = BatchResponse::new();
            let batch_items: Vec<_> = responses
                .into_iter()
                .map(|response| response.to_batch_response_item())
                .collect();
            batch_response.items = batch_items;

            let mut proto_response = ProtoResponse::new();
            proto_response.data = Some(ProtoResponseData::Batch(batch_response));
            proto_response
        }
    };

    tracing::info!("Request on thread {} handled", current_partition);
    Ok(proto_response)
}

pub async fn handle_command(
    command: ThreadCommand,
    memtable: Arc<Mutex<SkipList<Row>>>,
) -> ThreadResponse {
    let mut memtable = memtable.lock().await;
    match command {
        Get(hash_key, sort_key, table) => {
            let val = memtable
                .get(&Row::new(hash_key, sort_key, vec![], table))
                .cloned();
            ThreadResponse::Get(val)
        }
        Insert(hash_key, sort_key, values, table) => {
            let val = memtable.insert(Row::new(hash_key, sort_key, values, table));
            ThreadResponse::Insert(val)
        }
        Delete(hash_key, sort_key, table) => {
            let val = memtable.delete(&Row::new(hash_key, sort_key, vec![], table));
            ThreadResponse::Delete(val)
        }
    }
}
