use crate::listener::write_to_tcp;
use crate::proto_parsing::{parse_request_from_bytes, Request};
use crate::thread_channels::ThreadCommand::{Delete, Get, Insert};
use crate::thread_channels::{
    get_command_target_partition, send_batches, ThreadChannel, ThreadCommand, ThreadResponse,
};
use futures::lock::Mutex;
use futures::{SinkExt, StreamExt};
use monoio::io::AsyncReadRentExt;
use monoio::net::TcpStream;
use protobuf::Message;
use protos::util::client_error_to_proto_response;
use protos::{BatchResponse, ProtoResponse, ProtoResponseData, ServerError};
use std::io::ErrorKind;
use std::sync::Arc;
use storage::{Row, SkipList};

pub enum HandlerError {
    Client(String),
    Server(String),
    Disconnected,
}

pub async fn handle_tcp_stream(
    mut stream: TcpStream,
    partition: usize,
    channels: Vec<Arc<Mutex<ThreadChannel>>>,
    memtable: Arc<Mutex<SkipList<Row>>>,
) {
    tracing::info!("Accepting connection on thread {}", partition);

    loop {
        let response_bytes = match listen_for_tcp_request(
            &mut stream,
            partition,
            channels.clone(),
            memtable.clone(),
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

async fn listen_for_tcp_request(
    stream: &mut TcpStream,
    current_partition: usize,
    channels: Vec<Arc<Mutex<ThreadChannel>>>,
    memtable: Arc<Mutex<SkipList<Row>>>,
) -> Result<ProtoResponse, HandlerError> {
    let request_size_buffer = vec![0u8; 4];
    let (result, request_size_buffer) = stream.read_exact(request_size_buffer).await;

    result.map_err(|error| match error.kind() {
        ErrorKind::UnexpectedEof => HandlerError::Disconnected,
        _ => HandlerError::Server(format!(
            "Failed to parse request size: {}",
            error.to_string()
        )),
    })?;

    tracing::info!("Incoming request on thread {}", current_partition);

    let request_size_bytes: [u8; 4] = request_size_buffer.try_into().unwrap();
    let request_size = u32::from_be_bytes(request_size_bytes) as usize;

    let buffer = vec![0u8; request_size];
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
