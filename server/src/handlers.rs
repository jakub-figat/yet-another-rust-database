use crate::proto_parsing::{client_error_to_proto_response, parse_request_from_bytes, Request};
use crate::protos::response::{
    response::Data as ProtoResponseData, BatchResponse, Response as ProtoResponse,
};
use crate::thread_channels::ThreadCommand::{Delete, Get, Insert};
use crate::thread_channels::{
    get_command_target_partition, send_batches, ThreadChannel, ThreadCommand, ThreadResponse,
};
use futures::lock::Mutex;
use futures::{SinkExt, StreamExt};
use monoio::io::AsyncWriteRentExt;
use monoio::net::TcpStream;
use protobuf::Message;
use std::sync::Arc;
use storage::{Row, SkipList};

pub async fn receive_from_tcp(
    connection: TcpStream,
    buffer: &mut Vec<u8>,
    current_partition: usize,
    channels: Vec<Arc<Mutex<ThreadChannel>>>,
    memtable: Arc<Mutex<SkipList<Row>>>,
) {
    let request = parse_request_from_bytes(buffer);
    if let Err(error) = request {
        let proto_response = client_error_to_proto_response(error);
        let bytes = proto_response.write_to_bytes().unwrap();
        write_to_tcp(connection, bytes).await;
        return;
    }

    match request.unwrap() {
        Request::Command(command) => {
            let target_partition = get_command_target_partition(&command, channels.len());
            let response = match target_partition != current_partition {
                true => handle_command(command, memtable.clone()).await,
                false => {
                    let mut channel = channels[target_partition].lock().await;
                    channel.sender.send(command).await.unwrap();
                    channel.receiver.next().await.unwrap()
                }
            };
            write_to_tcp(
                connection,
                response.to_proto_response().write_to_bytes().unwrap(),
            )
            .await;
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

            write_to_tcp(connection, proto_response.write_to_bytes().unwrap()).await;
        }
    };
}

async fn write_to_tcp(mut connection: TcpStream, bytes: Vec<u8>) {
    if let (Err(error), _) = connection.write_all(bytes).await {
        tracing::error!("Couldn't write response to tcp, {}", error);
    }
}

pub async fn handle_command(
    command: ThreadCommand,
    memtable: Arc<Mutex<SkipList<Row>>>,
) -> ThreadResponse {
    let mut memtable = memtable.lock().await;
    match command {
        Get(hash_key, sort_key) => {
            let val = memtable.get(&Row::new(hash_key, sort_key, vec![])).cloned();
            ThreadResponse::Get(val)
        }
        Insert(hash_key, sort_key, values) => {
            let val = memtable.insert(Row::new(hash_key, sort_key, values));
            ThreadResponse::Insert(val)
        }
        Delete(hash_key, sort_key) => {
            let val = memtable.delete(&Row::new(hash_key, sort_key, vec![]));
            ThreadResponse::Delete(val)
        }
    }
}