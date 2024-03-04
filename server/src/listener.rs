use crate::handlers::{handle_command, receive_from_tcp, HandlerError};
use crate::thread_channels::{CommandReceiver, ResponseSender, ThreadChannel};
use futures::channel::mpsc;
use futures::lock::Mutex;
use futures::{SinkExt, StreamExt};
use monoio::io::AsyncWriteRentExt;
use monoio::net::{TcpListener, TcpStream};
use monoio::FusionDriver;
use protobuf::Message;
use protos::util::client_error_to_proto_response;
use protos::{ProtoResponse, ProtoResponseData, ServerError};
use std::sync::Arc;
use std::thread;
use storage::Row;
use storage::SkipList;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{filter, Layer};

static TCP_PORT: usize = 29876;

pub fn run_listener_threads(num_of_threads: usize) {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(filter::LevelFilter::INFO))
        .init();

    let mut channels = Vec::with_capacity(num_of_threads);
    let mut inner_channels = Vec::with_capacity(num_of_threads);

    for _ in 0..num_of_threads {
        let (command_sender, command_receiver) = mpsc::unbounded();
        let (response_sender, response_receiver) = mpsc::unbounded();

        channels.push(Arc::new(Mutex::new(ThreadChannel {
            sender: command_sender,
            receiver: response_receiver,
        })));

        inner_channels.push(Some((response_sender, command_receiver)));
    }

    for partition in 0..num_of_threads {
        let channels = channels.clone();
        let inner_channel = inner_channels[partition].take().unwrap();

        thread::spawn(move || {
            let mut runtime = monoio::RuntimeBuilder::<FusionDriver>::new()
                .build()
                .unwrap();

            runtime.block_on(thread_main(partition, inner_channel, channels));
        });
    }

    loop {}
}

async fn thread_main(
    partition: usize,
    inner_channel: (ResponseSender, CommandReceiver),
    channels: Vec<Arc<Mutex<ThreadChannel>>>,
) {
    let memtable = Arc::new(Mutex::new(SkipList::<Row>::default()));

    let tcp_listener = TcpListener::bind(format!("0.0.0.0:{}", TCP_PORT.to_string())).unwrap();
    tracing::info!("Listening on port {} on thread {}", TCP_PORT, partition);

    let (mut response_sender, mut command_receiver) = inner_channel;

    loop {
        monoio::select! {
            Ok((connection, _)) = tcp_listener.accept() => {
                let mut stream = connection;
                let response_bytes = match receive_from_tcp(
                    &mut stream, partition, channels.clone(), memtable.clone()
                ).await {
                    Ok(proto_response) => {
                        proto_response.write_to_bytes().unwrap()
                    }
                    Err(handler_error) => {
                        match handler_error {
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
                        }
                    }
                };
                write_to_tcp(&mut stream, response_bytes).await;
            }
            Some(command) = command_receiver.next() => {
                let response = handle_command(command, memtable.clone()).await;
                response_sender.send(response).await.unwrap();
            }
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
