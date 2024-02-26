use crate::handlers::{handle_command, receive_from_tcp};
use crate::thread_channels::{CommandReceiver, ResponseSender, ThreadChannel};
use futures::channel::mpsc;
use futures::lock::Mutex;
use futures::{SinkExt, StreamExt};
use monoio::net::TcpListener;
use monoio::FusionDriver;
use std::sync::Arc;
use std::thread;
use storage::Row;
use storage::{SkipList, MEGABYTE};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{filter, Layer};

static BUFFER_SIZE: usize = MEGABYTE * 512;
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
    let mut buffer = vec![0u8; BUFFER_SIZE];
    let memtable = Arc::new(Mutex::new(SkipList::<Row>::default()));

    let tcp_listener = TcpListener::bind(format!("0.0.0.0:{}", TCP_PORT.to_string())).unwrap();
    tracing::info!("Listening on port {} on thread {}", TCP_PORT, partition);

    let (mut response_sender, mut command_receiver) = inner_channel;

    loop {
        monoio::select! {
            Ok((connection, _)) = tcp_listener.accept() => {
                buffer = receive_from_tcp(connection, buffer, partition, channels.clone(), memtable.clone()).await;
            }
            Some(command) = command_receiver.next() => {
                let response = handle_command(command, memtable.clone()).await;
                response_sender.send(response).await.unwrap();
            }
        }
    }
}
