use crate::handlers::{handle_command, handle_tcp_stream};
use crate::thread_channels::{CommandReceiver, ResponseSender, ThreadChannel};
use futures::channel::mpsc;
use futures::lock::Mutex;
use futures::{SinkExt, StreamExt};
use monoio::net::TcpListener;
use monoio::FusionDriver;
use std::sync::Arc;
use std::thread;
use storage::Row;
use storage::SkipList;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{filter, Layer};

static TCP_STARTING_PORT: usize = 29800;

pub fn run_listener_threads(num_of_threads: usize) {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(filter::LevelFilter::WARN))
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
                .with_entries(2048)
                .build()
                .unwrap();

            runtime.block_on(thread_main(
                partition,
                num_of_threads,
                inner_channel,
                channels,
            ));
        });
    }

    loop {}
}

async fn thread_main(
    partition: usize,
    num_of_threads: usize,
    inner_channel: (ResponseSender, CommandReceiver),
    channels: Vec<Arc<Mutex<ThreadChannel>>>,
) {
    let memtable = Arc::new(Mutex::new(SkipList::<Row>::default()));

    let tcp_port = TCP_STARTING_PORT + partition;
    let tcp_listener = TcpListener::bind(format!("0.0.0.0:{}", tcp_port.to_string())).unwrap();
    tracing::info!("Listening on port {} on thread {}", tcp_port, partition);

    let (mut response_sender, mut command_receiver) = inner_channel;
    loop {
        monoio::select! {
            something = tcp_listener.accept() => {
                let (stream, _) = something.unwrap();
                monoio::spawn(handle_tcp_stream(stream, partition, num_of_threads, channels.clone(), memtable.clone()));
            }
            Some(command) = command_receiver.next() => {
                let response = handle_command(command, memtable.clone()).await;
                response_sender.send(response).await.unwrap();
            }
        }
    }
}
