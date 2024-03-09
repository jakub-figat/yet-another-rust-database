use crate::handlers::{handle_operation, handle_tcp_stream};
use crate::thread_channels::{OperationReceiver, OperationSender};
use futures::channel::mpsc;
use futures::lock::Mutex;
use futures::StreamExt;
use monoio::net::TcpListener;
use monoio::FusionDriver;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use storage::table::{read_table_schemas, Table};
use storage::Memtable;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{filter, Layer};

static TCP_STARTING_PORT: usize = 29800;

pub fn run_listener_threads(num_of_threads: usize) {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(filter::LevelFilter::WARN))
        .init();

    let mut senders = Vec::with_capacity(num_of_threads);
    let mut receivers = Vec::with_capacity(num_of_threads);

    for _ in 0..num_of_threads {
        let (command_sender, command_receiver) = mpsc::unbounded();

        senders.push(command_sender);
        receivers.push(Some(command_receiver));
    }

    for partition in 0..num_of_threads {
        let senders = senders.clone();
        let receiver = receivers[partition].take().unwrap();

        thread::spawn(move || {
            let mut runtime = monoio::RuntimeBuilder::<FusionDriver>::new()
                .build()
                .unwrap();

            runtime.block_on(thread_main(partition, num_of_threads, senders, receiver));
        });
    }

    loop {}
}

async fn thread_main(
    partition: usize,
    num_of_threads: usize,
    senders: Vec<OperationSender>,
    mut receiver: OperationReceiver,
) {
    let table_schemas = read_table_schemas().await.unwrap();
    let tables: Arc<HashMap<String, Mutex<Table>>> = Arc::new(
        table_schemas
            .into_iter()
            .map(|table_schema| {
                (
                    table_schema.name.clone(),
                    Mutex::new(Table::new(Memtable::default(), table_schema)),
                )
            })
            .collect(),
    );

    let tcp_port = TCP_STARTING_PORT + partition;
    let tcp_listener = TcpListener::bind(format!("0.0.0.0:{}", tcp_port.to_string())).unwrap();
    tracing::info!("Listening on port {} on thread {}", tcp_port, partition);

    loop {
        monoio::select! {
            stream = tcp_listener.accept() => {
                monoio::spawn(handle_tcp_stream(
                    stream.unwrap().0, partition, num_of_threads, senders.clone(), tables.clone())
                );
            }
            Some((operations, table_name, response_sender)) = receiver.next() => {
                let mut table = tables.get(&table_name).unwrap().lock().await;

                let responses: Vec<_> = operations
                    .into_iter()
                    .map(|operation| handle_operation(operation, &mut table)).collect();
                response_sender.send(responses).unwrap();
            }
        }
    }
}

// TODO allow batches to only affect one table
