use crate::handlers::handle_tcp_stream;
use crate::thread_channels::{OperationReceiver, OperationSender, ThreadMessage};
use crate::transaction_manager::TransactionManager;
use futures::channel::mpsc;
use futures::lock::Mutex;
use futures::StreamExt;
use monoio::net::TcpListener;
use monoio::FusionDriver;
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use storage::table::{drop_table, read_table_schemas, sync_model, Table};
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
    let tables: Arc<Mutex<HashMap<String, Table>>> = Arc::new(Mutex::new(
        table_schemas
            .into_iter()
            .map(|table_schema| {
                (
                    table_schema.name.clone(),
                    Table::new(Memtable::default(), table_schema),
                )
            })
            .collect(),
    ));
    let transaction_manager = Arc::new(Mutex::new(TransactionManager::new()));

    let tcp_port = TCP_STARTING_PORT + partition;
    let tcp_listener = TcpListener::bind(format!("0.0.0.0:{}", tcp_port.to_string())).unwrap();
    tracing::info!("Listening on port {} on thread {}", tcp_port, partition);

    loop {
        monoio::select! {
            stream = tcp_listener.accept() => {
                monoio::spawn(handle_tcp_stream(
                    stream.unwrap().0,
                    partition,
                    num_of_threads,
                    senders.clone(),
                    tables.clone(),
                    transaction_manager.clone()
                )
                );
            }
            Some(thread_message) = receiver.next() => {
                match thread_message {
                    ThreadMessage::TransactionBegun(transaction_id) => {
                        let mut manager = transaction_manager.lock().await;
                        manager.add(transaction_id);
                    }
                    ThreadMessage::TransactionPrepare(transaction_id, response_sender) => {
                        let manager = transaction_manager.lock().await;
                        let transaction = manager.transactions.get(&transaction_id).unwrap();
                        response_sender.send(transaction.can_commit(tables.clone()).await).unwrap();
                    }
                    ThreadMessage::TransactionCommit(transaction_id) => {
                        let mut manager = transaction_manager.lock().await;
                        let mut transaction = manager.transactions.remove(&transaction_id).unwrap();
                        transaction.commit(tables.clone(), partition).await;
                    }
                    ThreadMessage::TransactionAborted(transaction_id) => {
                        let mut manager = transaction_manager.lock().await;
                        manager.remove(transaction_id);
                    }
                    ThreadMessage::SyncModel(schema_string) => {
                        sync_model(schema_string, tables.clone()).await.unwrap();
                    }
                    ThreadMessage::DropTable(table_name) => {
                        drop_table(table_name, tables.clone()).await.unwrap();
                    }
                }
            }
        }
    }
}
