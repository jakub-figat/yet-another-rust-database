use crate::context::ThreadContext;
use crate::handlers::handle_tcp_stream;
use crate::thread_channels::{OperationReceiver, OperationSender, ThreadMessage};
use crate::transaction_manager::TransactionManager;
use futures::channel::{mpsc, oneshot};
use futures::lock::Mutex;
use futures::{SinkExt, StreamExt};
use monoio::net::TcpListener;
use monoio::utils::CtrlC;
use monoio::FusionDriver;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::thread;
use storage::commit_log::{replay_commit_logs, CommitLog};
use storage::sstable::{compaction_main, flush_memtable_to_sstable, SSTABLES_DIR};
use storage::table::{
    drop_table, read_table_schemas, sync_model, Table, TableSchema, TABLE_SCHEMAS_DIR,
    TABLE_SCHEMAS_FILE_PATH,
};
use storage::Memtable;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{filter, Layer};

static TCP_STARTING_PORT: usize = 29800;

pub async fn run_listener_threads(num_of_threads: usize) {
    std::fs::create_dir_all(TABLE_SCHEMAS_DIR).unwrap();
    std::fs::create_dir_all(SSTABLES_DIR).unwrap();
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_filter(filter::LevelFilter::WARN))
        .init();

    let num_of_threads = match num_of_threads > 0 {
        true => num_of_threads,
        false => 1,
    };

    let num_of_partitions = 1000usize;
    let table_schemas = read_table_schemas(TABLE_SCHEMAS_FILE_PATH).await.unwrap();

    let (mut compaction_thread_sender, compaction_thread_receiver) = mpsc::channel(16);

    let table_schemas2 = table_schemas.clone();
    thread::spawn(move || {
        let mut runtime = monoio::RuntimeBuilder::<FusionDriver>::new()
            .build()
            .unwrap();

        runtime.block_on(compaction_main(
            compaction_thread_receiver,
            table_schemas2,
            num_of_partitions,
            SSTABLES_DIR,
        ));
    });

    let mut senders = Vec::with_capacity(num_of_threads);
    let mut receivers = Vec::with_capacity(num_of_threads);

    for _ in 0..num_of_threads {
        let (command_sender, command_receiver) = mpsc::unbounded();

        senders.push(command_sender);
        receivers.push(Some(command_receiver));
    }

    let mut partitions_per_thread = HashMap::new();
    for partition in 0..num_of_partitions {
        partitions_per_thread
            .entry(partition % num_of_threads)
            .or_insert(HashSet::new())
            .insert(partition);
    }

    for thread_num in 0..num_of_threads {
        let table_schemas = table_schemas.clone();
        let senders = senders.clone();
        let receiver = receivers[thread_num].take().unwrap();
        let thread_partitions = partitions_per_thread.remove(&thread_num).unwrap();
        let thread_context = ThreadContext {
            partitions: thread_partitions,
            total_number_of_partitions: num_of_threads,
            current_thread_number: thread_num,
            number_of_threads: num_of_threads,
        };

        thread::spawn(move || {
            // TODO: make sure thread is pinned to core
            let mut runtime = monoio::RuntimeBuilder::<FusionDriver>::new()
                .build()
                .unwrap();

            runtime.block_on(thread_main(
                thread_context,
                senders,
                receiver,
                table_schemas.clone(),
            ));
        });
    }

    let ctrl_c = CtrlC::new().unwrap();

    ctrl_c.await;
    tracing::info!("Shutting down...");

    let mut ctrl_c_receivers = Vec::new();
    for thread_num in 0..num_of_threads {
        let (sender, receiver) = oneshot::channel();
        ctrl_c_receivers.push(receiver);
        senders[thread_num]
            .send(ThreadMessage::CtrlC(sender))
            .await
            .unwrap();
    }

    let (sender, receiver) = oneshot::channel();
    ctrl_c_receivers.push(receiver);
    compaction_thread_sender.send(sender).await.unwrap();

    for ctrl_c_receiver in ctrl_c_receivers {
        ctrl_c_receiver.await.unwrap();
    }
}

async fn thread_main(
    thread_context: ThreadContext,
    senders: Vec<OperationSender>,
    mut receiver: OperationReceiver,
    table_schemas: Vec<TableSchema>,
) {
    let mut tables = HashMap::new();
    for table_schema in table_schemas {
        replay_commit_logs(
            &table_schema,
            &thread_context.partitions,
            thread_context.total_number_of_partitions,
        )
        .await;
        let memtable = Memtable::default();

        let commit_log = CommitLog::open_new(&table_schema, &thread_context.partitions).await;

        tables.insert(
            table_schema.name.clone(),
            Table::new(memtable, commit_log, table_schema),
        );
    }

    let tables = Arc::new(Mutex::new(tables));
    let transaction_manager = Arc::new(Mutex::new(TransactionManager::new()));

    let tcp_port = TCP_STARTING_PORT + thread_context.current_thread_number;
    let tcp_listener = TcpListener::bind(format!("0.0.0.0:{}", tcp_port.to_string())).unwrap();
    tracing::info!(
        "Listening on port {} on thread {}",
        tcp_port,
        thread_context.current_thread_number
    );

    loop {
        monoio::select! {
            stream = tcp_listener.accept() => {
                monoio::spawn(handle_tcp_stream(
                    stream.unwrap().0,
                    thread_context.clone(),
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
                        transaction.commit(tables.clone(), thread_context.total_number_of_partitions).await;
                    }
                    ThreadMessage::TransactionAborted(transaction_id) => {
                        let mut manager = transaction_manager.lock().await;
                        manager.remove(transaction_id);
                    }
                    ThreadMessage::SyncModel(schema_string) => {
                        sync_model(schema_string, tables.clone(), &thread_context.partitions, TABLE_SCHEMAS_FILE_PATH).await.unwrap();
                    }
                    ThreadMessage::DropTable(table_name) => {
                        drop_table(table_name, tables.clone(), TABLE_SCHEMAS_FILE_PATH, SSTABLES_DIR).await.unwrap();
                    }
                    ThreadMessage::CtrlC(sender) => {
                        tracing::info!("Shutting down database thread, flushing memtables...");
                        let mut tables = tables.lock().await;
                        for (_, table) in tables.iter_mut() {
                            let mut memtable = Memtable::default();
                            std::mem::swap(&mut table.memtable, &mut memtable);

                            flush_memtable_to_sstable(
                                memtable,
                                table.commit_log.clone(),
                                table.table_schema.clone(),
                                thread_context.total_number_of_partitions
                            )
                            .await;
                        }
                        sender.send(()).unwrap();
                    }
                }
            }
        }
    }
}
