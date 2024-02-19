use crate::listener::Command::{Delete, Get, Insert};
use futures::channel::mpsc;
use futures::lock::Mutex;
use futures::{SinkExt, StreamExt};
use monoio::io::AsyncWriteRentExt;
use monoio::net::{TcpListener, TcpStream};
use monoio::FusionDriver;
use murmur3::murmur3_32;
use std::io::Cursor;
use std::sync::Arc;
use std::thread;
use storage::SkipList;
use storage::Value::Int32;
use storage::{Row, Value};

static MURMUR3_SEED: u32 = 1119284470;

type CommandSender = mpsc::UnboundedSender<Command>;
type ResponseReceiver = mpsc::UnboundedReceiver<Response>;

struct ThreadChannel {
    sender: CommandSender,
    receiver: ResponseReceiver,
}

pub enum Command {
    Get(String, Value),
    Insert(String, Value, Vec<Value>),
    Delete(String, Value),
}

impl Command {
    pub fn hash_key(&self) -> String {
        match self {
            Get(hash_key, _) => hash_key.clone(),
            Insert(hash_key, _, _) => hash_key.clone(),
            Delete(hash_key, _) => hash_key.clone(),
        }
    }
}

#[derive(Debug)]
pub enum Response {
    Get(Option<Row>),
    Write(Result<(), String>),
    Delete(Option<Row>),
}

pub fn run_listener_threads(num_of_threads: usize) {
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

            runtime.block_on(async {
                let memtable = Arc::new(Mutex::new(SkipList::<Row>::default()));
                let tcp_listener = TcpListener::bind("0.0.0.0:29876").unwrap();
                let (mut response_sender, mut command_receiver) = inner_channel;

                loop {
                    monoio::select! {
                        Ok((connection, _)) = tcp_listener.accept() => {
                            receive_from_tcp(connection, partition, channels.clone(), memtable.clone()).await
                        }
                        Some(command) = command_receiver.next() => {
                            let response = handle_command(command, memtable.clone()).await;
                            response_sender.send(response).await.unwrap();
                        }
                    }
                }
            })
        });
    }
}

async fn receive_from_tcp(
    mut connection: TcpStream,
    current_partition: usize,
    channels: Vec<Arc<Mutex<ThreadChannel>>>,
    memtable: Arc<Mutex<SkipList<Row>>>,
) {
    let dummy_command = Get("a".to_string(), Int32(2));
    let target_partition = get_command_target_partition(&dummy_command, channels.len());
    let response = match target_partition != current_partition {
        true => handle_command(dummy_command, memtable.clone()).await,
        false => {
            let mut channel = channels[target_partition].lock().await;
            channel.sender.send(dummy_command).await.unwrap();
            channel.receiver.next().await.unwrap()
        }
    };

    // set response back to tcp
    connection.write_all("sasdasd").await;
}

async fn handle_command(command: Command, memtable: Arc<Mutex<SkipList<Row>>>) -> Response {
    let mut memtable = memtable.lock().await;
    match command {
        Get(hash_key, sort_key) => {
            let val = memtable.get(&Row::new(hash_key, sort_key, vec![])).cloned();
            Response::Get(val)
        }
        Insert(hash_key, sort_key, values) => {
            let val = memtable.insert(Row::new(hash_key, sort_key, values));
            Response::Write(val)
        }
        Delete(hash_key, sort_key) => {
            let val = memtable.delete(&Row::new(hash_key, sort_key, vec![]));
            Response::Delete(val)
        }
    }
}

fn get_command_target_partition(command: &Command, num_of_threads: usize) -> usize {
    let hash_key = command.hash_key();
    let hash = murmur3_32(&mut Cursor::new(&hash_key), MURMUR3_SEED).unwrap();
    (hash % (num_of_threads as u32)) as usize
}
