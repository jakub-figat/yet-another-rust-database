use crate::Command::{Delete, Get, Insert};
use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};
use monoio::FusionDriver;
use murmur3::murmur3_32;
use std::io::Cursor;
use std::thread;
use storage::SkipList;
use storage::{Row, Value};

static MURMUR3_SEED: u32 = 1119284470;

pub struct ThreadRouter {
    senders: Vec<CommandSender>,
    receivers: Vec<ResponseReceiver>,
    num_of_threads: usize,
}

impl ThreadRouter {
    pub fn new(num_of_threads: usize) -> ThreadRouter {
        let (senders, receivers) = run_storage_threads(num_of_threads);
        ThreadRouter {
            senders,
            receivers,
            num_of_threads,
        }
    }

    pub async fn send_command(&mut self, command: Command) -> Option<Response> {
        let primary_key = command.primary_key();
        let hash = murmur3_32(&mut Cursor::new(&primary_key), MURMUR3_SEED).unwrap();
        let partition = (hash % (self.num_of_threads as u32)) as usize;
        let mut sender = self.senders[partition].clone();
        let receiver = &mut self.receivers[partition];
        let _ = sender.send(command).await;
        receiver.next().await
    }
}

pub enum Command {
    Get(String, Value),
    Insert(String, Value, Vec<Value>),
    Delete(String, Value),
    // GetBetween(String, String, u64, u64),
}

impl Command {
    pub fn primary_key(&self) -> String {
        match self {
            Get(hash_key, sort_key) => get_primary_key(hash_key, sort_key),
            Insert(hash_key, sort_key, _) => get_primary_key(hash_key, sort_key),
            Delete(hash_key, sort_key) => get_primary_key(hash_key, sort_key),
        }
    }
}

#[derive(Debug)]
pub enum Response {
    Get(Option<Row>),
    Write(Result<(), String>),
    Delete(Option<Row>),
}

fn get_primary_key(hash_key: &str, sort_key: &Value) -> String {
    let sort_key_string = sort_key.to_string();
    let mut primary_key = String::with_capacity(hash_key.len() + sort_key_string.len() + 1);

    primary_key.push_str(&hash_key);
    primary_key.push_str(":");
    primary_key.push_str(&sort_key_string);
    primary_key
}

pub type CommandSender = mpsc::Sender<Command>;
pub type CommandReceiver = mpsc::Receiver<Command>;

pub type ResponseSender = mpsc::Sender<Response>;
pub type ResponseReceiver = mpsc::Receiver<Response>;

fn run_storage_threads(num_of_threads: usize) -> (Vec<CommandSender>, Vec<ResponseReceiver>) {
    let mut senders = Vec::with_capacity(num_of_threads);
    let mut receivers = Vec::with_capacity(num_of_threads);
    for _ in 0..num_of_threads {
        let (command_sender, mut command_receiver) = mpsc::channel(64);
        senders.push(command_sender);

        let (response_sender, response_receiver) = mpsc::channel(64);
        receivers.push(response_receiver);

        thread::spawn(move || {
            let mut response_sender = response_sender.clone();
            let mut runtime = monoio::RuntimeBuilder::<FusionDriver>::new()
                .build()
                .unwrap();
            runtime.block_on(async {
                let mut list = SkipList::default();

                while let Some(command) = command_receiver.next().await {
                    match command {
                        Get(hash_key, sort_key) => {
                            let val = list.get(&Row::new(hash_key, sort_key, vec![])).cloned();
                            let _ = response_sender.send(Response::Get(val)).await;
                        }
                        Insert(hash_key, sort_key, values) => {
                            let val = list.insert(Row::new(hash_key, sort_key, values));
                            let _ = response_sender.send(Response::Write(val)).await;
                        }
                        Delete(hash_key, sort_key) => {
                            let val = list.delete(&Row::new(hash_key, sort_key, vec![]));
                            let _ = response_sender.send(Response::Delete(val)).await;
                        }
                    }
                }
            })
        });
    }

    (senders, receivers)
}
