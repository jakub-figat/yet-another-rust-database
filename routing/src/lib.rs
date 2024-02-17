use crate::Command::{Delete, Get, Insert};
use futures::channel::mpsc;
use futures::lock::Mutex;
use futures::{SinkExt, StreamExt};
use monoio::FusionDriver;
use murmur3::murmur3_32;
use std::io::Cursor;
use std::thread;
use storage::SkipList;
use storage::{Row, Value};

static MURMUR3_SEED: u32 = 1119284470;

pub type CommandSender = mpsc::UnboundedSender<Command>;
pub type CommandReceiver = mpsc::UnboundedReceiver<Command>;

pub type ResponseSender = mpsc::UnboundedSender<Response>;
pub type ResponseReceiver = mpsc::UnboundedReceiver<Response>;

pub struct ThreadRouter {
    channels: Vec<Mutex<ThreadChannel>>,
    num_of_threads: usize,
}

struct ThreadChannel {
    sender: CommandSender,
    receiver: ResponseReceiver,
}

impl ThreadRouter {
    pub fn new(num_of_threads: usize) -> ThreadRouter {
        let channels = run_storage_threads(num_of_threads);
        ThreadRouter {
            channels,
            num_of_threads,
        }
    }

    pub async fn send_command(&self, command: Command) -> Option<Response> {
        let hash_key = command.hash_key();
        let hash = murmur3_32(&mut Cursor::new(&hash_key), MURMUR3_SEED).unwrap();
        let partition = (hash % (self.num_of_threads as u32)) as usize;
        // let mut sender = self.senders[partition].clone();
        // let receiver = &mut self.receivers[partition];
        let mut channel = self.channels[partition].lock().await;
        let _ = channel.sender.send(command).await;
        channel.receiver.next().await
    }

    pub async fn send_in_batches(&self, commands: Vec<Command>) -> Vec<Response> {
        let mut batches: Vec<_> = (0..self.num_of_threads).map(|_| Vec::new()).collect();
        let mut responses_vec: Vec<Option<Response>> = (0..commands.len()).map(|_| None).collect();
        let mut response_index = 0usize;

        for command in commands {
            let hash_key = command.hash_key();
            let hash = murmur3_32(&mut Cursor::new(&hash_key), MURMUR3_SEED).unwrap();
            let partition = (hash % (self.num_of_threads as u32)) as usize;
            batches[partition].push((response_index, command));
            response_index += 1;
        }

        let mut response_batches: Vec<_> = (0..batches.len()).map(|_| Vec::new()).collect();
        for (partition, batch) in batches
            .into_iter()
            .enumerate()
            .filter(|(_, batch)| !batch.is_empty())
        {
            let mut channel = self.channels[partition].lock().await;
            for (response_index, command) in batch {
                response_batches[partition].push(response_index);
                channel.sender.send(command).await.unwrap();
            }

            for response_index in &response_batches[partition] {
                let response = channel.receiver.next().await.unwrap();
                responses_vec[response_index.clone()] = Some(response);
            }
        }

        responses_vec
            .into_iter()
            .map(|response| response.unwrap())
            .collect()
    }
}

pub enum Command {
    Get(String, Value),
    Insert(String, Value, Vec<Value>),
    Delete(String, Value),
    // GetBetween(String, String, u64, u64),
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

// fn get_primary_key(hash_key: &str, sort_key: &Value) -> String {
//     let sort_key_string = sort_key.to_string();
//     let mut primary_key = String::with_capacity(hash_key.len() + sort_key_string.len() + 1);
//
//     primary_key.push_str(&hash_key);
//     primary_key.push_str(":");
//     primary_key.push_str(&sort_key_string);
//     primary_key
// }

fn run_storage_threads(num_of_threads: usize) -> Vec<Mutex<ThreadChannel>> {
    let mut channels = Vec::with_capacity(num_of_threads);
    for _ in 0..num_of_threads {
        let (command_sender, mut command_receiver) = mpsc::unbounded();

        let (response_sender, response_receiver) = mpsc::unbounded();
        channels.push(Mutex::new(ThreadChannel {
            sender: command_sender,
            receiver: response_receiver,
        }));

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

    channels
}
