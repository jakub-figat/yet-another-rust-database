use crate::Command::{Delete, Get, Insert};
use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};
use monoio::FusionDriver;
use std::thread;
use storage::row::Row;
use storage::SkipList;

#[derive(Debug)]
pub enum Command {
    Get(String, u64),
    Insert(String, u64),
    Delete(String, u64),
    // GetBetween(String, String, u64, u64),
}

impl Command {
    pub fn primary_key(&self) -> String {
        match self {
            Get(hash_key, sort_key) => get_primary_key(hash_key, sort_key),
            Insert(hash_key, sort_key) => get_primary_key(hash_key, sort_key),
            Delete(hash_key, sort_key) => get_primary_key(hash_key, sort_key),
        }
    }
}

#[derive(Debug)]
pub enum Response {
    Get(Option<(String, u64)>),
    Write(Result<(), String>),
    Delete(Option<(String, u64)>),
}

fn get_primary_key(hash_key: &str, sort_key: &u64) -> String {
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

pub fn run_storage_threads(num_of_workers: usize) -> (Vec<CommandSender>, Vec<ResponseReceiver>) {
    let mut senders = Vec::with_capacity(num_of_workers);
    let mut receivers = Vec::with_capacity(num_of_workers);
    for _ in 0..num_of_workers {
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
                            let val = list
                                .get(&Row::new(hash_key, sort_key, 1))
                                .map(|row| (row.hash_key.clone(), row.sort_key));
                            let _ = response_sender.send(Response::Get(val)).await;
                        }
                        Insert(hash_key, sort_key) => {
                            let val = list.insert(Row::new(hash_key, sort_key, 1));
                            let _ = response_sender.send(Response::Write(val)).await;
                        }
                        Delete(hash_key, sort_key) => {
                            let val = list
                                .delete(&Row::new(hash_key, sort_key, 1))
                                .map(|row| (row.hash_key.clone(), row.sort_key));
                            let _ = response_sender.send(Response::Delete(val)).await;
                        }
                    }
                }
            })
        });
    }

    (senders, receivers)
}
