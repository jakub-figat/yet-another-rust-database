use futures::channel::mpsc;
use futures::{SinkExt, StreamExt};
use monoio::FusionDriver;
use murmur3::murmur3_32;
use routing::Command::*;
use routing::Response;
use std::io::Cursor;
use std::thread::{self, available_parallelism};
use std::time::Duration;
use storage::row::Row;
use storage::SkipList;

fn main() {
    // 2. tests
    // 3. sstables
    // 4. writing to comit ahead log
    // 5. sequential write to disk with header file
    // 6. loading into memory
    // 7. compaction
    //
    let mut main_runtime = monoio::RuntimeBuilder::<FusionDriver>::new()
        .build()
        .unwrap();

    main_runtime.block_on(async {
        let num_of_workers = available_parallelism().unwrap().get() - 1;
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

        let commands = vec![
            Get("5".to_string(), 5),
            Insert("5".to_string(), 5),
            Get("5".to_string(), 5),
            Delete("5".to_string(), 5),
            Get("5".to_string(), 5),
        ];

        for command in commands {
            let primary_key = command.primary_key();
            let hash = murmur3_32(&mut Cursor::new(&primary_key), 0).unwrap();
            let partition = (hash % (num_of_workers as u32)) as usize;
            let mut sender = senders[partition].clone();
            let receiver = &mut receivers[partition];
            let _ = sender.send(command).await;
            let response = receiver.next().await.unwrap();
            println!("{:?}", response);
        }
        thread::sleep(Duration::from_secs(2));
    });

    // let mut list = SkipList::new(16, 0.5);
    // let data: Vec<_> = (1..=i32::pow(10, 1)).collect();
    // for num in data.iter() {
    //     let row = Row::new(num.to_string(), num.clone(), num.clone());
    //     list.insert(row).unwrap();
    // }

    // let mut current = Some(list.head);
    // unsafe {
    //     while let Some(current_node) = current {
    //         println!("{}", (*current_node.as_ptr()).value.primary_key);
    //         current = (*current_node.as_ptr()).refs[0];
    //     }
    // }
    // println!("Size: {} bytes", list.memory_size);
    // let mut s = String::new();
    // stdin().read_line(&mut s).unwrap();
}
