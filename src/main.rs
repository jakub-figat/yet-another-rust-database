use futures::{SinkExt, StreamExt};
use monoio::FusionDriver;
use murmur3::murmur3_32;
use routing::run_storage_threads;
use routing::Command::*;
use std::io::Cursor;
use std::thread::{self, available_parallelism};
use std::time::Duration;

fn main() {
    // TODO: allow for multiple columns (table, strong-typed schema)
    // instead of row with singular value

    // implement as some kind of "Value" enum with all possible types
    // somehow dynamically store columns, types, all table metadata info per row
    // maybe as field with vector containing concrete types
    // and vector with values matching column types' positions?

    let mut main_runtime = monoio::RuntimeBuilder::<FusionDriver>::new()
        .build()
        .unwrap();
    let num_of_workers = available_parallelism().unwrap().get() - 1;

    main_runtime.block_on(async {
        let (senders, mut receivers) = run_storage_threads(num_of_workers);

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
}
