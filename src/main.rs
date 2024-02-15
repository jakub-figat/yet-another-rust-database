use monoio::FusionDriver;
use routing::Command::*;
use routing::ThreadRouter;
use std::thread::{self, available_parallelism};
use std::time::Duration;
use storage::Value::*;

fn main() {
    let mut main_runtime = monoio::RuntimeBuilder::<FusionDriver>::new()
        .build()
        .unwrap();
    let num_of_threads = available_parallelism().unwrap().get() - 1;
    let mut thread_router = ThreadRouter::new(num_of_threads);

    main_runtime.block_on(async {
        let commands = vec![
            Get("5".to_string(), Int32(5)),
            Insert(
                "5".to_string(),
                Int32(5),
                vec![Text("abc".to_string()), Boolean(true)],
            ),
            Get("5".to_string(), Int32(5)),
            Delete("5".to_string(), Int32(5)),
            Get("5".to_string(), Int32(5)),
        ];

        for command in commands {
            let response = thread_router.send_command(command).await.unwrap();
            println!("{:?}", response);
        }
        thread::sleep(Duration::from_secs(2));
    });
}

// skiplist expected times
// 6secs for 1mln, 42s! for 5mln, 2mins! for 10mln
// with unsafe: 3s for 1mln, 21s for 5mln, 53s for 10mln

// on release build: 1sec for 1mln, 10secs for 5mln, 28s for 10mln
// on release with unsafe: 0.8s for 1mln, 9sec for 5mln, 24s for 10mln
