use monoio::FusionDriver;
use routing::Command::*;
use routing::ThreadRouter;
use std::thread::available_parallelism;
use storage::Value::*;

fn main() {
    let mut main_runtime = monoio::RuntimeBuilder::<FusionDriver>::new()
        .build()
        .unwrap();
    main_runtime.block_on(run());
}

async fn run() {
    let num_of_threads = available_parallelism().unwrap().get() - 1;
    let mut thread_router = ThreadRouter::new(num_of_threads);
    let insert_nums: Vec<_> = (1..=100).collect();

    let commands: Vec<_> = insert_nums
        .iter()
        .map(|&num| Insert(num.to_string(), Unsigned64(num), vec![]))
        .collect();

    let _ = thread_router.send_in_batches(commands).await;
}

// skiplist expected times
// 6secs for 1mln, 42s! for 5mln, 2mins! for 10mln
// with unsafe: 3s for 1mln, 21s for 5mln, 53s for 10mln

// on release build: 1sec for 1mln, 10secs for 5mln, 28s for 10mln
// on release with unsafe: 0.8s for 1mln, 9sec for 5mln, 24s for 10mln
