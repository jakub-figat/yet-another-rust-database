use monoio::FusionDriver;
use network::start_tcp_server;
use routing::Command::*;
use storage::Value::*;

fn main() {
    let mut main_runtime = monoio::RuntimeBuilder::<FusionDriver>::new()
        .build()
        .unwrap();
    main_runtime.block_on(run());
}

async fn run() {
    start_tcp_server().await
}

// skiplist expected times
// 6secs for 1mln, 42s! for 5mln, 2mins! for 10mln
// with unsafe: 3s for 1mln, 21s for 5mln, 53s for 10mln

// on release build: 1sec for 1mln, 10secs for 5mln, 28s for 10mln
// on release with unsafe: 0.8s for 1mln, 9sec for 5mln, 24s for 10mln
