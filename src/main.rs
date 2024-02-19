use server::run_listener_threads;
use std::thread::available_parallelism;

fn main() {
    let num_of_threads = available_parallelism().unwrap().get();
    run_listener_threads(num_of_threads);
}

// skiplist expected times
// 6secs for 1mln, 42s! for 5mln, 2mins! for 10mln
// with unsafe: 3s for 1mln, 21s for 5mln, 53s for 10mln

// on release build: 1sec for 1mln, 10secs for 5mln, 28s for 10mln
// on release with unsafe: 0.8s for 1mln, 9sec for 5mln, 24s for 10mln
