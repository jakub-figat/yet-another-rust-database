use rand::seq::SliceRandom;
use rand::thread_rng;
use std::time::Instant;
use storage::SkipList;

fn main() {
    // 6secs for 1mln, 42s! for 5mln, 2mins! for 10mln
    // with unsafe: 3s for 1mln, 21s for 5mln, 53s for 10mln

    // on release build: 1sec for 1mln, 10secs for 5mln, 28s for 10mln
    // on release with unsafe: 0.8s for 1mln, 9sec for 5mln, 24s for 10mln

    // for dev: 2x speedup with unsafe
    // for release: some minimal speedup
    let mut list = SkipList::new(16, 0.5);

    let mut rng = thread_rng();
    let mut nums: Vec<_> = (1..=i32::pow(10, 6)).collect();
    nums.shuffle(&mut rng);

    let start = Instant::now();
    for num in nums.iter() {
        list.insert(num.clone(), num.clone()).unwrap();
    }

    println!("Total write time: {}ms", start.elapsed().as_millis());

    let search = Instant::now();
    list.get(&888888);

    println!("Search time: {} microseconds", search.elapsed().as_micros());
}
