use rand::seq::SliceRandom;
use rand::thread_rng;
use std::io::stdin;
use std::time::Instant;
use storage::SkipList;

fn main() {
    // 6secs for 1mln, 42s! for 5mln
    // on release build: 1sec for 1mln, 10secs for 5mln
    let mut list = SkipList::new(16, 0.5);
    let mut rng = thread_rng();

    let mut nums_prep: Vec<_> = (1..=i32::pow(10, 6)).collect();
    nums_prep.shuffle(&mut rng);
    let mut nums = vec![];

    for num in nums_prep {
        nums.push(num);
    }

    let start = Instant::now();
    for num in nums.iter() {
        list.insert(num.clone(), num.clone()).unwrap();
    }

    println!("Total time: {}ms", start.elapsed().as_millis());
    let mut input = String::new();
    stdin().read_line(&mut input).unwrap();

    let search = Instant::now();
    list.get(&7000000);

    println!("Search time: {} microseconds", search.elapsed().as_micros());
    // let delete = Instant::now();
    // list.delete(&700000);
    // println!("Delete time: {} microseconds", delete.elapsed().as_micros());
}
