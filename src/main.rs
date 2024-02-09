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

    let mut nums: Vec<_> = (1..=i32::pow(10, 6)).collect();
    nums.shuffle(&mut rng);

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


#[cfg(test)]
mod tests {
    use rand::prelude::SliceRandom;
    use rand::thread_rng;
    use storage::SkipList;

    #[test]
    fn test_list() {
        {
            let mut list = SkipList::new(16, 0.5);

            let mut rng = thread_rng();
            let mut nums: Vec<_> = (1..=i32::pow(10, 3)).collect();
            nums.shuffle(&mut rng);

            for num in nums.iter() {
                list.insert(num.clone(), num.clone()).unwrap();
            }

            list.get(&777).unwrap();
            list.delete(&777).unwrap();
            list.insert(777, 1).unwrap();
            list.get(&777).unwrap();
        }
    }
}