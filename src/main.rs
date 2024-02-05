use rand::seq::SliceRandom;
use rand::thread_rng;
use storage::BalancedBST;

fn main() {
    let mut tree = BalancedBST::new();
    let mut rng = thread_rng();

    let mut nums: Vec<_> = (1..=10000).collect();
    nums.shuffle(&mut rng);

    for num in nums {
        tree.insert(num, num).unwrap();
    }

    for node in tree.as_in_order_vec() {
        println!("{}", node.key)
    }
}
