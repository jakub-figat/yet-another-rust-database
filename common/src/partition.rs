use murmur3::murmur3_32;
use std::io::Cursor;

pub static MURMUR3_SEED: u32 = 1119284470;

pub fn get_hash_key_target_partition(hash_key: &str, num_of_partitions: usize) -> usize {
    let hash = murmur3_32(&mut Cursor::new(&hash_key), MURMUR3_SEED).unwrap();
    (hash % (num_of_partitions as u32)) as usize
}
