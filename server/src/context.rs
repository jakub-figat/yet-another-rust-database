use std::collections::HashSet;

#[derive(Clone)]
pub struct ThreadContext {
    pub partitions: HashSet<usize>,
    pub total_number_of_partitions: usize,
    pub current_thread_number: usize,
    pub number_of_threads: usize,
}
