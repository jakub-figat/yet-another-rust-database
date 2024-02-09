use get_size::GetSize;
use rand::{thread_rng, Rng};
use std::fmt::Debug;
use std::mem::size_of;
use std::ptr::NonNull;

// memtable, skiplist implementation
// TODO: commit log for preventing data loss on crash
// when max size is reached, flush to sstable with metadata
// default 64MB?

pub static MEGABYTE: usize = usize::pow(2, 20);
static MEMTABLE_MAX_SIZE_MEGABYTES: usize = 128;
// TODO: fix bad memory tracking
// looking at top command, seems like 1mln is around 52MB
// by jemalloc analysis, 1mln = 47MB

type ListNode<V> = NonNull<Node<V>>;

pub struct SkipList<V>
where
    V: Clone + Sized + Default + GetSize + Debug,
{
    pub head: ListNode<V>,
    pub max_level: usize,
    pub level_probability: f64,
    pub memory_size: usize,
    pub size: usize,
}

impl<V> SkipList<V>
where
    V: Clone + Sized + Default + GetSize + Debug,
{
    pub fn new(max_level: usize, level_probability: f64) -> SkipList<V> {
        SkipList {
            head: Node::new(String::new(), V::default(), max_level),
            max_level,
            level_probability,
            memory_size: 0,
            size: 0,
        }
    }

    pub fn get(&self, key: &str) -> Option<V> {
        let mut current = self.head;

        unsafe {
            for level in (0..self.max_level).rev() {
                while let Some(next_node) = (*current.as_ptr()).refs[level] {
                    if key > &(*next_node.as_ptr()).key {
                        current = next_node;
                    } else {
                        break;
                    }
                }
            }

            if let Some(next_node) = (*current.as_ptr()).refs[0].clone() {
                if key == &(*next_node.as_ptr()).key {
                    return Some((*next_node.as_ptr()).value.clone());
                }
            }
        }

        None
    }

    pub fn insert(&mut self, key: String, value: V) -> Result<(), String> {
        if self.memory_size > MEMTABLE_MAX_SIZE_MEGABYTES * MEGABYTE {
            Err(format!(
                "Memtable reached max size of {} MB with {} nodes",
                MEMTABLE_MAX_SIZE_MEGABYTES, self.size
            ))?
        }

        let new_level = self.get_random_level();
        let update_vec = self.get_update_vec(&key, new_level);

        unsafe {
            if let Some(next_node) = (*update_vec.last().unwrap().as_ptr()).refs[0] {
                if &key == &(*next_node.as_ptr()).key {
                    (*next_node.as_ptr()).key = key;
                    return Ok(());
                }
            }

            let new_node = Node::new(key.clone(), value, new_level);
            for (level, placement_node) in update_vec.into_iter().rev().enumerate() {
                (*new_node.as_ptr()).refs[level] = (*placement_node.as_ptr()).refs[level].clone();
                (*placement_node.as_ptr()).refs[level] = Some(new_node);
            }
            self.memory_size += (*new_node.as_ptr()).get_memory_size();
        }

        self.size += 1;
        Ok(())
    }

    pub fn delete(&mut self, key: &str) -> Option<V> {
        let update_vec = self.get_update_vec(key, self.max_level);

        unsafe {
            if let Some(next_node) = (*update_vec.last().unwrap().as_ptr()).refs[0] {
                let next_key = &(*next_node.as_ptr()).key;
                if key == next_key {
                    let node_level = (*next_node.as_ptr()).refs.len();
                    for (level, placement_node) in
                        update_vec.iter().rev().take(node_level).enumerate()
                    {
                        if &(*(*placement_node.as_ptr()).refs[level].unwrap().as_ptr()).key
                            != next_key
                        {
                            break;
                        }
                        (*placement_node.as_ptr()).refs[level] = (*next_node.as_ptr()).refs[level];
                    }
                    let boxed_node = Box::from_raw(next_node.as_ptr());

                    self.memory_size -= (*next_node.as_ptr()).get_memory_size();
                    self.size -= 1;
                    return Some(boxed_node.value.clone());
                }
            }
        }
        None
    }

    fn get_update_vec(&mut self, key: &str, level_limit: usize) -> Vec<ListNode<V>> {
        let mut update_vec = Vec::with_capacity(self.max_level);

        unsafe {
            let mut current = self.head;
            for level in (0..self.max_level).rev() {
                while let Some(next_node) = (*current.as_ptr()).refs[level] {
                    if key > &(*next_node.as_ptr()).key {
                        current = next_node;
                    } else {
                        break;
                    }
                }

                if level < level_limit {
                    update_vec.push(current);
                }
            }
        }
        update_vec
    }

    fn get_random_level(&self) -> usize {
        let mut rng = thread_rng();
        let mut level = 1;

        while rng.gen_bool(self.level_probability) && level < self.max_level {
            level += 1;
        }

        level
    }
}

impl<V> Drop for SkipList<V>
where
    V: Clone + Sized + Default + GetSize + Debug,
{
    fn drop(&mut self) {
        let mut current = Some(self.head);
        unsafe {
            while let Some(current_node) = current {
                let boxed_node = Box::from_raw(current_node.as_ptr());
                current = boxed_node.refs[0];
            }
        }
    }
}
//
// impl SkipList<i32, i32> {
//     pub fn print_levels(&self) {
//         for level in (0..self.max_level).rev() {
//             let mut line: Vec<String> = Vec::new();
//             let mut current = self.head.clone();
//
//             unsafe {
//                 while let Some(next_node) = current.as_ref().refs[level] {
//                     line.push(next_node.as_ref().key.to_string());
//                     current = next_node;
//                 }
//             }
//         }
//     }
// }

#[derive(Debug)]
pub struct Node<V>
where
    V: Clone + Sized + Default + GetSize + Debug,
{
    pub key: String,
    pub value: V,
    pub refs: Vec<Option<ListNode<V>>>,
}

impl<V> Node<V>
where
    V: Clone + Sized + Default + GetSize + Debug,
{
    pub fn new(key: String, value: V, level: usize) -> ListNode<V> {
        let boxed_node = Box::new(Node {
            key,
            value,
            refs: (0..level).map(|_| None).collect(),
        });
        NonNull::from(Box::leak(boxed_node))
    }

    pub fn get_memory_size(&self) -> usize {
        self.key.get_size()
            + self.value.get_size()
            + size_of::<Vec<Option<ListNode<V>>>>()
            + size_of::<Option<ListNode<V>>>() * self.refs.capacity()
    }
}
