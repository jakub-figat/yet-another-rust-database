use rand::{thread_rng, Rng};
use std::fmt::Debug;
use std::mem::size_of_val;
use std::ptr::NonNull;

// memtable, skiplist implementation
// TODO: commit log for preventing data loss on crash
// when max size is reached, flush to sstable with metadata
// default 32MB?
// try unsafe for better performacne?

pub static MEGABYTE: usize = usize::pow(2, 20);
static MEMTABLE_MAX_SIZE_MEGABYTES: usize = 128;

type ListNode<K, V> = NonNull<Node<K, V>>;

pub struct SkipList<K, V>
where
    K: Clone + Sized + MinusInf + PartialEq + Debug,
    V: Clone + Sized + Debug,
{
    pub head: ListNode<K, V>,
    pub max_level: usize,
    pub level_probability: f64,
    pub memory_size: usize,
    pub size: usize,
}

impl<K, V> SkipList<K, V>
where
    K: Clone + Sized + MinusInf + PartialEq + Debug,
    V: Clone + Sized + Default + Debug,
{
    pub fn new(max_level: usize, level_probability: f64) -> SkipList<K, V> {
        SkipList {
            head: Node::new(K::get_minus_inf(), V::default(), max_level),
            max_level,
            level_probability,
            memory_size: 0,
            size: 0,
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
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

    pub fn insert(&mut self, key: K, value: V) -> Result<(), String> {
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

            self.memory_size += size_of_val(&new_node);
        }

        self.size += 1;

        Ok(())
    }

    pub fn delete(&mut self, key: &K) -> Option<V> {
        let update_vec = self.get_update_vec(key, self.max_level);

        unsafe {
            if let Some(next_node) = (*update_vec.last().unwrap().as_ptr()).refs[0] {
                let next_key = &(*next_node.as_ptr()).key;
                if key == next_key {
                    self.memory_size -= size_of_val(&next_node);
                    self.size -= 1;
                    for (level, placement_node) in update_vec.iter().rev().enumerate() {
                        if &(*(*placement_node.as_ptr()).refs[level].unwrap().as_ptr()).key
                            != next_key
                        {
                            break;
                        }
                        (*placement_node.as_ptr()).refs[level] = (*next_node.as_ptr()).refs[level];
                    }
                    return Some((*next_node.as_ptr()).value.clone());
                }
            }
        }
        None
    }

    fn get_update_vec(&mut self, key: &K, level_limit: usize) -> Vec<ListNode<K, V>> {
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

impl SkipList<i32, i32> {
    pub fn print_levels(&self) {
        for level in (0..self.max_level).rev() {
            let mut line: Vec<String> = Vec::new();
            let mut current = self.head.clone();

            unsafe {
                while let Some(next_node) = current.as_ref().refs[level] {
                    line.push(next_node.as_ref().key.to_string());
                    current = next_node;
                }
            }

            println!("level {}", level + 1);
            println!("elements: {}", line.len());
            println!("{}", line.join("-"));
            println!("\n");
        }
    }
}

#[derive(Debug)]
pub struct Node<K, V>
where
    K: Clone + Sized + MinusInf + PartialEq + Debug,
    V: Clone + Sized + Debug,
{
    pub key: K,
    pub value: V,
    pub refs: Vec<Option<ListNode<K, V>>>,
}

impl<K, V> Node<K, V>
where
    K: Clone + Sized + MinusInf + PartialEq + Debug,
    V: Clone + Sized + Debug,
{
    pub fn new(key: K, value: V, level: usize) -> ListNode<K, V> {
        let boxed_node = Box::new(Node {
            key,
            value,
            refs: (0..level).map(|_| None).collect(),
        });
        NonNull::from(Box::leak(boxed_node))
        // TODO: memory cleanup
        // for every node:
        // put it back into box from raw pointer
        // move to next node
    }
}

pub trait MinusInf: PartialOrd {
    fn get_minus_inf() -> Self;
}

impl MinusInf for i32 {
    fn get_minus_inf() -> Self {
        i32::MIN
    }
}

impl MinusInf for usize {
    fn get_minus_inf() -> Self {
        usize::MIN
    }
}
