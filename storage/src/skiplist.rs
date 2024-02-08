use rand::{thread_rng, Rng};
use std::cell::RefCell;
use std::fmt::Debug;
use std::mem::size_of_val;
use std::rc::Rc;

// memtable, skiplist implementation
// TODO: commit log for preventing data loss on crash
// when max size is reached, flush to sstable with metadata
// default 32MB?
// try unsafe for better performace?

pub static MEGABYTE: usize = usize::pow(2, 20);
static MEMTABLE_MAX_SIZE_MEGABYTES: usize = 64;

type ListNode<K, V> = Rc<RefCell<Node<K, V>>>;

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
        let mut current = self.head.clone();
        for level in (0..self.max_level).rev() {
            while let Some(next_node) = current.clone().borrow().refs[level].clone() {
                if key > &next_node.borrow().key {
                    current = next_node;
                } else {
                    break;
                }
            }
        }

        if let Some(next_node) = current.borrow().refs[0].clone() {
            if key == &next_node.borrow().key {
                return Some(next_node.borrow().value.clone());
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

        if let Some(next_node) = update_vec.last().unwrap().borrow().refs[0].clone() {
            if &key == &next_node.borrow().key {
                next_node.borrow_mut().key = key;
                return Ok(());
            }
        }

        let new_node = Node::new(key.clone(), value, new_level);
        for (level, placement_node) in update_vec.into_iter().rev().enumerate() {
            new_node.borrow_mut().refs[level] = placement_node.borrow().refs[level].clone();
            placement_node.borrow_mut().refs[level] = Some(new_node.clone());
        }

        self.size += 1;
        self.memory_size += size_of_val(&new_node);

        Ok(())
    }

    pub fn delete(&mut self, key: &K) -> Option<V> {
        let update_vec = self.get_update_vec(key, self.max_level);
        let next = update_vec.last().cloned().unwrap().borrow().refs[0].clone();
        if let Some(next_node) = next {
            let next_key = next_node.borrow().key.clone();
            if key == &next_key {
                self.memory_size -= size_of_val(&next_node);
                self.size -= 1;
                for (level, placement_node) in update_vec.iter().rev().enumerate() {
                    if placement_node.borrow().refs[level]
                        .as_ref()
                        .unwrap()
                        .borrow()
                        .key
                        != next_key
                    {
                        break;
                    }
                    placement_node.borrow_mut().refs[level] =
                        next_node.borrow().refs[level].clone();
                }
                return Some(next_node.borrow().value.clone());
            }
        }

        None
    }

    fn get_update_vec(&self, key: &K, level_limit: usize) -> Vec<ListNode<K, V>> {
        let mut update_vec = Vec::with_capacity(self.max_level);
        let mut current = self.head.clone();

        for level in (0..self.max_level).rev() {
            while let Some(next_node) = current.clone().borrow().refs[level].clone() {
                if key > &next_node.borrow().key {
                    current = next_node;
                } else {
                    break;
                }
            }

            if level < level_limit {
                update_vec.push(current.clone());
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

            while let Some(next_node) = current.clone().borrow().refs[level].clone() {
                line.push(next_node.borrow().key.to_string());
                current = next_node;
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
        Rc::new(RefCell::new(Node {
            key,
            value,
            refs: (0..level).map(|_| None).collect(),
        }))
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
