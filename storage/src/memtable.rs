use crate::util::millis_from_epoch;
use crate::Row;
use get_size::GetSize;
use rand::{thread_rng, Rng};
use std::mem::size_of;
use std::ptr::NonNull;

pub static MEGABYTE: usize = usize::pow(2, 20);
static MEMTABLE_MAX_SIZE_MEGABYTES: usize = 16;

type ListNode = NonNull<Node>;

pub struct Memtable {
    pub head: ListNode,
    pub max_level: usize,
    pub level_probability: f64,
    pub memory_size: usize,
    pub size: usize,
}

impl Memtable {
    pub fn new(max_level: usize, level_probability: f64) -> Memtable {
        Memtable {
            head: Node::new(Row::default(), max_level),
            max_level,
            level_probability,
            memory_size: 0,
            size: 0,
        }
    }

    pub fn default() -> Memtable {
        Memtable {
            head: Node::new(Row::default(), 16),
            max_level: 16,
            level_probability: 0.5,
            memory_size: 0,
            size: 0,
        }
    }

    pub fn get(&self, primary_key: &String) -> Option<&Row> {
        let mut current = self.head;

        unsafe {
            for level in (0..self.max_level).rev() {
                while let Some(next_node) = (*current.as_ptr()).refs[level] {
                    if primary_key > &(*next_node.as_ptr()).row.primary_key {
                        current = next_node;
                    } else {
                        break;
                    }
                }
            }

            if let Some(next_node) = (*current.as_ptr()).refs[0].clone() {
                let node_dereferenced = &(*next_node.as_ptr());
                if primary_key == &node_dereferenced.row.primary_key
                    && !node_dereferenced.row.marked_for_deletion
                {
                    return Some(&(*next_node.as_ptr()).row);
                }
            }
        }

        None
    }

    pub fn insert(&mut self, row: Row, from_commit_log: bool) {
        let new_level = self.get_random_level();
        let update_vec = self.get_update_vec(&row.primary_key, new_level);

        unsafe {
            if let Some(next_node) = (*update_vec.last().unwrap().as_ptr()).refs[0] {
                if &row.primary_key == &(*next_node.as_ptr()).row.primary_key {
                    if from_commit_log {
                        if (*next_node.as_ptr()).row.timestamp >= row.timestamp {
                            return;
                        } else {
                            (*next_node.as_ptr()).row.timestamp = row.timestamp;
                        }
                    } else {
                        (*next_node.as_ptr()).row.timestamp = millis_from_epoch();
                        (*next_node.as_ptr()).row.version += 1;
                    }

                    (*next_node.as_ptr()).row.values = row.values;
                    (*next_node.as_ptr()).row.marked_for_deletion = false;

                    return;
                }
            }

            let new_node = Node::new(row, new_level);
            for (level, placement_node) in update_vec.into_iter().rev().enumerate() {
                (*new_node.as_ptr()).refs[level] = (*placement_node.as_ptr()).refs[level].clone();
                (*placement_node.as_ptr()).refs[level] = Some(new_node);
            }
            self.memory_size += (*new_node.as_ptr()).get_memory_size();
        }

        self.size += 1;
    }

    pub fn delete(&mut self, primary_key: &String, timestamp: Option<u128>) -> bool {
        let mut current = self.head;

        unsafe {
            for level in (0..self.max_level).rev() {
                while let Some(next_node) = (*current.as_ptr()).refs[level] {
                    if primary_key > &(*next_node.as_ptr()).row.primary_key {
                        current = next_node;
                    } else {
                        break;
                    }
                }
            }

            if let Some(next_node) = (*current.as_ptr()).refs[0].clone() {
                if primary_key == &(*next_node.as_ptr()).row.primary_key {
                    if let Some(timestamp) = timestamp {
                        if (*next_node.as_ptr()).row.timestamp >= timestamp {
                            return false;
                        } else {
                            (*next_node.as_ptr()).row.timestamp = timestamp;
                        }
                    } else {
                        (*next_node.as_ptr()).row.timestamp = millis_from_epoch();
                    }
                    (*next_node.as_ptr()).row.marked_for_deletion = true;
                    return true;
                }
            }
        }

        false
    }

    pub fn max_size_reached(&self) -> bool {
        self.memory_size > MEMTABLE_MAX_SIZE_MEGABYTES * MEGABYTE
    }

    fn get_update_vec(&mut self, primary_key: &String, level_limit: usize) -> Vec<ListNode> {
        let mut update_vec = Vec::with_capacity(self.max_level);

        unsafe {
            let mut current = self.head;
            for level in (0..self.max_level).rev() {
                while let Some(next_node) = (*current.as_ptr()).refs[level] {
                    if primary_key > &(*next_node.as_ptr()).row.primary_key {
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

    pub fn to_sstable_rows(self) -> Vec<Row> {
        let mut rows = Vec::with_capacity(self.size);

        unsafe {
            let mut current = (*self.head.as_ptr()).refs[0];
            while let Some(current_node) = current {
                let boxed_node = Box::from_raw(current_node.as_ptr());
                current = boxed_node.refs[0];
                rows.push(boxed_node.row);
            }
        }

        rows
    }
}

impl Drop for Memtable {
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

pub struct Node {
    pub row: Row,
    pub refs: Vec<Option<ListNode>>,
}

impl Node {
    pub fn new(row: Row, level: usize) -> ListNode {
        let boxed_node = Box::new(Node {
            row,
            refs: (0..level).map(|_| None).collect(),
        });
        NonNull::from(Box::leak(boxed_node))
    }

    pub fn get_memory_size(&self) -> usize {
        self.row.get_size()
            + size_of::<Vec<Option<ListNode>>>()
            + size_of::<Option<ListNode>>() * self.refs.capacity()
    }
}
