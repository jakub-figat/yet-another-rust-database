use std::cmp::Ordering::Less;
use std::mem::{size_of_val, swap};

pub static MEGABYTE: usize = usize::pow(2, 20);
static MEMTABLE_MAX_SIZE_MEGABYTES: usize = 64;

pub struct BalancedBST<K, V>
where
    K: Clone + Sized + Ord + PartialEq,
    V: Clone + Sized,
{
    number_of_nodes: usize,
    pub size: usize,
    pub root: Option<Box<Node<K, V>>>,
}

impl<K, V> BalancedBST<K, V>
where
    K: Clone + Sized + Ord + PartialEq,
    V: Clone + Sized,
{
    pub fn new() -> BalancedBST<K, V> {
        BalancedBST {
            number_of_nodes: 0,
            size: 0,
            root: None,
        }
    }

    pub fn get(&self, key: &K) -> Option<&Box<Node<K, V>>> {
        let mut current = &self.root;

        while let Some(current_node) = current {
            // testing
            // unsafe {
            //     COUNTER += 1;
            //     println!("{}", COUNTER);
            // }

            if key == &current_node.key {
                return Some(current_node);
            }

            match key.cmp(&current_node.key) {
                Less => current = &current_node.left,
                _ => current = &current_node.right,
            }
        }

        None
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut Box<Node<K, V>>> {
        let mut current = &mut self.root;

        while let Some(current_node) = current {
            if key == &current_node.key {
                return Some(current_node);
            }

            match key.cmp(&current_node.key) {
                Less => current = &mut current_node.left,
                _ => current = &mut current_node.right,
            }
        }

        None
    }

    pub fn insert(&mut self, key: K, value: V) -> Result<(), String> {
        if self.size > MEMTABLE_MAX_SIZE_MEGABYTES * MEGABYTE {
            Err(format!(
                "Memtable reached max size of {} MB",
                MEMTABLE_MAX_SIZE_MEGABYTES
            ))?
        }

        self.size += size_of_val(&key) + size_of_val(&value);
        self.number_of_nodes += 1;

        let mut current = &mut self.root;

        while let Some(current_node) = current {
            match key.cmp(&current_node.key) {
                Less => current = &mut current_node.left,
                _ => current = &mut current_node.right,
            }
        }

        *current = Some(Box::new(Node::new(key, value)));
        Ok(())
    }

    pub fn delete(&mut self, key: &K) -> Option<Box<Node<K, V>>> {
        let mut current = &mut self.root;
        while let Some(mut current_node) = current.take() {
            if key == &current_node.key {
                // 1. two childs
                if current_node.left.is_some() && current_node.right.is_some() {
                    let mut next_in_order = &mut current_node.right;

                    // follow left side until the end
                    while next_in_order.as_ref().unwrap().left.is_some() {
                        next_in_order = &mut next_in_order.as_mut().unwrap().left;
                    }

                    let mut removed_node = next_in_order.take().unwrap();

                    // if the leftmost node had a child, put it in the parent position
                    if let Some(right_node) = removed_node.right.take() {
                        *next_in_order = Some(right_node);
                    }

                    swap(&mut current_node.key, &mut removed_node.key);
                    swap(&mut current_node.value, &mut removed_node.value);
                    *current = Some(current_node);

                    return Some(removed_node);
                }

                // 2. no childs
                if current_node.left.is_none() && current_node.right.is_none() {
                    return Some(current_node);
                }

                // 3. one child, either left or right
                let next_node = current_node
                    .left
                    .take()
                    .unwrap_or(current_node.right.take().unwrap());
                *current = Some(next_node);

                return Some(current_node);
            }

            match key.cmp(&current_node.key) {
                Less => {
                    *current = Some(current_node);
                    current = &mut current.as_mut().unwrap().left;
                }
                _ => {
                    *current = Some(current_node);
                    current = &mut current.as_mut().unwrap().right;
                }
            }
        }

        None
    }

    pub fn as_in_order_vec(&self) -> Vec<&Box<Node<K, V>>> {
        let mut nodes = Vec::with_capacity(self.number_of_nodes);
        let mut stack = Vec::new();

        let mut current = self.root.as_ref();

        loop {
            while let Some(current_node) = current {
                stack.push(current_node);
                current = current_node.left.as_ref();
            }

            if let Some(next_node) = stack.pop() {
                nodes.push(next_node);
                current = next_node.right.as_ref();
            }

            if current.is_none() && stack.is_empty() {
                break;
            }
        }
        nodes
    }
}

#[derive(Clone, Debug)]
pub struct Node<K, V>
where
    K: Clone + Sized + Ord + PartialEq,
    V: Clone + Sized,
{
    pub key: K,
    pub value: V,
    pub left: Option<Box<Node<K, V>>>,
    pub right: Option<Box<Node<K, V>>>,
    pub balance_factor: usize,
}

impl<K, V> Node<K, V>
where
    K: Clone + Sized + Ord + PartialEq,
    V: Clone + Sized,
{
    pub fn new(key: K, value: V) -> Node<K, V> {
        Node {
            key,
            value,
            left: None,
            right: None,
            balance_factor: 0,
        }
    }
}
