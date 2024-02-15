use get_size::GetSize;
use rand::{thread_rng, Rng};
use std::mem::size_of;
use std::ptr::NonNull;

pub static MEGABYTE: usize = usize::pow(2, 20);
static MEMTABLE_MAX_SIZE_MEGABYTES: usize = 64;

type ListNode<T> = NonNull<Node<T>>;

pub struct SkipList<T>
where
    T: PartialEq + PartialOrd + Default + GetSize,
{
    pub head: ListNode<T>,
    pub max_level: usize,
    pub level_probability: f64,
    pub memory_size: usize,
    pub size: usize,
}

impl<T> SkipList<T>
where
    T: PartialEq + PartialOrd + Default + GetSize,
{
    pub fn new(max_level: usize, level_probability: f64) -> SkipList<T> {
        SkipList {
            head: Node::new(T::default(), max_level),
            max_level,
            level_probability,
            memory_size: 0,
            size: 0,
        }
    }

    pub fn default() -> SkipList<T> {
        SkipList {
            head: Node::new(T::default(), 16),
            max_level: 16,
            level_probability: 0.5,
            memory_size: 0,
            size: 0,
        }
    }

    pub fn get(&self, value: &T) -> Option<&T> {
        let mut current = self.head;

        unsafe {
            for level in (0..self.max_level).rev() {
                while let Some(next_node) = (*current.as_ptr()).refs[level] {
                    if value > &(*next_node.as_ptr()).value {
                        current = next_node;
                    } else {
                        break;
                    }
                }
            }

            if let Some(next_node) = (*current.as_ptr()).refs[0].clone() {
                if value == &(*next_node.as_ptr()).value {
                    return Some(&(*next_node.as_ptr()).value);
                }
            }
        }

        None
    }

    pub fn insert(&mut self, value: T) -> Result<(), String> {
        if self.memory_size > MEMTABLE_MAX_SIZE_MEGABYTES * MEGABYTE {
            Err(format!(
                "Memtable reached max size of {} MB with {} nodes",
                MEMTABLE_MAX_SIZE_MEGABYTES, self.size
            ))?
        }

        let new_level = self.get_random_level();
        let update_vec = self.get_update_vec(&value, new_level);

        unsafe {
            if let Some(next_node) = (*update_vec.last().unwrap().as_ptr()).refs[0] {
                if &value == &(*next_node.as_ptr()).value {
                    (*next_node.as_ptr()).value = value;
                    return Ok(());
                }
            }

            let new_node = Node::new(value, new_level);
            for (level, placement_node) in update_vec.into_iter().rev().enumerate() {
                (*new_node.as_ptr()).refs[level] = (*placement_node.as_ptr()).refs[level].clone();
                (*placement_node.as_ptr()).refs[level] = Some(new_node);
            }
            self.memory_size += (*new_node.as_ptr()).get_memory_size();
        }

        self.size += 1;
        Ok(())
    }

    pub fn delete(&mut self, value: &T) -> Option<T> {
        let update_vec = self.get_update_vec(value, self.max_level);

        unsafe {
            if let Some(next_node) = (*update_vec.last().unwrap().as_ptr()).refs[0] {
                let next_value = &(*next_node.as_ptr()).value;
                if value == next_value {
                    let node_level = (*next_node.as_ptr()).refs.len();
                    for (level, placement_node) in
                        update_vec.iter().rev().take(node_level).enumerate()
                    {
                        if &(*(*placement_node.as_ptr()).refs[level].unwrap().as_ptr()).value
                            != next_value
                        {
                            break;
                        }
                        (*placement_node.as_ptr()).refs[level] = (*next_node.as_ptr()).refs[level];
                    }
                    let boxed_node = Box::from_raw(next_node.as_ptr());

                    self.memory_size -= (*next_node.as_ptr()).get_memory_size();
                    self.size -= 1;
                    return Some(boxed_node.value);
                }
            }
        }
        None
    }

    fn get_update_vec(&mut self, value: &T, level_limit: usize) -> Vec<ListNode<T>> {
        let mut update_vec = Vec::with_capacity(self.max_level);

        unsafe {
            let mut current = self.head;
            for level in (0..self.max_level).rev() {
                while let Some(next_node) = (*current.as_ptr()).refs[level] {
                    if value > &(*next_node.as_ptr()).value {
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

impl<T> Drop for SkipList<T>
where
    T: PartialEq + PartialOrd + Default + GetSize,
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

pub struct Node<T>
where
    T: PartialEq + PartialOrd + Default + GetSize,
{
    pub value: T,
    pub refs: Vec<Option<ListNode<T>>>,
}

impl<T> Node<T>
where
    T: PartialEq + PartialOrd + Default + GetSize,
{
    pub fn new(value: T, level: usize) -> ListNode<T> {
        let boxed_node = Box::new(Node {
            value,
            refs: (0..level).map(|_| None).collect(),
        });
        NonNull::from(Box::leak(boxed_node))
    }

    pub fn get_memory_size(&self) -> usize {
        self.value.get_size()
            + size_of::<Vec<Option<ListNode<T>>>>()
            + size_of::<Option<ListNode<T>>>() * self.refs.capacity()
    }
}
