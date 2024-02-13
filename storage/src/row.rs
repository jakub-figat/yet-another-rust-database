use get_size::GetSize;
use std::cmp::Ordering;

pub struct Row<S, V>
where
    S: Default + GetSize + ToString,
    V: Default + GetSize,
{
    pub hash_key: String,
    pub sort_key: S,
    pub primary_key: String,
    value: V,
}

impl<S, V> Row<S, V>
where
    S: Default + GetSize + ToString,
    V: Default + GetSize,
{
    pub fn new(hash_key: String, sort_key: S, value: V) -> Row<S, V> {
        let sort_key_string = sort_key.to_string();
        let mut primary_key = String::with_capacity(hash_key.len() + sort_key_string.len() + 1);

        primary_key.push_str(&hash_key);
        primary_key.push_str(":");
        primary_key.push_str(&sort_key_string);

        Row {
            hash_key,
            sort_key,
            primary_key,
            value,
        }
    }
}

impl<S, V> PartialEq for Row<S, V>
where
    S: Default + GetSize + ToString,
    V: Default + GetSize,
{
    fn eq(&self, other: &Self) -> bool {
        self.primary_key == other.primary_key
    }
}

impl<S, V> PartialOrd for Row<S, V>
where
    S: Default + GetSize + ToString,
    V: Default + GetSize,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.primary_key.partial_cmp(&other.primary_key)
    }
}

impl<S, V> Default for Row<S, V>
where
    S: Default + GetSize + ToString,
    V: Default + GetSize,
{
    fn default() -> Self {
        Row {
            hash_key: "".to_string(),
            sort_key: S::default(),
            primary_key: "".to_string(),
            value: V::default(),
        }
    }
}

impl<S, V> GetSize for Row<S, V>
where
    S: Default + GetSize + ToString,
    V: Default + GetSize,
{
    fn get_size(&self) -> usize {
        self.hash_key.get_size()
            + self.sort_key.get_size()
            + self.primary_key.get_size()
            + self.value.get_size()
    }
}
