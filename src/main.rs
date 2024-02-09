fn main() {

}

#[cfg(test)]
mod tests {
    use storage::SkipList;

    #[test]
    fn test_list() {
        {
            let mut list = SkipList::new(16, 0.5);
            let nums: Vec<_> = (1..=i32::pow(10, 3)).collect();

            for num in nums.iter() {
                list.insert(num.clone(), num.clone()).unwrap();
            }

            list.get(&777).unwrap();
            list.delete(&777).unwrap();
            list.insert(777, 1).unwrap();
            list.get(&777).unwrap();
        }
    }
}
