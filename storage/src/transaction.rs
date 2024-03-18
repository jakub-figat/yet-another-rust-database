use crate::sstable::flush_memtable_to_sstable;
use crate::table::Table;
use crate::{Memtable, Row};
use futures::lock::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

pub struct Transaction {
    // MVCC transaction
    pub id: u64,
    // map of primary_key:version inside of map table_name:map
    affected_rows: HashMap<String, HashMap<String, u32>>,
    operations: HashMap<String, Vec<Operation>>,
    committed: bool,
}

impl Transaction {
    pub fn new(transaction_id: u64) -> Transaction {
        Transaction {
            id: transaction_id,
            affected_rows: HashMap::new(),
            operations: HashMap::new(),
            committed: false,
        }
    }

    pub fn get_for_update(&mut self, row: Option<&Row>, table_name: String) {
        if let Some(row) = row {
            self.add_affected_row(row, table_name);
        }
    }

    pub fn insert(&mut self, row: Row, table: &Table) {
        let table_name = table.table_schema.name.clone();

        if let Some(replaced_row) = table.memtable.get(&row.primary_key) {
            self.add_affected_row(replaced_row, table_name.clone());
        }

        self.operations
            .entry(table_name)
            .or_insert(Vec::new())
            .push(Operation::Insert(row));
    }

    pub fn delete(&mut self, primary_key: String, table: &Table) -> bool {
        let table_name = table.table_schema.name.clone();

        if let Some(deleted_row) = table.memtable.get(&primary_key) {
            self.add_affected_row(deleted_row, table_name.clone());
            self.operations
                .entry(table_name)
                .or_insert(Vec::new())
                .push(Operation::Delete(primary_key));
            return true;
        }

        false
    }

    pub fn add_affected_row(&mut self, row: &Row, table_name: String) {
        let table_affected_rows = self
            .affected_rows
            .entry(table_name)
            .or_insert(HashMap::new());
        if !table_affected_rows.contains_key(&row.primary_key) {
            table_affected_rows.insert(row.primary_key.clone(), row.version);
        }
    }

    pub async fn can_commit(&self, tables: Arc<Mutex<HashMap<String, Table>>>) -> bool {
        let tables = tables.lock().await;
        for (table_name, affected_row_versions) in &self.affected_rows {
            let memtable = &tables.get(table_name).unwrap().memtable;

            for (primary_key, version) in affected_row_versions {
                match memtable.get(&primary_key) {
                    Some(memtable_row) => {
                        if version != &memtable_row.version {
                            return false;
                        }
                    }
                    None => {
                        return false;
                    }
                }
            }
        }
        true
    }

    pub async fn commit(
        &mut self,
        tables: Arc<Mutex<HashMap<String, Table>>>,
        total_number_of_partitions: usize,
    ) {
        let mut tables = tables.lock().await;
        self.committed = true;

        for (table_name, operations) in &self.operations {
            let table = tables.get_mut(table_name).unwrap();
            for operation in operations {
                match operation {
                    Operation::Insert(row) => {
                        table.memtable.insert(row.clone(), false);
                        if table.memtable.max_size_reached() {
                            let mut full_memtable = Memtable::default();
                            std::mem::swap(&mut table.memtable, &mut full_memtable);

                            monoio::spawn(flush_memtable_to_sstable(
                                full_memtable,
                                table.commit_log.clone(),
                                table.table_schema.clone(),
                                total_number_of_partitions,
                            ));
                        }
                    }
                    Operation::Delete(primary_key) => {
                        table.memtable.delete(primary_key, None);
                    }
                }
            }
        }
    }
}

enum Operation {
    Insert(Row),
    Delete(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit_log::CommitLog;
    use crate::table::{ColumnType, TableSchema};
    use common::value::Value::{Int32, Varchar};
    use rand::{thread_rng, RngCore};

    fn get_table() -> Table {
        let table_schema = TableSchema::new("table".to_string(), ColumnType::Varchar(100));
        let memtable = Memtable::default();
        let commit_log = CommitLog {
            file: None,
            file_path: "test".to_string(),
            file_offset: 0,
            table_schema: table_schema.clone(),
            partition: 0,
            closed: true,
        };
        Table {
            memtable,
            commit_log: Arc::new(Mutex::new(commit_log)),
            table_schema,
        }
    }

    fn get_row(key: &str) -> Row {
        Row::new(key.to_string(), Varchar(key.to_string()), HashMap::new())
    }

    fn get_new_transaction() -> Transaction {
        let mut rng = thread_rng();
        Transaction::new(rng.next_u64())
    }

    #[test]
    fn test_get_for_update() {
        let row = get_row("1");

        let mut transaction = get_new_transaction();
        transaction.get_for_update(Some(&row), "table".to_string());

        let row_version = transaction.affected_rows["table"][&row.primary_key];
        assert_eq!(row_version, 1);
    }

    #[test]
    fn test_insert_delete() {
        let mut transaction = get_new_transaction();
        let table = get_table();
        let row = get_row("1");

        transaction.insert(row.clone(), &table);
        transaction.delete("some_delete".to_string(), &table);

        assert_eq!(transaction.affected_rows, HashMap::new());
        assert_eq!(transaction.operations["table"].len(), 1);
    }

    #[test]
    fn test_insert_delete_when_data_in_memtable() {
        let mut transaction = get_new_transaction();
        let mut table = get_table();

        let row = get_row("1");
        table.memtable.insert(row.clone(), false);

        transaction.insert(row.clone(), &table);
        transaction.delete(row.primary_key.clone(), &table);

        let row_version = transaction.affected_rows["table"][&row.primary_key];
        assert_eq!(row_version, 1);
        assert_eq!(transaction.operations["table"].len(), 2);
    }

    #[monoio::test]
    async fn test_commit_transaction() {
        let mut table = get_table();

        let mut row = get_row("1");
        let row_2 = get_row("2");

        table.memtable.insert(row.clone(), false);
        table.memtable.insert(row_2.clone(), false);

        row.values.insert("a".to_string(), Int32(1));
        let mut transaction = get_new_transaction();
        transaction.insert(row.clone(), &table);
        transaction.delete(row_2.primary_key.clone(), &table);

        let tables = HashMap::from([("table".to_string(), table)]);
        let tables = Arc::new(Mutex::new(tables));

        assert!(transaction.can_commit(tables.clone()).await);
        transaction.commit(tables.clone(), 1).await;
        {
            let tables = tables.lock().await;
            let table = tables.get("table").unwrap();

            let modified_row = table.memtable.get(&"1:1".to_string()).unwrap();
            let deleted_row = table.memtable.get(&"2:2".to_string());
            assert_eq!(modified_row.values["a"], Int32(1));
            assert_eq!(modified_row.version, 2);
            assert!(deleted_row.is_none());
        }
    }

    #[monoio::test]
    async fn test_get_for_update_cannot_commit_after_insert() {
        let mut table = get_table();

        let mut row = get_row("1");
        table.memtable.insert(row.clone(), false);

        let mut transaction = get_new_transaction();
        transaction.get_for_update(Some(&row), "table".to_string());
        row.values.insert("a".to_string(), Int32(1));
        table.memtable.insert(row.clone(), false);

        let tables = Arc::new(Mutex::new(HashMap::from([("table".to_string(), table)])));
        assert!(!transaction.can_commit(tables).await);
    }

    #[monoio::test]
    async fn test_delete_cannot_commit_after_insert() {
        let mut table = get_table();

        let mut row = get_row("1");
        table.memtable.insert(row.clone(), false);

        let mut transaction = get_new_transaction();
        transaction.delete(row.primary_key.clone(), &table);

        row.values.insert("a".to_string(), Int32(1));
        table.memtable.insert(row.clone(), false);

        let tables = Arc::new(Mutex::new(HashMap::from([("table".to_string(), table)])));
        assert!(!transaction.can_commit(tables).await);
    }

    #[monoio::test]
    async fn test_two_transactions_simultaneously_commit() {
        let table = get_table();
        let row = get_row("1");
        let row_2 = get_row("2");

        let mut transaction_1 = get_new_transaction();
        let mut transaction_2 = get_new_transaction();

        transaction_1.insert(row.clone(), &table);
        transaction_2.insert(row_2.clone(), &table);

        let tables = Arc::new(Mutex::new(HashMap::from([("table".to_string(), table)])));
        assert!(transaction_1.can_commit(tables.clone()).await);
        assert!(transaction_2.can_commit(tables.clone()).await);

        transaction_1.commit(tables.clone(), 1).await;
        assert!(transaction_2.can_commit(tables.clone()).await);
        transaction_2.commit(tables.clone(), 1).await;

        let tables_lock = tables.lock().await;
        let table = tables_lock.get("table").unwrap();
        assert!(table.memtable.get(&row.primary_key).is_some());
        assert!(table.memtable.get(&row_2.primary_key).is_some());
    }
}
