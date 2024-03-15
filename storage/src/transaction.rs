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
