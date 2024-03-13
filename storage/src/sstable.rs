use crate::table::TableSchema;
use crate::{Memtable, Row, HASH_KEY_MAX_SIZE};
use monoio::fs::OpenOptions;
use rand::{thread_rng, RngCore};
use std::io::{BufWriter, Write};

static SSTABLES_PATH: &str = "/var/lib/yard/sstables";

pub struct SSTableSegment {
    table_schema: TableSchema,
    memtable_rows: Vec<Row>,
    partition: usize,
}

impl SSTableSegment {
    pub fn new(table_schema: TableSchema, rows: Vec<Row>, partition: usize) -> SSTableSegment {
        SSTableSegment {
            table_schema,
            memtable_rows: rows,
            partition,
        }
    }

    pub async fn write_to_disk(self) -> Result<(), String> {
        let encoded_rows: Vec<_> = self
            .memtable_rows
            .into_iter()
            .map(|row| encode_row(row, &self.table_schema))
            .reduce(|current, next| current.into_iter().chain(next.into_iter()).collect())
            .unwrap();

        let mut rng = thread_rng();
        let file_name = format!(
            "{}/{}-{}-{}",
            SSTABLES_PATH,
            self.table_schema.name,
            self.partition,
            rng.next_u64()
        );
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(file_name)
            .await
            .map_err(|e| e.to_string())?;

        file.write_all_at(encoded_rows, 0)
            .await
            .0
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    fn get_row_length(&self) -> usize {
        let sort_key_size = self.table_schema.sort_key_type.byte_size();
        let columns_size: usize = self
            .table_schema
            .columns
            .iter()
            .map(|(_, column)| column.column_type.byte_size())
            .sum();

        HASH_KEY_MAX_SIZE + sort_key_size + columns_size
    }
}

pub async fn flush_memtable_to_sstable(
    memtable: Memtable,
    table_schema: TableSchema,
    partition: usize,
) {
    let sstable_segment = SSTableSegment::new(table_schema, memtable.to_sstable_rows(), partition);
    if let Err(error) = sstable_segment.write_to_disk().await {
        tracing::error!("Failed to flush memtable to sstable: {}", error);
    }
}

fn encode_row(mut row: Row, table_schema: &TableSchema) -> Vec<u8> {
    let mut bytes = Vec::new();

    let mut hash_key_bytes = BufWriter::new(vec![0u8; HASH_KEY_MAX_SIZE]);
    hash_key_bytes.write_all(row.hash_key.as_bytes()).unwrap();
    bytes.append(hash_key_bytes.get_mut());

    let mut sort_key_bytes = row.sort_key.to_bytes();
    bytes.append(&mut sort_key_bytes);

    for name in table_schema.columns.keys() {
        let mut value_bytes = row.values.remove(name).unwrap().to_bytes();
        bytes.append(&mut value_bytes);
    }

    bytes
}

// TODO: hash key max length: 64
