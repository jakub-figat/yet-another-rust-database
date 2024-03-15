use crate::commit_log::CommitLog;
use crate::table::{Table, TableSchema};
use crate::util::{decode_row, encode_row, millis_from_epoch};
use crate::{Memtable, Row};
use futures::lock::Mutex;
use monoio::fs::{File, OpenOptions};
use std::collections::HashMap;
use std::fs::read_dir;
use std::sync::Arc;

pub static SSTABLES_PATH: &str = "/var/lib/yard/sstables";

pub struct SSTableSegment {
    table_schema: TableSchema,
    memtable_rows: Vec<Row>,
    partition_index: HashMap<usize, usize>,
}

impl SSTableSegment {
    pub fn new(
        table_schema: TableSchema,
        rows: Vec<Row>,
        partition_index: HashMap<usize, usize>,
    ) -> SSTableSegment {
        SSTableSegment {
            table_schema,
            memtable_rows: rows,
            partition_index,
        }
    }

    pub async fn write_to_disk(self) -> Result<(), String> {
        let partition_index_bytes = self
            .partition_index
            .into_iter()
            .map(|(partition, row_number)| format!("{}:{}", partition, row_number))
            .collect::<Vec<_>>()
            .join(",")
            .into_bytes();

        let num_of_rows = self.memtable_rows.len();
        let encoded_rows: Vec<_> = self
            .memtable_rows
            .into_iter()
            .map(|row| {
                let mut row_bytes = encode_row(&row, &self.table_schema);
                if row.marked_for_deletion {
                    row_bytes.push(1);
                } else {
                    row_bytes.push(0);
                }

                row_bytes
            })
            .reduce(|current, next| current.into_iter().chain(next.into_iter()).collect())
            .unwrap();

        let partition_index_length = partition_index_bytes.len() as u64;
        let file_name = format!(
            "{}/{}-{}-{}-{}",
            SSTABLES_PATH,
            self.table_schema.name,
            partition_index_length,
            num_of_rows,
            millis_from_epoch()
        );
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(file_name)
            .await
            .map_err(|e| e.to_string())?;

        file.write_all_at(partition_index_bytes, 0)
            .await
            .0
            .map_err(|e| e.to_string())?;

        file.write_all_at(encoded_rows, partition_index_length)
            .await
            .0
            .map_err(|e| e.to_string())?;
        file.sync_all().await.unwrap();

        Ok(())
    }
}

pub struct SSTableMetadata {
    pub file_path: String,
    pub table_name: String,
    pub partition_index_size: usize,
    pub number_of_rows: usize,
    pub timestamp: u128,
}

pub async fn flush_memtable_to_sstable(
    memtable: Memtable,
    commit_log: Arc<Mutex<CommitLog>>,
    table_schema: TableSchema,
    total_number_of_partitions: usize,
) {
    let (rows, partition_index) = memtable.to_sstable_rows(total_number_of_partitions);
    let sstable_segment = SSTableSegment::new(table_schema, rows, partition_index);
    if let Err(error) = sstable_segment.write_to_disk().await {
        tracing::error!("Failed to flush memtable to sstable: {}", error);
    }

    let mut commit_log = commit_log.lock().await;
    commit_log.delete().await;
}

pub async fn read_row_from_sstable(
    primary_key: &str,
    partition: usize,
    table: &Table,
) -> Option<Row> {
    let mut sstable_metadatas = get_sstables_metadata(&table.table_schema.name);
    sstable_metadatas.sort_by(|metadata1, metadata2| metadata2.timestamp.cmp(&metadata1.timestamp));

    for sstable_metadata in sstable_metadatas {
        let file = OpenOptions::new()
            .read(true)
            .open(sstable_metadata.file_path)
            .await
            .unwrap();

        let partition_index_bytes = vec![0u8; sstable_metadata.partition_index_size];
        let partition_index_bytes = file.read_exact_at(partition_index_bytes, 0).await.1;

        let partition_index: HashMap<_, _> = String::from_utf8(partition_index_bytes)
            .unwrap()
            .split(',')
            .map(|pair_string| {
                let (partition_string, row_number_string) = pair_string.split_once(":").unwrap();
                (
                    partition_string.parse().unwrap(),
                    row_number_string.parse().unwrap(),
                )
            })
            .collect();

        if let Some(row) = binary_search_row_in_file(
            primary_key,
            partition,
            file,
            partition_index,
            sstable_metadata.number_of_rows,
            &table.table_schema,
        )
        .await
        {
            return Some(row);
        }
    }

    None
}

async fn binary_search_row_in_file(
    primary_key: &str,
    partition: usize,
    file: File,
    partition_index: HashMap<usize, usize>,
    num_of_rows: usize,
    table_schema: &TableSchema,
) -> Option<Row> {
    if !partition_index.contains_key(&partition) {
        return None;
    }

    let mut left_row_number = partition_index[&partition];
    let mut right_row_number = match partition_index.keys().max().unwrap() == &partition {
        true => num_of_rows - 1,
        false => partition_index[&(partition + 1)],
    };

    let mut row_bytes = vec![0u8; table_schema.row_byte_size()];

    while left_row_number <= right_row_number {
        let current_row_number = (left_row_number + right_row_number) / 2;
        row_bytes = file
            .read_exact_at(
                row_bytes,
                (current_row_number * table_schema.row_byte_size()) as u64,
            )
            .await
            .1;
        let current_row = decode_row(&row_bytes, table_schema, true);

        if primary_key > current_row.primary_key.as_str() {
            left_row_number = current_row_number + 1;
        } else if primary_key < current_row.primary_key.as_str() {
            right_row_number = current_row_number - 1;
        } else {
            return Some(current_row);
        }
    }

    None
}

pub fn get_sstables_metadata(table_name: &str) -> Vec<SSTableMetadata> {
    read_dir(SSTABLES_PATH)
        .unwrap()
        .filter_map(|memtable_path| {
            let memtable_path = memtable_path.unwrap();
            let file_path = memtable_path.path().to_str().unwrap().to_string();
            let file_name = memtable_path.file_name().to_str().unwrap().to_string();
            let split_result: Vec<_> = file_name.split("-").collect();
            let file_table_name = split_result[0];
            let partition_index_size = split_result[1].parse::<usize>().unwrap();
            let number_of_rows = split_result[2].parse::<usize>().unwrap();
            let timestamp = split_result[3].parse::<u128>().unwrap();

            if table_name == file_table_name {
                return Some(SSTableMetadata {
                    file_path: file_path.to_string(),
                    table_name: file_table_name.to_string(),
                    partition_index_size,
                    number_of_rows,
                    timestamp,
                });
            }
            None
        })
        .collect()
}

// TODO: hash key max length
