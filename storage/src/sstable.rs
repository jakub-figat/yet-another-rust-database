use crate::commit_log::CommitLog;
use crate::table::{Table, TableSchema};
use crate::util::{decode_row, encode_row, millis_from_epoch};
use crate::{Memtable, Row};
use futures::lock::Mutex;
use monoio::fs::{File, OpenOptions};
use std::fs::read_dir;
use std::sync::Arc;

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
        let num_of_rows = self.memtable_rows.len();
        let encoded_rows: Vec<_> = self
            .memtable_rows
            .into_iter()
            .map(|row| {
                let mut row_bytes = encode_row(&row, &self.table_schema);

                let mut timestamp_bytes = row.timestamp.to_be_bytes().to_vec();
                row_bytes.append(&mut timestamp_bytes);

                if row.marked_for_deletion {
                    row_bytes.push(1);
                } else {
                    row_bytes.push(0);
                }

                row_bytes
            })
            .reduce(|current, next| current.into_iter().chain(next.into_iter()).collect())
            .unwrap();

        let file_name = format!(
            "{}/{}-{}-{}-{}",
            SSTABLES_PATH,
            self.table_schema.name,
            self.partition,
            num_of_rows,
            millis_from_epoch()
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
        file.sync_all().await.unwrap();

        Ok(())
    }
}

pub async fn flush_memtable_to_sstable(
    memtable: Memtable,
    commit_log: Arc<Mutex<CommitLog>>,
    table_schema: TableSchema,
    partition: usize,
) {
    let sstable_segment = SSTableSegment::new(table_schema, memtable.to_sstable_rows(), partition);
    if let Err(error) = sstable_segment.write_to_disk().await {
        tracing::error!("Failed to flush memtable to sstable: {}", error);
    }

    let mut commit_log = commit_log.lock().await;
    commit_log.delete().await;
}

pub async fn read_row_from_sstable(
    primary_key: &str,
    table: &Table,
    current_partition: usize,
    num_of_partitions: usize,
) -> Option<Row> {
    let mut memtable_file_names = get_sstables_filenames_with_metadata(
        &table.table_schema.name,
        current_partition,
        num_of_partitions,
    );
    memtable_file_names
        .sort_by(|(_, _, timestamp1), (_, _, timestamp2)| timestamp2.cmp(timestamp1));

    for (sstable_filename, num_of_rows, _) in memtable_file_names {
        let file = OpenOptions::new()
            .read(true)
            .open(format!("{}/{}", SSTABLES_PATH, sstable_filename))
            .await
            .unwrap();

        if let Some(row) =
            binary_search_row_in_file(primary_key, file, num_of_rows, &table.table_schema).await
        {
            return Some(row);
        }
    }

    None
}

async fn binary_search_row_in_file(
    primary_key: &str,
    file: File,
    num_of_rows: usize,
    table_schema: &TableSchema,
) -> Option<Row> {
    let mut left_row_number = 0;
    let mut right_row_number = num_of_rows - 1;
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

fn get_sstables_filenames_with_metadata(
    table_name: &str,
    current_partition: usize,
    num_of_partitions: usize,
) -> Vec<(String, usize, u128)> {
    read_dir(SSTABLES_PATH)
        .unwrap()
        .filter_map(|memtable_path| {
            let file_name = memtable_path
                .unwrap()
                .file_name()
                .to_str()
                .unwrap()
                .to_string();
            let split_result: Vec<_> = file_name.split("-").collect();
            let file_table_name = split_result[0];
            let file_partition = split_result[1].parse::<usize>().unwrap();
            let num_of_rows = split_result[2].parse::<usize>().unwrap();
            let file_timestamp = split_result[3].parse::<u128>().unwrap();

            if table_name == file_table_name
                && current_partition == (file_partition % num_of_partitions)
            {
                return Some((file_name, num_of_rows, file_timestamp));
            }
            None
        })
        .collect()
}

// TODO: hash key max length: 64
