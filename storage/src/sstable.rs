use crate::commit_log::CommitLog;
use crate::table::{Table, TableSchema};
use crate::util::{decode_row, encode_row, millis_from_epoch};
use crate::{Memtable, Row, MEGABYTE};
use futures::channel::mpsc::Receiver;
use futures::channel::oneshot;
use futures::lock::Mutex;
use futures::StreamExt;
use monoio;
use monoio::fs::{File, OpenOptions};
use monoio::time::sleep;
use std::collections::HashMap;
use std::fs::read_dir;
use std::os::unix::fs::MetadataExt;
use std::sync::Arc;
use std::time::Duration;

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
            .map(|row| encode_row(&row, &self.table_schema))
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
    pub file_size: u64,
}

pub async fn flush_memtable_to_sstable(
    memtable: Memtable,
    commit_log: Arc<Mutex<CommitLog>>,
    table_schema: TableSchema,
    total_number_of_partitions: usize,
) {
    let (rows, partition_index) = memtable.to_sstable_rows(total_number_of_partitions, false);
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
        let current_row = decode_row(&row_bytes, table_schema);

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
            let file_size = memtable_path.metadata().unwrap().size();
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
                    file_size,
                });
            }
            None
        })
        .collect()
}

pub async fn compact_sstables(table_schema: &TableSchema, total_number_of_partitions: usize) {
    // size tiered compaction
    let bucket_low = 0.5;
    let bucket_high = 1.5;
    let bucket_size_range = 4..=32;
    let sstable_min_size = (50 * MEGABYTE) as f64;

    let mut sstables_metadatas = get_sstables_metadata(&table_schema.name);
    sstables_metadatas
        .sort_by(|metadata1, metadata2| metadata2.file_size.cmp(&metadata1.file_size));

    let mut buckets = Vec::new();
    'outer: for sstable_metadata in sstables_metadatas {
        let sstable_size = sstable_metadata.file_size as f64;
        for bucket in buckets.iter_mut() {
            let bucket_average_size = get_bucket_average_size(&bucket);
            let bucket_range =
                (bucket_low * bucket_average_size)..(bucket_high * bucket_average_size);
            if bucket_range.contains(&sstable_size)
                || (sstable_size < sstable_min_size && bucket_average_size < sstable_min_size)
            {
                bucket.push(sstable_metadata);
                continue 'outer;
            }
        }

        buckets.push(vec![sstable_metadata]);
    }

    for bucket in buckets {
        if bucket_size_range.contains(&bucket.len()) {
            compact_bucket(bucket, table_schema, total_number_of_partitions).await;
        }
    }
}

fn get_bucket_average_size(bucket: &Vec<SSTableMetadata>) -> f64 {
    let bucket_total_size: u64 = bucket.iter().map(|metadata| metadata.file_size).sum();
    bucket_total_size as f64 / bucket.len() as f64
}

async fn compact_bucket(
    bucket: Vec<SSTableMetadata>,
    table_schema: &TableSchema,
    total_number_of_partitions: usize,
) {
    let mut memtable = Memtable::default();

    for sstable_metadata in bucket.iter() {
        let file = OpenOptions::new()
            .read(true)
            .open(&sstable_metadata.file_path)
            .await
            .unwrap();

        let content_buffer = Vec::with_capacity(
            sstable_metadata.file_size as usize - sstable_metadata.partition_index_size,
        );
        let content_buffer = file
            .read_exact_at(content_buffer, sstable_metadata.partition_index_size as u64)
            .await
            .1;

        let mut offset = sstable_metadata.partition_index_size;
        while offset != content_buffer.len() {
            let row = decode_row(
                &content_buffer[offset..offset + table_schema.row_byte_size()],
                table_schema,
            );
            memtable.insert(row, true);

            offset += table_schema.row_byte_size();
        }
    }

    let (rows, partition_index) = memtable.to_sstable_rows(total_number_of_partitions, true);

    let sstable_segment = SSTableSegment::new(table_schema.clone(), rows, partition_index);
    if let Err(error) = sstable_segment.write_to_disk().await {
        tracing::error!("Failed to flush memtable to sstable: {}", error);
    }

    for sstable_metadata in bucket {
        std::fs::remove_file(sstable_metadata.file_path).unwrap();
    }
}

pub async fn compaction_main(
    mut ctrl_c_receiver: Receiver<oneshot::Sender<()>>,
    table_schemas: Vec<TableSchema>,
    total_number_of_partitions: usize,
) {
    let interval = Duration::from_secs(60);
    loop {
        monoio::select! {
            _ = sleep(interval) => {
                for schema in &table_schemas {
                    conditionally_compact_table_sstables(schema, total_number_of_partitions).await;
                }
            }
            Some(ctrl_c_sender) = ctrl_c_receiver.next() => {
                ctrl_c_sender.send(()).unwrap();
                break;
            }
        }
    }
}

async fn conditionally_compact_table_sstables(
    table_schema: &TableSchema,
    total_number_of_partitions: usize,
) {
    let sstable_metadatas = get_sstables_metadata(&table_schema.name);

    if sstable_metadatas.len() > 4 {
        compact_sstables(table_schema, total_number_of_partitions).await;
    }
}
