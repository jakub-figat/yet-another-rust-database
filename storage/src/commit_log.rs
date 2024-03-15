use crate::sstable::flush_memtable_to_sstable;
use crate::table::TableSchema;
use crate::util::{decode_row, encode_row, millis_from_epoch};
use crate::{Memtable, Row, MEGABYTE};
use futures::lock::Mutex;
use monoio::fs::{File, OpenOptions};
use monoio::time::sleep;
use rand::Rng;
use std::collections::HashSet;
use std::fs::read_dir;
use std::mem::size_of;
use std::sync::Arc;
use std::time::Duration;

static COMMIT_LOG_SEGMENTS_FILE_PATH: &str = "/var/lib/yard/commit_logs";

pub struct CommitLog {
    file: Option<File>,
    file_path: String,
    pub file_offset: u64,
    table_schema: TableSchema,
    pub partition: usize,
    pub closed: bool,
}

impl CommitLog {
    pub async fn open_new(table_schema: &TableSchema, partitions: &HashSet<usize>) -> CommitLog {
        let mut rng = rand::thread_rng();
        let partition = rng.gen_range(0usize..partitions.iter().max().unwrap().clone());

        let file_path = format!(
            "{}/{}-{}-{}",
            COMMIT_LOG_SEGMENTS_FILE_PATH,
            table_schema.name,
            partition,
            millis_from_epoch()
        );

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(&file_path)
            .await
            .unwrap();

        CommitLog {
            file: Some(file),
            file_path,
            file_offset: 0,
            table_schema: table_schema.clone(),
            partition,
            closed: false,
        }
    }

    pub async fn write_insert(&mut self, row: &Row) {
        let mut operation_bytes = Vec::new();
        operation_bytes.push(1u8);

        let mut row_bytes = encode_row(row, &self.table_schema);
        operation_bytes.append(&mut row_bytes);
        operation_bytes.push(b'\n');

        let bytes_len = operation_bytes.len() as u64;
        self.file
            .as_ref()
            .unwrap()
            .write_all_at(operation_bytes, self.file_offset)
            .await
            .0
            .unwrap();
        self.file_offset += bytes_len;
    }

    pub async fn write_delete(&mut self, primary_key: &str) {
        let mut operation_bytes = Vec::new();
        operation_bytes.push(2u8);

        let mut timestamp_bytes = millis_from_epoch().to_be_bytes().to_vec();
        operation_bytes.append(&mut timestamp_bytes);

        let mut primary_key_bytes = primary_key.as_bytes().to_vec();
        operation_bytes.append(&mut primary_key_bytes);

        operation_bytes.push(b'\n');

        let bytes_len = operation_bytes.len() as u64;
        self.file
            .as_ref()
            .unwrap()
            .write_all_at(operation_bytes, self.file_offset)
            .await
            .0
            .unwrap();
        self.file_offset += bytes_len;
    }

    pub async fn sync(&self) {
        self.file.as_ref().unwrap().sync_all().await.unwrap();
    }

    pub async fn delete(&mut self) {
        let file = self.file.take().unwrap();
        file.close().await.unwrap();

        // ugly, blocking, monoio does not seem to provide a way to do it non-blocking
        std::fs::remove_file(&self.file_path).unwrap();
    }
}

pub async fn periodically_sync_commit_log(commit_log: Arc<Mutex<CommitLog>>, interval: Duration) {
    loop {
        sleep(interval).await;
        let commit_log = commit_log.lock().await;
        if !commit_log.closed {
            commit_log.sync().await;
        } else {
            break;
        }
    }
}

pub async fn replay_commit_logs(
    table_schema: &TableSchema,
    partitions: &HashSet<usize>,
    total_number_of_partitions: usize,
) {
    let commit_logs = open_for_startup(table_schema, partitions).await;
    let mut buffer = Vec::with_capacity(24 * MEGABYTE);

    for mut commit_log in commit_logs {
        let (result, mut new_buffer) = commit_log.file.as_ref().unwrap().read_at(buffer, 0).await;

        let bytes_read = result.unwrap();
        commit_log.file_offset = bytes_read as u64;

        let byte_operations = split_by_newline(&new_buffer);
        let mut memtable = Memtable::default();
        for operation_bytes in byte_operations {
            match operation_bytes[0] {
                1 => {
                    let row = decode_row(&operation_bytes[1..], table_schema);
                    memtable.insert(row, true);
                }
                2 => {
                    let timestamp_size = size_of::<u128>();
                    let timestamp = u128::from_be_bytes(
                        operation_bytes[1..1 + timestamp_size]
                            .to_vec()
                            .try_into()
                            .unwrap(),
                    );
                    let primary_key =
                        String::from_utf8(operation_bytes[1 + timestamp_size..].to_vec()).unwrap();
                    memtable.delete(&primary_key, Some(timestamp));
                }
                _ => panic!("Invalid operation code"),
            }
        }

        monoio::spawn(flush_memtable_to_sstable(
            memtable,
            Arc::new(Mutex::new(commit_log)),
            table_schema.clone(),
            total_number_of_partitions,
        ));

        new_buffer.clear();
        buffer = new_buffer;
    }
}

async fn open_for_startup(
    table_schema: &TableSchema,
    partitions: &HashSet<usize>,
) -> Vec<CommitLog> {
    let mut commit_logs = Vec::new();
    let mut commit_log_files =
        get_commit_logs_filenames_with_metadata(&table_schema.name, partitions);
    commit_log_files.sort_by(|(_, _, timestamp1), (_, _, timestamp2)| timestamp1.cmp(timestamp2));

    for (filename, file_partition, _) in commit_log_files {
        let file_path = format!("{}/{}", COMMIT_LOG_SEGMENTS_FILE_PATH, filename);
        let file = OpenOptions::new()
            .read(true)
            .open(&file_path)
            .await
            .unwrap();
        commit_logs.push(CommitLog {
            file: Some(file),
            file_path,
            file_offset: 0,
            partition: file_partition,
            table_schema: table_schema.clone(),
            closed: false,
        });
    }

    commit_logs
}

fn get_commit_logs_filenames_with_metadata(
    table_name: &str,
    partitions: &HashSet<usize>,
) -> Vec<(String, usize, u128)> {
    read_dir(COMMIT_LOG_SEGMENTS_FILE_PATH)
        .unwrap()
        .filter_map(|commit_log_path| {
            let file_name = commit_log_path
                .unwrap()
                .file_name()
                .to_str()
                .unwrap()
                .to_string();
            let split_result: Vec<_> = file_name.split("-").collect();
            let file_table_name = split_result[0];
            let file_partition = split_result[1].parse::<usize>().unwrap();
            let file_timestamp = split_result[2].parse::<u128>().unwrap();

            if table_name == file_table_name && partitions.contains(&file_partition) {
                return Some((file_name, file_partition, file_timestamp));
            }
            None
        })
        .collect()
}

fn split_by_newline(data: &Vec<u8>) -> Vec<Vec<u8>> {
    let newline_byte = b'\n';
    let mut result = Vec::new();
    let mut current_line = Vec::new();

    for byte in data {
        if byte == &newline_byte {
            if !current_line.is_empty() {
                result.push(current_line.clone());
                current_line.clear();
            }
        } else {
            current_line.push(byte.clone());
        }
    }

    if !current_line.is_empty() {
        result.push(current_line);
    }

    result
}
