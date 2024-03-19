use self::ColumnType::*;
use crate::commit_log::{periodically_sync_commit_log, CommitLog};
use crate::sstable::{flush_memtable_to_sstable, get_sstables_metadata};
use crate::{Memtable, HASH_KEY_BYTE_SIZE};
use futures::lock::Mutex;
use monoio::fs::OpenOptions;
use regex::Regex;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::mem::size_of;
use std::sync::Arc;
use std::time::Duration;

pub static TABLE_SCHEMAS_DIR: &str = "/var/lib/yard";
pub static TABLE_SCHEMAS_FILE_PATH: &str = "var/lib/yard/table_schemas";

pub struct Table {
    pub memtable: Memtable,
    pub commit_log: Arc<Mutex<CommitLog>>,
    pub table_schema: TableSchema,
}

impl Table {
    pub fn new(memtable: Memtable, commit_log: CommitLog, table_schema: TableSchema) -> Table {
        let commit_log = Arc::new(Mutex::new(commit_log));
        monoio::spawn(periodically_sync_commit_log(
            commit_log.clone(),
            Duration::from_secs(10),
        ));

        Table {
            memtable,
            commit_log,
            table_schema,
        }
    }

    pub async fn flush_memtable_to_disk(
        &mut self,
        partitions: &HashSet<usize>,
        total_number_of_partitions: usize,
    ) {
        {
            let mut commit_log = self.commit_log.lock().await;
            commit_log.closed = true;
        }

        let mut full_memtable = Memtable::default();
        let mut old_commit_log = Arc::new(Mutex::new(
            CommitLog::open_new(&self.table_schema, partitions).await,
        ));

        std::mem::swap(&mut self.commit_log, &mut old_commit_log);
        std::mem::swap(&mut self.memtable, &mut full_memtable);

        monoio::spawn(periodically_sync_commit_log(
            self.commit_log.clone(),
            Duration::from_secs(10),
        ));
        monoio::spawn(flush_memtable_to_sstable(
            full_memtable,
            old_commit_log,
            self.table_schema.clone(),
            total_number_of_partitions,
        ));
    }
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub sort_key_type: ColumnType,
    pub columns: BTreeMap<String, Column>,
}

impl TableSchema {
    pub fn new(table_name: String, sort_key_type: ColumnType) -> TableSchema {
        TableSchema {
            name: table_name,
            sort_key_type,
            columns: BTreeMap::new(),
        }
    }

    pub fn from_string(schema_string: &str) -> Result<TableSchema, String> {
        let (table_name, columns_string) = schema_string
            .split_once('>')
            .ok_or("Invalid schema string".to_string())?;
        let mut columns = BTreeMap::new();
        for column_string in columns_string.split(";") {
            let (column_name, mut column_type_string) = column_string
                .split_once(':')
                .ok_or("Invalid schema string".to_string())?;

            let mut nullable = false;
            if let Some((type_string, _)) = column_type_string.split_once('?') {
                if column_name == "sort_key" {
                    return Err("sort_key cannot be nullable".to_string());
                }
                column_type_string = type_string;
                nullable = true;
            }

            columns.insert(
                column_name.to_string(),
                Column {
                    column_type: ColumnType::from_string(column_type_string)?,
                    nullable,
                },
            );
        }

        let sort_key_column = columns
            .remove("sort_key")
            .ok_or("Invalid first column, should be 'sort_key'".to_string())?;
        Ok(TableSchema {
            name: table_name.to_string(),
            sort_key_type: sort_key_column.column_type,
            columns,
        })
    }

    pub fn row_byte_size(&self) -> usize {
        let values_byte_size: usize = self
            .columns
            .values()
            .map(|column| column.column_type.byte_size())
            .sum();

        // hash_key, sort_key, values, timestamp, tombstone
        HASH_KEY_BYTE_SIZE
            + self.sort_key_type.byte_size()
            + values_byte_size
            + size_of::<u128>()
            + 1
    }
}

impl Display for TableSchema {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut columns = Vec::new();
        columns.push(format!("{}:{}", "sort_key", self.sort_key_type.to_string()));
        for (name, column) in &self.columns {
            columns.push(format!("{}:{}", name, column.to_string()));
        }
        write!(f, "{}>{}", self.name, columns.join(";"))
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub column_type: ColumnType,
    pub nullable: bool,
}

impl Column {
    pub fn new(column_type: ColumnType, nullable: bool) -> Column {
        Column {
            column_type,
            nullable,
        }
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut text = self.column_type.to_string();
        if self.nullable {
            text.push_str("?");
        }

        write!(f, "{}", text)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    Varchar(usize),
    Int32,
    Int64,
    Unsigned32,
    Unsigned64,
    Float32,
    Float64,
    Boolean,
}

impl ColumnType {
    pub fn from_string(type_string: &str) -> Result<ColumnType, String> {
        let varchar_regex = Regex::new(r"^VARCHAR\((\d+)\)$").unwrap();
        if let Some(varchar_captures) = varchar_regex.captures(type_string) {
            let num_of_chars = varchar_captures
                .get(1)
                .unwrap()
                .as_str()
                .parse::<usize>()
                .unwrap();
            if num_of_chars < 1 {
                return Err("Invalid number of chars for VARCHAR".to_string());
            }
            return Ok(Varchar(num_of_chars));
        }

        match type_string {
            "INT32" => Ok(Int32),
            "INT64" => Ok(Int64),
            "UNSIGNED32" => Ok(Unsigned32),
            "UNSIGNED64" => Ok(Unsigned64),
            "FLOAT32" => Ok(Float32),
            "FLOAT64" => Ok(Float64),
            "BOOLEAN" => Ok(Boolean),
            _ => Err("Invalid column type".to_string()),
        }
    }

    pub fn byte_size(&self) -> usize {
        match self {
            Varchar(size) => size.clone(),
            Int32 => size_of::<i32>(),
            Int64 => size_of::<i64>(),
            Unsigned32 => size_of::<u32>(),
            Unsigned64 => size_of::<u64>(),
            Float32 => size_of::<f32>(),
            Float64 => size_of::<f64>(),
            Boolean => size_of::<bool>(),
        }
    }
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            Varchar(size) => format!("VARCHAR({})", size),
            Int32 => "INT32".to_string(),
            Int64 => "INT64".to_string(),
            Unsigned32 => "UNSIGNED32".to_string(),
            Unsigned64 => "UNSIGNED64".to_string(),
            Float32 => "FLOAT32".to_string(),
            Float64 => "FLOAT64".to_string(),
            Boolean => "BOOLEAN".to_string(),
        };
        write!(f, "{}", text)
    }
}

pub async fn read_table_schemas(file_path: &str) -> Result<Vec<TableSchema>, String> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(file_path)
        .await
        .map_err(|e| e.to_string())?;

    let buffer = Vec::with_capacity(16 * 1024);
    let (result, buffer) = file.read_at(buffer, 0).await;
    let num_of_bytes = result.map_err(|e| e.to_string())?;
    if num_of_bytes > buffer.capacity() {
        return Err("Buffer overflow".to_string());
    }

    if buffer.is_empty() {
        return Ok(vec![]);
    }

    let schema_strings: Vec<_> = String::from_utf8(buffer)
        .map_err(|e| e.to_string())?
        .split('\n')
        .map(|s| s.to_string())
        .collect();

    Ok(schema_strings
        .iter()
        .map(|schema_string| TableSchema::from_string(schema_string).unwrap())
        .collect())
}

pub async fn write_table_schemas_to_file(
    table_schemas: Vec<TableSchema>,
    file_path: &str,
) -> Result<(), String> {
    let file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(file_path)
        .await
        .map_err(|e| e.to_string())?;

    let schema_strings: Vec<_> = table_schemas
        .iter()
        .map(|schema| schema.to_string())
        .collect();
    let schema_file_content = schema_strings.join("\n");

    file.write_all_at(schema_file_content.into_bytes(), 0)
        .await
        .0
        .map_err(|e| e.to_string())?;

    Ok(())
}

pub async fn sync_model(
    schema_string: String,
    tables: Arc<Mutex<HashMap<String, Table>>>,
    partitions: &HashSet<usize>,
    file_path: &str,
) -> Result<(), String> {
    let table_schema = TableSchema::from_string(&schema_string)?;
    let mut tables = tables.lock().await;
    if tables.contains_key(&table_schema.name) {
        return Err(format!("Table '{}' already exists", &table_schema.name));
    }

    tables.insert(
        table_schema.name.clone(),
        Table::new(
            Memtable::default(),
            CommitLog::open_new(&table_schema, partitions).await,
            table_schema.clone(),
        ),
    );

    let mut table_schemas = Vec::new();
    for table in tables.values() {
        table_schemas.push(table.table_schema.clone());
    }
    table_schemas.push(table_schema);
    write_table_schemas_to_file(table_schemas, file_path).await?;

    Ok(())
}

pub async fn drop_table(
    table_name: String,
    tables: Arc<Mutex<HashMap<String, Table>>>,
    schema_file_path: &str,
    sstable_dir: &str,
) -> Result<(), String> {
    let mut tables = tables.lock().await;
    match tables.remove(&table_name) {
        Some(_) => {
            let table_schemas: Vec<_> = tables
                .values()
                .map(|table| table.table_schema.clone())
                .collect();

            write_table_schemas_to_file(table_schemas, schema_file_path).await?;
            drop_table_sstables(&table_name, sstable_dir);

            Ok(())
        }
        None => Err(format!("Table '{}' does not exist", &table_name)),
    }
}

fn drop_table_sstables(table_name: &str, sstable_dir: &str) {
    let filenames = get_sstables_metadata(table_name, sstable_dir);
    for sstable_metadata in filenames {
        std::fs::remove_file(sstable_metadata.file_path).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use monoio::fs::File;
    use std::iter::zip;

    fn get_table() -> Table {
        let table_schema = TableSchema::new("table".to_string(), Varchar(100));
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

    #[test]
    fn table_to_schema_string() {
        let mut table = get_table();
        table
            .table_schema
            .columns
            .insert("age".to_string(), Column::new(Unsigned32, true));
        table
            .table_schema
            .columns
            .insert("name".to_string(), Column::new(Varchar(100), false));

        let expected_schema_string =
            "table>sort_key:VARCHAR(100);age:UNSIGNED32?;name:VARCHAR(100)".to_string();
        assert_eq!(table.table_schema.to_string(), expected_schema_string);
    }

    #[test]
    fn schema_string_to_table_and_back() {
        let schema_string =
            "table>sort_key:UNSIGNED32;age:UNSIGNED32?;last_name:VARCHAR(10);some_field:FLOAT64?";
        let table_schema = TableSchema::from_string(schema_string).unwrap();

        let expected_schema_string =
            "table>sort_key:UNSIGNED32;age:UNSIGNED32?;last_name:VARCHAR(10);some_field:FLOAT64?"
                .to_string();
        assert_eq!(table_schema.to_string(), expected_schema_string);
    }

    #[test]
    fn sort_key_cannot_be_nullable() {
        let schema_string = "table_name>sort_key:VARCHAR(100)?";
        let error = TableSchema::from_string(schema_string).unwrap_err();

        assert_eq!(error, "sort_key cannot be nullable".to_string());
    }

    #[test]
    fn sort_key_must_be_present() {
        let schema_string = "table>name:VARCHAR(100)";
        let error = TableSchema::from_string(schema_string).unwrap_err();

        assert_eq!(
            error,
            "Invalid first column, should be 'sort_key'".to_string()
        );
    }

    #[test]
    fn invalid_schema_string() {
        let arrow_missing = "table:name:VARCHAR(100)";
        let bad_delimiter = "table>sort_key:INT32;name:VARCHAR(100)|age:INT32";
        let varchar_with_bad_number = "table>name:VARCHAR(0)";

        let error1 = TableSchema::from_string(arrow_missing).unwrap_err();
        let error2 = TableSchema::from_string(bad_delimiter).unwrap_err();
        let error3 = TableSchema::from_string(varchar_with_bad_number).unwrap_err();

        assert_eq!(error1, "Invalid schema string".to_string());
        assert_eq!(error2, "Invalid column type".to_string());
        assert_eq!(error3, "Invalid number of chars for VARCHAR".to_string());
    }

    #[monoio::test]
    async fn read_tables_from_empty_file() {
        let file_path = "/tmp/read_empty_schemas";
        let table_schemas = read_table_schemas(file_path).await.unwrap();
        assert!(table_schemas.is_empty());

        std::fs::remove_file(file_path).unwrap();
    }

    #[monoio::test]
    async fn write_and_read_table_schema() {
        let file_path = "/tmp/write_and_read_schemas";
        let mut table_schema = get_table().table_schema.clone();

        table_schema
            .columns
            .insert("age".to_string(), Column::new(Unsigned32, true));
        table_schema
            .columns
            .insert("name".to_string(), Column::new(Varchar(100), true));

        write_table_schemas_to_file(Vec::from([table_schema.clone()]), file_path)
            .await
            .unwrap();

        let schema_from_file = read_table_schemas(file_path).await.unwrap().pop().unwrap();

        assert_eq!(&schema_from_file.name, &table_schema.name);
        assert_eq!(&schema_from_file.sort_key_type, &table_schema.sort_key_type);

        for ((name1, col1), (name2, col2)) in zip(&schema_from_file.columns, &table_schema.columns)
        {
            assert_eq!(name1, name2);
            assert_eq!(col1.column_type, col2.column_type);
            assert_eq!(col1.nullable, col2.nullable);
        }

        std::fs::remove_file(file_path).unwrap();
    }

    #[monoio::test]
    async fn drop_table_with_sstables() {
        let schema_path = "/tmp/drop_table_schemas";
        let sstable_dir = "/tmp/drop_table_with_sstables";

        let table = get_table();
        write_table_schemas_to_file(Vec::from([table.table_schema.clone()]), schema_path)
            .await
            .unwrap();
        std::fs::create_dir_all(sstable_dir).unwrap();
        File::create(format!(
            "{}/{}-1-1-1",
            sstable_dir, &table.table_schema.name
        ))
        .await
        .unwrap();

        let tables = Arc::new(Mutex::new(HashMap::from([("table".to_string(), table)])));
        drop_table(
            "table".to_string(),
            tables.clone(),
            schema_path,
            sstable_dir,
        )
        .await
        .unwrap();

        let tables_lock = tables.lock().await;
        assert!(tables_lock.is_empty());
        assert!(get_sstables_metadata("table", sstable_dir).is_empty());

        std::fs::remove_file(schema_path).unwrap();
        std::fs::remove_dir_all(sstable_dir).unwrap();
    }
}
