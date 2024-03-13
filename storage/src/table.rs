use self::ColumnType::*;
use crate::{HASH_KEY_BYTE_SIZE, Memtable};
use futures::lock::Mutex;
use monoio::fs::OpenOptions;
use regex::Regex;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Display, Formatter};
use std::mem::size_of;
use std::sync::Arc;

static TABLE_SCHEMAS_FILE_PATH: &str = "/var/lib/yard/schemas";

pub struct Table {
    pub memtable: Memtable,
    pub table_schema: TableSchema,
}

impl Table {
    pub fn new(memtable: Memtable, table_schema: TableSchema) -> Table {
        Table {
            memtable,
            table_schema,
        }
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
                column_type_string = type_string;
                nullable = true;
            }

            columns.insert(
                column_name.to_string(),
                Column {
                    column_type: ColumnType::from_string(column_type_string),
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
        HASH_KEY_BYTE_SIZE + self.sort_key_type.byte_size() + values_byte_size
    }
}

impl Display for TableSchema {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut columns = Vec::new();
        columns.push(format!("{}:{}", "sort_key", self.sort_key_type.to_string()));
        for (name, column) in &self.columns {
            format!("{}:{}", name, column.to_string());
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

#[derive(Debug, Clone)]
pub enum ColumnType {
    Varchar(usize),
    Int32,
    Int64,
    Unsigned32,
    Unsigned64,
    Float32,
    Float64,
    Decimal(usize, usize),
    Datetime,
    Boolean,
}

impl ColumnType {
    pub fn from_string(type_string: &str) -> ColumnType {
        let decimal_regex = Regex::new(r"DECIMAL\((\d+),(\d+)\)").unwrap();
        let varchar_regex = Regex::new(r"VARCHAR\((\d+)\)").unwrap();

        if let Some(decimal_captures) = decimal_regex.captures(type_string) {
            let num_of_digits = decimal_captures
                .get(1)
                .unwrap()
                .as_str()
                .parse::<usize>()
                .unwrap();
            let decimal_places = decimal_captures
                .get(2)
                .unwrap()
                .as_str()
                .parse::<usize>()
                .unwrap();
            return Decimal(num_of_digits, decimal_places);
        }

        if let Some(varchar_captures) = varchar_regex.captures(type_string) {
            let num_of_chars = varchar_captures
                .get(1)
                .unwrap()
                .as_str()
                .parse::<usize>()
                .unwrap();
            return Varchar(num_of_chars);
        }

        match type_string {
            "INT32" => Int32,
            "INT64" => Int64,
            "UNSIGNED32" => Unsigned32,
            "UNSIGNED64" => Unsigned64,
            "FLOAT32" => Float32,
            "FLOAT64" => Float64,
            "DATETIME" => Datetime,
            "BOOLEAN" => Boolean,
            _ => panic!("Invalid column type"),
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
            Decimal(num_of_digits, decimal_places) => num_of_digits + decimal_places,
            Datetime => 100, // TODO
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
            Decimal(numbers, decimal_places) => format!("DECIMAL({},{})", numbers, decimal_places),
            Datetime => "DATETIME".to_string(),
            Boolean => "BOOLEAN".to_string(),
        };
        write!(f, "{}", text)
    }
}

pub async fn read_table_schemas() -> Result<Vec<TableSchema>, String> {
    let file = OpenOptions::new()
        .read(true)
        .open(TABLE_SCHEMAS_FILE_PATH)
        .await
        .map_err(|e| e.to_string())?;

    let buffer = Vec::with_capacity(32 * 1024);
    let (result, buffer) = file.read_at(buffer, 0).await;
    let num_of_bytes = result.map_err(|e| e.to_string())?;
    if num_of_bytes > buffer.capacity() {
        return Err("Buffer overflow".to_string());
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

pub async fn write_table_schemas_to_file(table_schemas: Vec<TableSchema>) -> Result<(), String> {
    let file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(TABLE_SCHEMAS_FILE_PATH)
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
) -> Result<(), String> {
    let table_schema = TableSchema::from_string(&schema_string)?;
    let mut tables = tables.lock().await;
    if tables.contains_key(&table_schema.name) {
        return Err(format!("Table '{}' already exists", &table_schema.name));
    }

    tables.insert(
        table_schema.name.clone(),
        Table::new(Memtable::default(), table_schema.clone()),
    );

    let mut table_schemas = Vec::new();
    for table in tables.values() {
        table_schemas.push(table.table_schema.clone());
    }
    table_schemas.push(table_schema);
    write_table_schemas_to_file(table_schemas).await?;

    Ok(())
}

pub async fn drop_table(
    table_name: String,
    tables: Arc<Mutex<HashMap<String, Table>>>,
) -> Result<(), String> {
    let mut tables = tables.lock().await;
    match tables.remove(&table_name) {
        Some(_) => {
            let table_schemas: Vec<_> = tables
                .values()
                .map(|table| table.table_schema.clone())
                .collect();
            write_table_schemas_to_file(table_schemas).await?;
            Ok(())
        }
        None => Err(format!("Table '{}' already exists", &table_name)),
    }
}
