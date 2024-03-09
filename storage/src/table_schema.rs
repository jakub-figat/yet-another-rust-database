use self::ColumnType::*;
use monoio::fs::OpenOptions;
use regex::Regex;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

static TABLE_SCHEMAS_FILE_PATH: &str = "/var/lib/yard/schemas";

pub struct TableSchema {
    name: String,
    columns: BTreeMap<String, Column>,
}

impl TableSchema {
    pub fn from_string(schema_string: &str) -> TableSchema {
        let (table_name, columns_string) = schema_string.split_once('>').unwrap();

        let columns: BTreeMap<String, Column> = columns_string
            .split(';')
            .map(|column_string| {
                let (column_name, mut column_type_string) = column_string.split_once(':').unwrap();

                let mut nullable = false;
                if let Some((type_string, _)) = column_type_string.split_once('?') {
                    column_type_string = type_string;
                    nullable = true;
                }

                (
                    column_name.to_string(),
                    Column {
                        column_type: ColumnType::from_string(column_type_string),
                        nullable,
                    },
                )
            })
            .collect();

        TableSchema {
            name: table_name.to_string(),
            columns,
        }
    }
}

impl Display for TableSchema {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let columns: Vec<_> = self
            .columns
            .iter()
            .map(|(name, column)| format!("{}:{}", name, column.to_string()))
            .collect();
        write!(f, "{}>{}", self.name, columns.join(";"))
    }
}

struct Column {
    column_type: ColumnType,
    nullable: bool,
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
        let varchar_regex = Regex::new(r"VARCHAR\(\d+\)").unwrap();

        if let Some(decimal_captures) = decimal_regex.captures(type_string) {
            let num_of_digits = decimal_captures
                .get(0)
                .unwrap()
                .as_str()
                .parse::<usize>()
                .unwrap();
            let decimal_places = decimal_captures
                .get(1)
                .unwrap()
                .as_str()
                .parse::<usize>()
                .unwrap();
            return Decimal(num_of_digits, decimal_places);
        }

        if let Some(varchar_captures) = varchar_regex.captures(type_string) {
            let num_of_chars = varchar_captures
                .get(0)
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

pub async fn read_table_schemas_from_file() -> Result<Vec<TableSchema>, String> {
    let file = OpenOptions::new()
        .read(true)
        .create(true)
        .open(TABLE_SCHEMAS_FILE_PATH)
        .await
        .map_err(|e| e.to_string())?;

    let mut buffer = Vec::with_capacity(16 * 1024);
    let mut offset = 0usize;

    loop {
        let result = file.read_at(buffer, offset as u64).await;

        buffer = result.1;
        match result.0 {
            Ok(bytes_read) => {
                if bytes_read == 0 {
                    break;
                }
                offset += bytes_read;
            }
            Err(error) => {
                return Err(error.to_string());
            }
        }
    }

    let schema_strings: Vec<_> = String::from_utf8(buffer)
        .map_err(|e| e.to_string())?
        .split('\n')
        .map(|s| s.to_string())
        .collect();

    Ok(schema_strings
        .iter()
        .map(|schema_string| TableSchema::from_string(schema_string))
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
