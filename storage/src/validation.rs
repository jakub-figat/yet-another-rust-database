use crate::table::{ColumnType, TableSchema};
use common::value::Value;
use std::collections::{HashMap, HashSet};

pub fn validate_values_against_schema(
    sort_key: &Value,
    values: &HashMap<String, Value>,
    table_schema: &TableSchema,
) -> Result<(), String> {
    let table_schema_columns: HashSet<_> = table_schema.columns.keys().collect();

    let values_columns: HashSet<_> = values.keys().collect();
    let column_diff: Vec<_> = table_schema_columns.difference(&values_columns).collect();

    if !column_diff.is_empty() {
        return Err(format!(
            "Invalid fields for table '{}', {:?} are missing in request",
            &table_schema.name, column_diff
        ));
    }

    let mut errors = Vec::new();
    if let Value::Null = sort_key {
        errors.push("'sort_key' cannot be null".to_string());
    }
    if !check_value_matches_column_type(sort_key, &table_schema.sort_key_type) {
        errors.push(format!(
            "'sort_key': expected '{}', got '{}'",
            &table_schema.sort_key_type,
            value_to_column_type(sort_key)
        ));
    }

    for (column_name, column) in &table_schema.columns {
        let value = values.get(column_name).unwrap();
        if let Value::Null = value {
            if !column.nullable {
                errors.push(format!("'{}': Field cannot be null", column_name));
            }
            continue;
        }

        if !check_value_matches_column_type(value, &column.column_type) {
            errors.push(format!(
                "'{}': expected '{}', got '{}'",
                column_name,
                &column.column_type,
                value_to_column_type(value)
            ));
        }

        if !check_string_length(&value, &column.column_type) {
            let max_length = match &column.column_type {
                ColumnType::Varchar(max_length) => max_length,
                _ => panic!("Invalid column type"),
            };
            errors.push(format!(
                "'{}': String length exceeded max length of {}",
                column_name, max_length
            ));
        }
    }

    if !errors.is_empty() {
        return Err(format!(
            "Invalid field types for table '{}': {:?}",
            &table_schema.name, errors
        ));
    }

    Ok(())
}

fn check_string_length(value: &Value, column_type: &ColumnType) -> bool {
    match value {
        Value::Varchar(value) => {
            let max_length = match column_type {
                ColumnType::Varchar(max_length) => max_length,
                _ => panic!("Invalid column type"),
            };

            max_length >= &value.len()
        }
        _ => true,
    }
}

fn check_value_matches_column_type(value: &Value, column_type: &ColumnType) -> bool {
    match (value, column_type) {
        (Value::Varchar(_), ColumnType::Varchar(_)) => true,
        (Value::Int32(_), ColumnType::Int32) => true,
        (Value::Int64(_), ColumnType::Int64) => true,
        (Value::Unsigned32(_), ColumnType::Unsigned32) => true,
        (Value::Unsigned64(_), ColumnType::Unsigned64) => true,
        (Value::Float32(_), ColumnType::Float32) => true,
        (Value::Float64(_), ColumnType::Float64) => true,
        (Value::Boolean(_), ColumnType::Boolean) => true,
        _ => false,
    }
}

fn value_to_column_type(value: &Value) -> ColumnType {
    match value {
        Value::Varchar(_) => ColumnType::Varchar(0), // TODO string
        Value::Int32(_) => ColumnType::Int32,
        Value::Int64(_) => ColumnType::Int64,
        Value::Unsigned32(_) => ColumnType::Unsigned32,
        Value::Unsigned64(_) => ColumnType::Unsigned64,
        Value::Float32(_) => ColumnType::Float32,
        Value::Float64(_) => ColumnType::Float64,
        Value::Boolean(_) => ColumnType::Boolean,
        _ => panic!("Invalid value variant"),
    }
}
