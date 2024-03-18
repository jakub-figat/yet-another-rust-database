use crate::table::{ColumnType, TableSchema};
use crate::{Row, HASH_KEY_BYTE_SIZE};
use common::value::Value;
use std::collections::HashMap;
use std::io::{BufWriter, Write};
use std::mem::size_of;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn millis_from_epoch() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

pub fn encode_row(row: &Row, table_schema: &TableSchema) -> Vec<u8> {
    let mut bytes = Vec::new();

    let mut hash_key_bytes = BufWriter::new(vec![0u8; HASH_KEY_BYTE_SIZE]);
    hash_key_bytes.write_all(row.hash_key.as_bytes()).unwrap();
    bytes.append(hash_key_bytes.get_mut());

    let mut sort_key_bytes = row.sort_key.clone().to_bytes();
    bytes.append(&mut sort_key_bytes);

    for (name, column) in &table_schema.columns {
        let mut value_buffer = BufWriter::new(vec![0u8; column.column_type.byte_size()]);
        value_buffer
            .write_all(&row.values.get(name).unwrap().clone().to_bytes())
            .unwrap();
        bytes.append(&mut value_buffer.get_mut());
    }

    let mut timestamp_bytes = row.timestamp.to_be_bytes().to_vec();
    bytes.append(&mut timestamp_bytes);

    if row.marked_for_deletion {
        bytes.push(1);
    } else {
        bytes.push(0);
    }

    bytes
}

pub fn decode_row(bytes: &[u8], table_schema: &TableSchema) -> Row {
    let hash_key = String::from_utf8(bytes[..HASH_KEY_BYTE_SIZE].to_vec()).unwrap();
    let mut offset = HASH_KEY_BYTE_SIZE;

    let sort_key_size = table_schema.sort_key_type.byte_size();
    let sort_key = parse_value_from_bytes(
        bytes[offset..offset + sort_key_size].to_vec(),
        table_schema.sort_key_type.clone(),
    );
    offset += sort_key_size;

    let mut values = HashMap::new();
    for (column_name, column) in &table_schema.columns {
        let column_size = column.column_type.byte_size();
        let value = parse_value_from_bytes(
            bytes[offset..offset + column_size].to_vec(),
            column.column_type.clone(),
        );
        offset += column_size;

        values.insert(column_name.clone(), value);
    }

    let timestamp_size = size_of::<u128>();
    let timestamp = u128::from_be_bytes(
        bytes[offset..offset + timestamp_size]
            .to_vec()
            .try_into()
            .unwrap(),
    );
    offset += timestamp_size;

    let mut row = Row::new_with_timestamp(hash_key, sort_key, values, timestamp);

    if bytes[offset] == 1u8 {
        row.marked_for_deletion = true;
    }

    row
}

pub fn parse_value_from_bytes(bytes: Vec<u8>, column_type: ColumnType) -> Value {
    if bytes[0] == b'\x00' {
        return Value::Null;
    }

    match column_type {
        ColumnType::Varchar(_) => Value::Varchar(String::from_utf8(bytes).unwrap()),
        ColumnType::Int32 => Value::Int32(i32::from_be_bytes(bytes.try_into().unwrap())),
        ColumnType::Int64 => Value::Int64(i64::from_be_bytes(bytes.try_into().unwrap())),
        ColumnType::Unsigned32 => Value::Unsigned32(u32::from_be_bytes(bytes.try_into().unwrap())),
        ColumnType::Unsigned64 => Value::Unsigned64(u64::from_be_bytes(bytes.try_into().unwrap())),
        ColumnType::Float32 => Value::Float32(f32::from_be_bytes(bytes.try_into().unwrap())),
        ColumnType::Float64 => Value::Float64(f64::from_be_bytes(bytes.try_into().unwrap())),
        ColumnType::Boolean => {
            let value = bytes[0];
            Value::Boolean(value == 1)
        }
    }
}
