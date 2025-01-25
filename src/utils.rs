use std::{collections::HashMap, error::Error, pin::Pin};

use arrow::array::RecordBatch;
use futures::StreamExt;
use parquet::{arrow::async_reader::ParquetRecordBatchStream, file::metadata::FileMetaData};
use tokio::fs::File;

#[derive(Clone)]
pub struct Condition {
    pub column_name: String,
    pub threshold: ThresholdValue,
    pub comparison: Comparison,
}

#[derive(Clone)]
pub enum ThresholdValue {
    Int64(i64),
    Float64(f64),
    Boolean(bool),
    Utf8String(String),
}

#[derive(Clone)]
pub enum Expression {
    Condition(Condition),
    And(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
}

#[derive(PartialEq, Clone, Debug)]
pub enum Comparison {
    LessThan,
    LessThanOrEqual,
    Equal,
    GreaterThanOrEqual,
    GreaterThan,
}

pub struct ColumnMaps {
    pub index_to_name: HashMap<usize, String>,
    pub name_to_index: HashMap<String, usize>,
}

impl Comparison {
    pub fn from_str(input: &str) -> Option<Self> {
        match input {
            "<" => Some(Comparison::LessThan),
            "<=" => Some(Comparison::LessThanOrEqual),
            "==" => Some(Comparison::Equal),
            ">=" => Some(Comparison::GreaterThanOrEqual),
            ">" => Some(Comparison::GreaterThan),
            _ => None,
        }
    }
}

pub trait Float: Copy + PartialOrd {
    fn abs(self) -> Self;
    fn equal(self, other: Self) -> bool;
}

impl Float for f32 {
    fn abs(self) -> Self {
        self.abs()
    }
    fn equal(self, other: Self) -> bool {
        (self - other).abs() < f32::EPSILON
    }
}

impl Float for f64 {
    fn abs(self) -> Self {
        self.abs()
    }
    fn equal(self, other: Self) -> bool {
        (self - other).abs() < f64::EPSILON
    }
}

pub fn bytes_to_value(bytes: &[u8], column_type: &str) -> Result<ThresholdValue, Box<dyn Error>> {
    match column_type {
        "INT32" => {
            if bytes.len() != 4 {
                return Err("Expected 4 bytes for INT32".into());
            }
            let v = i32::from_le_bytes(bytes.try_into()?);
            Ok(ThresholdValue::Int64(v as i64))
        }
        "INT64" => {
            if bytes.len() != 8 {
                return Err("Expected 8 bytes for INT64".into());
            }
            let v = i64::from_le_bytes(bytes.try_into()?);
            Ok(ThresholdValue::Int64(v))
        }
        "FLOAT" => {
            if bytes.len() != 4 {
                return Err("Expected 4 bytes for FLOAT".into());
            }
            let v = f32::from_le_bytes(bytes.try_into()?);
            Ok(ThresholdValue::Float64(v as f64))
        }
        "DOUBLE" => {
            if bytes.len() != 8 {
                return Err("Expected 8 bytes for DOUBLE".into());
            }
            let v = f64::from_le_bytes(bytes.try_into()?);
            Ok(ThresholdValue::Float64(v))
        }
        "BOOLEAN" => {
            if bytes.len() != 1 {
                return Err("Expected 1 bytes for BOOLEAN".into());
            }
            let bool_value = match bytes[0] {
                0 => false,
                1 => true,
                _ => return Err("Invalid Boolean byte value".into()),
            };
            Ok(ThresholdValue::Boolean(bool_value))
        }
        "BYTE_ARRAY" => {
            use std::str;
            let s = str::from_utf8(bytes)?.to_owned();
            Ok(ThresholdValue::Utf8String(s))
        }
        _ => Err(format!("Unsupported column type: {}", column_type).into()),
    }
}

pub async fn get_next_item_from_reader(
    pinned_stream: &mut Pin<&mut ParquetRecordBatchStream<File>>,
) -> Option<RecordBatch> {
    match &pinned_stream.as_mut().next().await {
        Some(Ok(record_batch)) => Some(record_batch.clone()),
        Some(Err(e)) => {
            eprintln!("Error: {:?}", e);
            None
        }
        None => None,
    }
}

pub fn get_column_projection_from_expression(expression: &Expression) -> Vec<String> {
    let mut column_projection = Vec::new();

    fn get_column_projection(expr: &Expression, cols: &mut Vec<String>) {
        match expr {
            Expression::Condition(cond) => {
                if !cols.contains(&cond.column_name) {
                    cols.push(cond.column_name.clone());
                }
            }
            Expression::And(left, right) | Expression::Or(left, right) => {
                get_column_projection(left, cols);
                get_column_projection(right, cols);
            }
            Expression::Not(inner) => get_column_projection(inner, cols),
        }
    }

    get_column_projection(expression, &mut column_projection);
    column_projection
}


pub fn get_column_maps(metadata: &FileMetaData) -> ColumnMaps {
    let mut index_to_name: HashMap<usize, String> = HashMap::new();
    let name_to_index = metadata
        .schema_descr()
        .columns()
        .iter()
        .enumerate()
        .map(|(i, column)| {
            let column_name = column
                .path()
                .to_string()
                .trim()
                .trim_matches('"')
                .to_string();
            index_to_name.insert(i, column_name.clone());
            (column_name, i)
        })
        .collect();
    ColumnMaps {
        index_to_name,
        name_to_index,
    }
}
