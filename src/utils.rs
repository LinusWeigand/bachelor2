use std::io::{self, Read, Seek, SeekFrom};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::{collections::HashMap, error::Error};

use crate::aggregation::{Aggregation, Aggregator};
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::error::Error as ArrowError;
use chrono::{NaiveDateTime, TimeZone, Utc};
use parquet2::metadata::FileMetaData;
use parquet2::schema::types::PhysicalType;
use parquet2::statistics::{
    BinaryStatistics, BooleanStatistics, FixedLenStatistics, PrimitiveStatistics, Statistics,
};

#[derive(Clone, Debug)]
pub struct Condition {
    pub column_name: String,
    pub threshold: ThresholdValue,
    pub comparison: Comparison,
}

#[derive(Clone, Debug)]
pub enum ThresholdValue {
    Int64(i64),
    Float64(f64),
    Boolean(bool),
    Utf8String(String),
}

#[derive(Clone, Debug)]
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

pub fn bytes_to_value(
    bytes: &[u8],
    column_type: PhysicalType,
) -> Result<ThresholdValue, Box<dyn Error + Send + Sync>> {
    match column_type {
        PhysicalType::Int32 => {
            if bytes.len() != 4 {
                return Err("Expected 4 bytes for INT32".into());
            }
            let v = i32::from_le_bytes(bytes.try_into()?);
            Ok(ThresholdValue::Int64(v as i64))
        }
        PhysicalType::Int64 => {
            if bytes.len() != 8 {
                return Err("Expected 8 bytes for INT64".into());
            }
            let v = i64::from_le_bytes(bytes.try_into()?);
            Ok(ThresholdValue::Int64(v))
        }
        PhysicalType::Float => {
            if bytes.len() != 4 {
                return Err("Expected 4 bytes for FLOAT".into());
            }
            let v = f32::from_le_bytes(bytes.try_into()?);
            Ok(ThresholdValue::Float64(v as f64))
        }
        PhysicalType::Double => {
            if bytes.len() != 8 {
                return Err("Expected 8 bytes for DOUBLE".into());
            }
            let v = f64::from_le_bytes(bytes.try_into()?);
            Ok(ThresholdValue::Float64(v))
        }
        PhysicalType::Boolean => {
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
        PhysicalType::ByteArray => {
            use std::str;
            let s = str::from_utf8(bytes)?.to_owned();
            Ok(ThresholdValue::Utf8String(s))
        }
        _ => Err(format!("Unsupported column type: {:?}", column_type).into()),
    }
}

pub fn get_column_projection_from_aggregations(aggregations: &Vec<Aggregation>) -> Vec<String> {
    let mut result: Vec<String> = Vec::new();
    for aggregation in aggregations {
        let col_name = &aggregation.column_name;
        if result.contains(&col_name) {
            result.push(col_name.to_owned());
        }
    }
    result
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

pub fn get_column_name_to_index(schema: &Schema) -> HashMap<String, usize> {
        schema
        .fields
        .iter()
        .enumerate()
        .map(|(i, field)| (field.name.clone(), i))
        .collect()
}

pub fn get_column_names(metadata: &FileMetaData) -> Vec<String> {
    metadata
        .schema()
        .columns()
        .iter()
        .filter_map(|column| column.path_in_schema.last())
        .map(|d| d.to_string())
        .collect()
}

pub fn aggregate_batch(
    aggregators: &mut Vec<Option<Box<dyn Aggregator>>>,
    batch: &Chunk<Box<dyn Array>>,
) -> Result<(), ArrowError> {
    for aggregator in aggregators {
        if let Some(aggregator) = aggregator {
            aggregator.aggregate_batch(batch)?;
        }
    }
    Ok(())
}

pub fn tokenize(input: &str) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
    let mut tokens = Vec::new();
    let mut current = String::new();

    for c in input.chars() {
        match c {
            '(' | ')' | ' ' => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
                if c != ' ' {
                    tokens.push(c.to_string());
                }
            }
            _ => current.push(c),
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    Ok(tokens)
}

// pub fn convert_parquet_type_to_arrow(parquet_type: ParquetType) -> DataType {
//     match parquet_type {
//         ParquetType::BOOLEAN => DataType::Boolean,
//         ParquetType::INT32 => DataType::Int32,
//         ParquetType::INT64 => DataType::Int64,
//         ParquetType::INT96 => DataType::Int64,
//         ParquetType::FLOAT => DataType::Float32,
//         ParquetType::DOUBLE => DataType::Float64,
//         ParquetType::BYTE_ARRAY => DataType::Utf8,
//         ParquetType::FIXED_LEN_BYTE_ARRAY => DataType::Binary,
//     }
// }

pub fn get_naive_date_time_from_timestamp(timestamp: i64) -> Option<NaiveDateTime> {
    let secs = timestamp / 1000;
    let nanos: u32 = match ((timestamp % 1000) * 1_000_000).try_into() {
        Ok(v) => v,
        Err(_) => return None,
    };
    Utc.timestamp_opt(secs, nanos)
        .single()
        .map(|dt| dt.naive_utc())
}

pub struct CountingReader<R> {
    inner: R,
    bytes_read: Arc<AtomicUsize>,
}

impl<R> CountingReader<R> {
    pub fn new(inner: R, bytes_read: Arc<AtomicUsize>) -> Self {
        Self { inner, bytes_read }
    }

    pub fn bytes_read(&self) -> usize {
        self.bytes_read.load(Ordering::Relaxed)
    }
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.bytes_read.fetch_add(n, Ordering::Relaxed);
        Ok(n)
    }
}

impl<R: Seek> Seek for CountingReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }
}

pub fn get_min_max_threshold(
    stats: &Arc<dyn Statistics>,
) -> Option<(ThresholdValue, ThresholdValue)> {
    if let Some(typed_stats) = stats.as_any().downcast_ref::<BinaryStatistics>() {
        let min_str = String::from_utf8(typed_stats.min_value.clone()?).ok()?;
        let max_str = String::from_utf8(typed_stats.max_value.clone()?).ok()?;
        return Some((
            ThresholdValue::Utf8String(min_str),
            ThresholdValue::Utf8String(max_str),
        ));
    }

    if let Some(typed_stats) = stats.as_any().downcast_ref::<BooleanStatistics>() {
        return Some((
            ThresholdValue::Boolean(typed_stats.min_value?),
            ThresholdValue::Boolean(typed_stats.max_value?),
        ));
    }

    if let Some(typed_stats) = stats.as_any().downcast_ref::<FixedLenStatistics>() {
        let min_str = String::from_utf8(typed_stats.min_value.clone()?).ok()?;
        let max_str = String::from_utf8(typed_stats.max_value.clone()?).ok()?;
        return Some((
            ThresholdValue::Utf8String(min_str),
            ThresholdValue::Utf8String(max_str),
        ));
    }

    if let Some(typed_stats) = stats.as_any().downcast_ref::<PrimitiveStatistics<i64>>() {
        return Some((
            ThresholdValue::Int64(typed_stats.min_value?),
            ThresholdValue::Int64(typed_stats.max_value?),
        ));
    }
    if let Some(typed_stats) = stats.as_any().downcast_ref::<PrimitiveStatistics<i32>>() {
        return Some((
            ThresholdValue::Int64(typed_stats.min_value? as i64),
            ThresholdValue::Int64(typed_stats.max_value? as i64),
        ));
    }
    if let Some(typed_stats) = stats.as_any().downcast_ref::<PrimitiveStatistics<f32>>() {
        return Some((
            ThresholdValue::Float64(typed_stats.min_value? as f64),
            ThresholdValue::Float64(typed_stats.max_value? as f64),
        ));
    }
    if let Some(typed_stats) = stats.as_any().downcast_ref::<PrimitiveStatistics<f64>>() {
        return Some((
            ThresholdValue::Float64(typed_stats.min_value?),
            ThresholdValue::Float64(typed_stats.max_value?),
        ));
    }
    println!("No Downcast :(");

    None
}

pub fn aggregate_chunks(
    chunks: &[Chunk<Box<dyn arrow2::array::Array>>],
) -> Option<Chunk<Box<dyn arrow2::array::Array>>> {
    if chunks.is_empty() {
        return None;
    }

    let num_columns = chunks[0].columns().len();
    let mut concatenated_columns = Vec::new();

    for col_idx in 0..num_columns {
        let column_arrays: Vec<&dyn arrow2::array::Array> = chunks
            .iter()
            .map(|chunk| chunk.columns()[col_idx].as_ref())
            .collect();
        let concatenated_array = arrow2::compute::concatenate::concatenate(&column_arrays).ok()?;
        concatenated_columns.push(concatenated_array);
    }

    Some(Chunk::new(concatenated_columns))
}


pub fn filter_columns(batch: &Chunk<Box<dyn Array>>, selected_indices: &[usize]) -> Chunk<Box<dyn Array>> {
    let filtered_columns: Vec<_> = selected_indices
        .iter()
        .filter_map(|&index| batch.columns().get(index).cloned())
        .collect();

    Chunk::new(filtered_columns)
}
