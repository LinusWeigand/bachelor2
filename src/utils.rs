use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{collections::HashMap, error::Error, pin::Pin};

use crate::aggregation::Aggregator;
use arrow::{array::RecordBatch, datatypes::DataType, error::ArrowError};
use chrono::{NaiveDateTime, TimeZone, Utc};
use futures::StreamExt;
use parquet::basic::Type as ParquetType;
use parquet::{arrow::async_reader::ParquetRecordBatchStream, };
use parquet2::metadata::FileMetaData;
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

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

pub fn bytes_to_value(bytes: &[u8], column_type: &str) -> Result<ThresholdValue, Box<dyn Error + Send + Sync>> {
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

pub async fn get_next_item_from_reader_and_count_bytes(
    pinned_stream: &mut Pin<&mut ParquetRecordBatchStream<CountingReader<File>>>,
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

pub fn get_column_name_to_index(metadata: &FileMetaData) -> HashMap<String, usize> {
    metadata
        .schema()
        .columns()
        .iter()
        .enumerate()
        .filter_map(|(i, column)| {
            match column.path_in_schema.last() {
                Some(v) => Some((v.to_owned(), i)),
                None => None
            }
        })
        .collect()
}

pub fn aggregate_batch(
    aggregators: &mut Vec<Option<Box<dyn Aggregator>>>,
    batch: &RecordBatch,
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

pub fn convert_parquet_type_to_arrow(parquet_type: ParquetType) -> DataType {
    match parquet_type {
        ParquetType::BOOLEAN => DataType::Boolean,
        ParquetType::INT32 => DataType::Int32,
        ParquetType::INT64 => DataType::Int64,
        ParquetType::INT96 => DataType::Int64,
        ParquetType::FLOAT => DataType::Float32,
        ParquetType::DOUBLE => DataType::Float64,
        ParquetType::BYTE_ARRAY => DataType::Utf8,
        ParquetType::FIXED_LEN_BYTE_ARRAY => DataType::Binary,
    }
}

pub fn get_naive_date_time_from_timestamp(timestamp: i128) -> Option<NaiveDateTime> {
    let secs: i64 = match (timestamp / 1000).try_into() {
        Ok(v) => v,
        Err(_) => return None,
    };
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
}

impl<R: AsyncRead + Unpin> AsyncRead for CountingReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let start_len = buf.filled().len();
        let inner = Pin::new(&mut self.inner);

        // Perform the actual read
        match inner.poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                // Number of bytes read this time
                let new_len = buf.filled().len();
                let n = new_len - start_len;

                // Update our counter
                self.bytes_read.fetch_add(n, Ordering::Relaxed);

                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl<R: AsyncSeek + Unpin> AsyncSeek for CountingReader<R> {
    fn start_seek(mut self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
        Pin::new(&mut self.inner).start_seek(position)
    }

    fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        Pin::new(&mut self.inner).poll_complete(cx)
    }
}
