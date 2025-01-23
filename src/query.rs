use std::{collections::HashMap, error::Error, path::PathBuf, pin::Pin};

use arrow::array::RecordBatch;
use futures::StreamExt;
use parquet::{arrow::{arrow_reader::{ArrowPredicate, ArrowPredicateFn, ArrowReaderMetadata, RowFilter}, async_reader::ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder, ProjectionMask}, file::metadata::{FileMetaData, RowGroupMetaData}};
use tokio::fs::File;

use crate::{bloom_filter::BloomFilter, row_filter::predicate_function};

#[derive(PartialEq, Clone)]
pub enum Comparison {
    LessThan,
    LessThanOrEqual,
    Equal,
    GreaterThanOrEqual,
    GreaterThan,
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

#[derive(Clone)]
pub struct Condition {
    pub column_name: String,
    pub threshold: ThresholdValue,
    pub comparison: Comparison,
}

#[derive(Debug, Clone)]
pub enum ThresholdValue {
    Number(f64),
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

impl Comparison {
    pub fn keep(
        &self,
        row_group_min: &ThresholdValue,
        row_group_max: &ThresholdValue,
        user_threshold: &ThresholdValue,
    ) -> bool {
        match (row_group_min, row_group_max, user_threshold) {
            (
                ThresholdValue::Number(min),
                ThresholdValue::Number(max),
                ThresholdValue::Number(threshold),
            ) => match self {
                Comparison::LessThan => min < threshold,
                Comparison::LessThanOrEqual => min <= threshold,
                Comparison::Equal => threshold >= min && threshold <= max,
                Comparison::GreaterThanOrEqual => max >= threshold,
                Comparison::GreaterThan => max > threshold,
            },
            (
                ThresholdValue::Boolean(min),
                ThresholdValue::Boolean(max),
                ThresholdValue::Boolean(threshold),
            ) => match self {
                Comparison::LessThan => true,
                Comparison::LessThanOrEqual => true,
                Comparison::Equal => threshold == min || threshold == max,
                Comparison::GreaterThanOrEqual => true,
                Comparison::GreaterThan => true,
            },
            (
                ThresholdValue::Utf8String(min),
                ThresholdValue::Utf8String(max),
                ThresholdValue::Utf8String(threshold),
            ) => match self {
                Comparison::LessThan => min < threshold,
                Comparison::LessThanOrEqual => min <= threshold,
                Comparison::Equal => threshold >= min && threshold <= max,
                Comparison::GreaterThanOrEqual => max >= threshold,
                Comparison::GreaterThan => max > threshold,
            },
            _ => true,
        }
    }
}

pub struct MetadataEntry {
    pub file_path: PathBuf,
    pub metadata: ArrowReaderMetadata,
    pub column_index_map: HashMap<String, usize>,
}


pub async fn smart_query_parquet(
    metadata_entry: &MetadataEntry,
    bloom_filters: Vec<Vec<Option<BloomFilter>>>,
    expression: &Expression,
    select_columns: &Vec<String>,
) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
    let file = File::open(&metadata_entry.file_path).await?;

    let metadata = metadata_entry.metadata.clone();
    let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(file, metadata.clone());
    let metadata = metadata.metadata();

    let column_indices: Vec<usize> = select_columns
        .iter()
        .filter_map(|column| metadata_entry.column_index_map.get(column).map(|&x| x))
        .collect();
    let mask = ProjectionMask::roots(metadata.file_metadata().schema_descr(), column_indices);

    let mut row_groups: Vec<usize> = Vec::new();

    for i in 0..metadata.num_row_groups() {
        let row_group_metadata = metadata.row_group(i);
        let is_keep = keep_row_group(row_group_metadata, &bloom_filters[i], expression)?;
        println!("Row Group: {}, Skip: {}", i, !is_keep);
        if is_keep {
            row_groups.push(i);
        }
    }

    println!("Filtered Row Groups: {}", row_groups.len());

    let filter_columns: Vec<String> = get_column_projection_from_expression(expression);
    let column_indices: Vec<usize> = filter_columns
        .iter()
        .filter_map(|column| metadata_entry.column_index_map.get(column).map(|&x| x))
        .collect();
    let projection = ProjectionMask::roots(metadata.file_metadata().schema_descr(), column_indices);
    let predicate: Box<dyn ArrowPredicate> = Box::new(ArrowPredicateFn::new(
        projection,
        predicate_function(expression.clone()),
    ));
    let predicates: Vec<Box<dyn ArrowPredicate>> = vec![predicate];
    let row_filter: RowFilter = RowFilter::new(predicates);

    let mut stream = builder
        // .with_projection(mask)
        .with_row_groups(row_groups)
        // .with_row_filter(row_filter)
        .build()?;
    let mut pinned_stream = Pin::new(&mut stream);

    let record_batch = get_next_item_from_reader(&mut pinned_stream).await;

    if record_batch.is_none() {
        return Err("File is empty".into());
    }
    let record_batch = record_batch.unwrap();
    let mut result = Vec::new();
    result.push(record_batch);

    while let Some(record_batch) = get_next_item_from_reader(&mut pinned_stream).await {
        result.push(record_batch);
    }

    Ok(result)
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

pub fn keep_row_group(
    row_group: &RowGroupMetaData,
    bloom_filters: &Vec<Option<BloomFilter>>,
    expression: &Expression,
) -> Result<bool, Box<dyn Error>> {
    match expression {
        Expression::Condition(condition) => {
            if let Some((index, column)) = row_group
                .columns()
                .iter()
                .enumerate()
                .find(|(_, c)| c.column_path().string() == condition.column_name)
            {
                let column_type = column.column_type().to_string();
                let bloom_filter = &bloom_filters[index];
                if let Some(stats) = column.statistics() {
                    if let (Some(min_bytes), Some(max_bytes)) =
                        (stats.min_bytes_opt(), stats.max_bytes_opt())
                    {
                        let min_value = bytes_to_value(min_bytes, &column_type)?;
                        let max_value = bytes_to_value(max_bytes, &column_type)?;
                        // println!("MIN: {:#?}", min_value);
                        // println!("MAX: {:#?}", max_value);
                        // println!("Threshold: {:#?}", &condition.threshold);
                        let mut result =
                            condition
                                .comparison
                                .keep(&min_value, &max_value, &condition.threshold);
                        if !result && condition.comparison == Comparison::Equal {
                            if let Some(bloom_filter) = bloom_filter {
                                result = bloom_filter.contains(&condition.threshold);
                            }
                        }

                        return Ok(result);
                    }
                }
            }
            Ok(false)
        }
        Expression::And(left, right) => Ok(keep_row_group(row_group, bloom_filters, left)?
            && keep_row_group(row_group, bloom_filters, right)?),
        Expression::Or(left, right) => Ok(keep_row_group(row_group, bloom_filters, left)?
            || keep_row_group(row_group, bloom_filters, right)?),
        Expression::Not(inner) => Ok(!keep_row_group(row_group, bloom_filters, inner)?),
    }
}

pub fn get_column_name_to_index_map(metadata: &FileMetaData) -> HashMap<String, usize> {
    metadata
        .schema_descr()
        .columns()
        .iter()
        .enumerate()
        .map(|(i, column)| (column.path().to_string(), i))
        .collect()
}

fn bytes_to_value(bytes: &[u8], column_type: &str) -> Result<ThresholdValue, Box<dyn Error>> {
    match column_type {
        "INT32" => {
            if bytes.len() != 4 {
                return Err("Expected 4 bytes for INT32".into());
            }
            let int_value = i32::from_le_bytes(bytes.try_into()?);
            Ok(ThresholdValue::Number(int_value as f64))
        }
        "INT64" => {
            if bytes.len() != 8 {
                return Err("Expected 8 bytes for INT64".into());
            }
            let int_value = i64::from_le_bytes(bytes.try_into()?);
            Ok(ThresholdValue::Number(int_value as f64))
        }
        "FLOAT" => {
            if bytes.len() != 4 {
                return Err("Expected 4 bytes for FLOAT".into());
            }
            let float_value = f32::from_le_bytes(bytes.try_into()?);
            Ok(ThresholdValue::Number(float_value as f64))
        }
        "DOUBLE" => {
            if bytes.len() != 8 {
                return Err("Expected 8 bytes for DOUBLE".into());
            }
            let double_value = f64::from_le_bytes(bytes.try_into()?);
            Ok(ThresholdValue::Number(double_value))
        }
        "BYTE_ARRAY" => {
            use std::str;
            let s = str::from_utf8(bytes)?.to_owned();
            Ok(ThresholdValue::Utf8String(s))
        }
        _ => Err(format!("Unsupported column type: {}", column_type).into()),
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

