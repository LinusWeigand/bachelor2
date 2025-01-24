use std::{collections::HashMap, error::Error, path::PathBuf, pin::Pin};

use arrow::array::RecordBatch;
use futures::StreamExt;
use parquet::{
    arrow::{
        arrow_reader::ArrowReaderMetadata, async_reader::ParquetRecordBatchStream,
        ParquetRecordBatchStreamBuilder, ProjectionMask,
    },
    file::metadata::{FileMetaData, RowGroupMetaData},
};
use tokio::fs::File;

use crate::bloom_filter::BloomFilter;

#[derive(PartialEq, Clone, Debug)]
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

#[derive(Clone, Debug)]
pub struct Condition {
    pub column_name: String,
    pub threshold: ThresholdValue,
    pub comparison: Comparison,
}

#[derive(Debug, Clone)]
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

pub fn compare<T: Ord>(min: T, max: T, v: T, comparison: &Comparison, not: bool) -> bool {
    match comparison {
        Comparison::LessThan => match not {
            false => min < v,
            true => max >= v,
        },
        Comparison::LessThanOrEqual => match not {
            false => min <= v,
            true => max > v,
        },
        Comparison::Equal => match not {
            false => v >= min && v <= max,
            true => !(v == min && v == max),
        },
        Comparison::GreaterThanOrEqual => match not {
            false => max >= v,
            true => min < v,
        },
        Comparison::GreaterThan => match not {
            false => max > v,
            true => min <= v,
        },
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
pub fn compare_floats<T: Float>(min: T, max: T, v: T, comparison: &Comparison, not: bool) -> bool {
    match comparison {
        Comparison::LessThan => match not {
            false => min < v,
            true => max >= v,
        },
        Comparison::LessThanOrEqual => match not {
            false => min <= v,
            true => max > v,
        },
        Comparison::Equal => match not {
            false => v >= min && v <= max,
            true => !(v.equal(min) && v.equal(max)),
        },
        Comparison::GreaterThanOrEqual => match not {
            false => max >= v,
            true => min < v,
        },
        Comparison::GreaterThan => match not {
            false => max > v,
            true => min <= v,
        },
    }
}

impl Comparison {
    pub fn keep(
        &self,
        row_group_min: &ThresholdValue,
        row_group_max: &ThresholdValue,
        user_threshold: &ThresholdValue,
        not: bool,
    ) -> bool {
        match (row_group_min, row_group_max, user_threshold) {
            (ThresholdValue::Int64(min), ThresholdValue::Int64(max), ThresholdValue::Int64(v)) => {
                compare(min, max, v, self, not)
            }
            (
                ThresholdValue::Boolean(min),
                ThresholdValue::Boolean(max),
                ThresholdValue::Boolean(v),
            ) => match self {
                Comparison::LessThan => true,
                Comparison::LessThanOrEqual => true,
                Comparison::Equal => match not {
                    false => v == min || v == max,
                    true => v != min || v != max,
                },
                Comparison::GreaterThanOrEqual => true,
                Comparison::GreaterThan => true,
            },
            (
                ThresholdValue::Utf8String(min),
                ThresholdValue::Utf8String(max),
                ThresholdValue::Utf8String(v),
            ) => compare(min, max, v, self, not),
            _ => true,
        }
    }
}

pub struct MetadataEntry {
    pub file_path: PathBuf,
    pub metadata: ArrowReaderMetadata,
    pub column_maps: ColumnMaps,
}

pub async fn smart_query_parquet(
    metadata_entry: &MetadataEntry,
    bloom_filters: Vec<Vec<Option<BloomFilter>>>,
    expression: Option<Expression>,
    select_columns: Option<Vec<String>>,
) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
    let file = File::open(&metadata_entry.file_path).await?;

    let metadata = metadata_entry.metadata.clone();
    let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(file, metadata.clone());
    let metadata = metadata.metadata();

    let mut stream = builder;

    if let Some(select_columns) = select_columns {
        let column_indices: Vec<usize> = select_columns
            .iter()
            .filter_map(|column| {
                metadata_entry
                    .column_maps
                    .name_to_index
                    .get(column)
                    .map(|&x| x)
            })
            .collect();

        let mask = ProjectionMask::roots(metadata.file_metadata().schema_descr(), column_indices);

        stream = stream.with_projection(mask);
    }

    if let Some(expression) = expression {
        // Filter Row Groups
        let mut row_groups: Vec<usize> = Vec::new();

        for i in 0..metadata.num_row_groups() {
            let row_group_metadata = metadata.row_group(i);
            let is_keep = keep_row_group(
                row_group_metadata,
                &bloom_filters[i],
                i,
                &expression,
                false,
                &metadata_entry.column_maps,
            )?;
            println!("Row Group: {}, Skip: {}", i, !is_keep);
            if is_keep {
                row_groups.push(i);
            }
        }
        println!("Filtered Row Groups: {}", row_groups.len());

        stream = stream.with_row_groups(row_groups)
        // Filter Rows
        // let filter_columns: Vec<String> = get_column_projection_from_expression(&expression);
        // let column_indices: Vec<usize> = filter_columns
        //     .iter()
        //     .filter_map(|column| metadata_entry.column_maps.name_to_index.get(column).map(|&x| x))
        //     .collect();
        // let projection =
        //     ProjectionMask::roots(metadata.file_metadata().schema_descr(), column_indices);
        // let predicate: Box<dyn ArrowPredicate> = Box::new(ArrowPredicateFn::new(
        //     projection,
        //     predicate_function(expression.clone()),
        // ));
        // let predicates: Vec<Box<dyn ArrowPredicate>> = vec![predicate];
        // let _row_filter: RowFilter = RowFilter::new(predicates);
        // stream = stream
        // .with_row_filter(row_filter);
    }

    let mut stream = stream.build()?;
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
    row_group_index: usize,
    expression: &Expression,
    not: bool,
    column_maps: &ColumnMaps,
) -> Result<bool, Box<dyn Error>> {
    match expression {
        Expression::Condition(condition) => {
            if let Some((column_index, column)) = row_group
                .columns()
                .iter()
                .enumerate()
                .find(|(_, c)| c.column_path().string() == condition.column_name)
            {
                let column_type = column.column_type().to_string();

                let bloom_filter = match bloom_filters.get(column_index) {
                    Some(v) => v,
                    None => &None,
                };

                if let Some(stats) = column.statistics() {
                    if let (Some(min_bytes), Some(max_bytes)) =
                        (stats.min_bytes_opt(), stats.max_bytes_opt())
                    {
                        let min_value = bytes_to_value(min_bytes, &column_type)?;
                        let max_value = bytes_to_value(max_bytes, &column_type)?;
                        println!("MIN: {:#?}", min_value);
                        println!("MAX: {:#?}", max_value);
                        println!("Threshold: {:#?}", &condition.threshold);
                        let mut result = condition.comparison.keep(
                            &min_value,
                            &max_value,
                            &condition.threshold,
                            not,
                        );
                        if result && condition.comparison == Comparison::Equal {
                            if let Some(bloom_filter) = bloom_filter {
                                result = match &condition.threshold {
                                    ThresholdValue::Int64(v) => bloom_filter.contains(&v),
                                    ThresholdValue::Boolean(v) => bloom_filter.contains(&v),
                                    ThresholdValue::Utf8String(v) => bloom_filter.contains(&v),
                                    ThresholdValue::Float64(v) => {
                                        let value = *v as i32;
                                        bloom_filter.contains(&value)
                                    }
                                };

                                println!("Bloom Result: {}", result);
                            }
                        }

                        return Ok(result);
                    }
                }
            }
            Ok(false)
        }
        Expression::And(left, right) => Ok(match not {
            true => {
                keep_row_group(
                    row_group,
                    bloom_filters,
                    row_group_index,
                    left,
                    true,
                    column_maps,
                )? || keep_row_group(
                    row_group,
                    bloom_filters,
                    row_group_index,
                    right,
                    true,
                    column_maps,
                )?
            }
            false => {
                keep_row_group(
                    row_group,
                    bloom_filters,
                    row_group_index,
                    left,
                    false,
                    column_maps,
                )? && keep_row_group(
                    row_group,
                    bloom_filters,
                    row_group_index,
                    right,
                    false,
                    column_maps,
                )?
            }
        }),
        Expression::Or(left, right) => Ok(match not {
            true => {
                keep_row_group(
                    row_group,
                    bloom_filters,
                    row_group_index,
                    left,
                    true,
                    column_maps,
                )? && keep_row_group(
                    row_group,
                    bloom_filters,
                    row_group_index,
                    right,
                    true,
                    column_maps,
                )?
            }
            false => {
                keep_row_group(
                    row_group,
                    bloom_filters,
                    row_group_index,
                    left,
                    false,
                    column_maps,
                )? || keep_row_group(
                    row_group,
                    bloom_filters,
                    row_group_index,
                    right,
                    false,
                    column_maps,
                )?
            }
        }),
        Expression::Not(inner) => Ok(keep_row_group(
            row_group,
            bloom_filters,
            row_group_index,
            inner,
            !not,
            column_maps,
        )?),
    }
}

pub struct ColumnMaps {
    pub index_to_name: HashMap<usize, String>,
    pub name_to_index: HashMap<String, usize>,
}

// pub struct ColumnMaps {
//     pub row_group_column_maps: HashMap<usize, RowGroupColumnMaps>,
// }

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

fn bytes_to_value(bytes: &[u8], column_type: &str) -> Result<ThresholdValue, Box<dyn Error>> {
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
