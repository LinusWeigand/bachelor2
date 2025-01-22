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

const INPUT_FILE_NAME: &str = "output.parquet";
const COLUMN_NAME: &str = "memoryUsed";

#[derive(PartialEq)]
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

pub struct Condition {
    pub column_name: String,
    pub threshold: f64,
    pub comparison: Comparison,
}

pub enum Expression {
    Condition(Condition),
    And(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
}

impl Comparison {
    pub fn keep(&self, min_value: f64, max_value: f64, threshold: f64) -> bool {
        match self {
            Comparison::LessThan => min_value < threshold,
            Comparison::LessThanOrEqual => min_value <= threshold,
            Comparison::Equal => threshold >= min_value && threshold <= max_value,
            Comparison::GreaterThanOrEqual => max_value >= threshold,
            Comparison::GreaterThan => max_value > threshold,
        }
    }
}

pub struct MetadataEntry {
    file_path: PathBuf,
    metadata: ArrowReaderMetadata,
    column_index_map: HashMap<String, usize>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Store metadata
    let mut cached_metadata: Vec<MetadataEntry> = Vec::new();

    let file_path = PathBuf::from(INPUT_FILE_NAME);
    let mut file = File::open(&file_path).await?;
    let metadata = ArrowReaderMetadata::load_async(&mut file, Default::default()).await?;
    let file_metadata = metadata.metadata().file_metadata();
    let column_index_map = get_column_name_to_index_map(&file_metadata);

    let metadata_entry = MetadataEntry {
        file_path,
        metadata,
        column_index_map,
    };

    cached_metadata.push(metadata_entry);

    // Retrieve Metadata
    let file_path = PathBuf::from(INPUT_FILE_NAME);
    let metadata_entry = cached_metadata
        .iter()
        .find(|entry| entry.file_path == file_path);

    let metadata_entry = match metadata_entry {
        Some(v) => v,
        None => {
            let mut file = File::open(&file_path).await?;
            let metadata = ArrowReaderMetadata::load_async(&mut file, Default::default()).await?;
            let file_metadata = metadata.metadata().file_metadata();
            let column_index_map = get_column_name_to_index_map(&file_metadata);
            //TODO: Insert into cache
            &MetadataEntry {
                file_path,
                metadata,
                column_index_map,
            }
        }
    };

    // let input = format!(
    //     "{} == {} AND {} == {}",
    //     COLUMN_NAME, 30_000_000 as i64, COLUMN_NAME, 100_000_000_000 as i64,
    // );
    //
    let input = format!("endTime < 2018-03-02 14:23:41");
    let expression = parse_expression(&input)?;

    let select_columns = vec!["memoryUsed".to_owned()];

    let _result = smart_query_parquet(&metadata_entry, &expression, &select_columns).await?;
    Ok(())
}

pub async fn smart_query_parquet(
    metadata_entry: &MetadataEntry,
    expression: &Expression,
    select_columns: &Vec<String>,
) -> Result<RecordBatch, Box<dyn Error>> {
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
        let is_keep = keep_row_group(row_group_metadata, expression)?;
        println!("Row Group: {}, Skip: {}", i, is_keep);
        if is_keep {
            row_groups.push(i);
        }
    }

    println!("Filtered Row Groups: {}", row_groups.len());

    // let row_filter = RowFilter::new();

    let mut stream = builder
        .with_projection(mask)
        .with_row_groups(row_groups)
        // .with_row_filter(RowFi)
        .build()?;
    let mut pinned_stream = Pin::new(&mut stream);

    let record_batch = get_next_item_from_reader(&mut pinned_stream).await;

    if record_batch.is_none() {
        return Err("File is empty".into());
    }
    let record_batch = record_batch.unwrap();

    Ok(record_batch)
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
    expression: &Expression,
) -> Result<bool, Box<dyn Error>> {
    match expression {
        Expression::Condition(condition) => {
            if let Some(column) = row_group
                .columns()
                .iter()
                .find(|c| c.column_path().string() == condition.column_name)
            {
                let column_type = column.column_type().to_string();
                if let Some(stats) = column.statistics() {
                    if let (Some(min_bytes), Some(max_bytes)) =
                        (stats.min_bytes_opt(), stats.max_bytes_opt())
                    {
                        let min_value = bytes_to_value(min_bytes, &column_type)?;
                        let max_value = bytes_to_value(max_bytes, &column_type)?;
                        let threshold = condition.threshold;
                        let result = condition.comparison.keep(min_value, max_value, threshold);
                        return Ok(result);
                    }
                }
            }
            Ok(false)
        }
        //
        Expression::And(left, right) => {
            Ok(keep_row_group(row_group, left)? && keep_row_group(row_group, right)?)
        }
        Expression::Or(left, right) => {
            Ok(keep_row_group(row_group, left)? || keep_row_group(row_group, right)?)
        }
        Expression::Not(inner) => Ok(!keep_row_group(row_group, inner)?),
    }
}

fn get_filter_column_name(expression: &Expression) -> String {
    match expression {
        Expression::Condition(condition) => condition.column_name.clone(),
        Expression::And(left, _) => get_filter_column_name(left),
        Expression::Or(left, _) => get_filter_column_name(left),
        Expression::Not(inner) => get_filter_column_name(inner),
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

fn bytes_to_value(bytes: &[u8], column_type: &str) -> Result<f64, Box<dyn Error>> {
    match column_type {
        "INT32" => {
            if bytes.len() != 4 {
                return Err("Expected 4 bytes for INT32".into());
            }
            let int_value = i32::from_le_bytes(bytes.try_into()?);
            Ok(int_value as f64)
        }
        "INT64" => {
            if bytes.len() != 8 {
                return Err("Expected 8 bytes for INT64".into());
            }
            let int_value = i64::from_le_bytes(bytes.try_into()?);
            Ok(int_value as f64)
        }
        "FLOAT" => {
            if bytes.len() != 4 {
                return Err("Expected 4 bytes for FLOAT".into());
            }
            let float_value = f32::from_le_bytes(bytes.try_into()?);
            Ok(float_value as f64)
        }
        "DOUBLE" => {
            if bytes.len() != 8 {
                return Err("Expected 8 bytes for DOUBLE".into());
            }
            let double_value = f64::from_le_bytes(bytes.try_into()?);
            Ok(double_value)
        }
        _ => Err(format!("Unsupported column type: {}", column_type).into()),
    }
}

fn parse_expression(input: &str) -> Result<Expression, Box<dyn Error>> {
    let tokens = tokenize(input)?;
    let mut pos = 0;
    parse_or(&tokens, &mut pos)
}

fn tokenize(input: &str) -> Result<Vec<String>, Box<dyn Error>> {
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

fn parse_or(tokens: &[String], pos: &mut usize) -> Result<Expression, Box<dyn Error>> {
    let mut expr = parse_and(tokens, pos)?;

    while *pos < tokens.len() && tokens[*pos] == "OR" {
        *pos += 1;
        let right = parse_and(tokens, pos)?;
        expr = Expression::Or(Box::new(expr), Box::new(right));
    }

    Ok(expr)
}

fn parse_and(tokens: &[String], pos: &mut usize) -> Result<Expression, Box<dyn Error>> {
    let mut expr = parse_not(tokens, pos)?;

    while *pos < tokens.len() && tokens[*pos] == "AND" {
        *pos += 1;
        let right = parse_not(tokens, pos)?;
        expr = Expression::And(Box::new(expr), Box::new(right));
    }

    Ok(expr)
}

fn parse_not(tokens: &[String], pos: &mut usize) -> Result<Expression, Box<dyn Error>> {
    if *pos < tokens.len() && tokens[*pos] == "NOT" {
        *pos += 1;
        let expr = parse_primary(tokens, pos)?;
        return Ok(Expression::Not(Box::new(expr)));
    }

    parse_primary(tokens, pos)
}

fn parse_primary(tokens: &[String], pos: &mut usize) -> Result<Expression, Box<dyn Error>> {
    if *pos >= tokens.len() {
        return Err("Unexpected end of input".into());
    }

    if tokens[*pos] == "(" {
        *pos += 1;
        let expr = parse_or(tokens, pos)?;
        if *pos >= tokens.len() || tokens[*pos] != ")" {
            return Err("Expected closing parenthesis".into());
        }
        *pos += 1;
        return Ok(expr);
    }

    // Parse condition
    let column_name = tokens[*pos].clone();
    *pos += 1;

    if *pos >= tokens.len() {
        return Err("Expected comparison operator".into());
    }

    let comparison = Comparison::from_str(&tokens[*pos]).ok_or("Invalid comparison operator")?;
    *pos += 1;

    if *pos >= tokens.len() {
        return Err("Expected threshold value".into());
    }

    let threshold: f64 = tokens[*pos].parse()?;
    *pos += 1;

    Ok(Expression::Condition(Condition {
        column_name,
        comparison,
        threshold,
    }))
}
