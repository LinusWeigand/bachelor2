use std::{error::Error, path::PathBuf, pin::Pin};

use arrow::array::RecordBatch;
use futures::StreamExt;
use parquet::{
    arrow::{
        arrow_reader::ArrowReaderMetadata, async_reader::ParquetRecordBatchStream, ParquetRecordBatchStreamBuilder, ProjectionMask
    },
    file::metadata::RowGroupMetaData,
};
use tokio::fs::File;

const INPUT_FILE_NAME: &str = "output.parquet";
const MEMORY_MEAN: f64 = 6729298150.;
const COL_NAME: &str = "memoryUsed";
static SELECT_INDICES: [usize; 3] = [0, 1, 3];

pub enum Comparison {
    LessThan,
    LessThanOrEqual,
    Equal,
    GreaterThanOrEqual,
    GreaterThan
}

pub struct Condition {
    pub column_name: String,
    pub threshold: f64,
    pub comparison: Comparison,
} 

pub enum Expression {
    Comparison(Comparison),
    And(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
}

impl Comparison {
    pub fn can_skip(&self, min_value: f64, max_value: f64, threshold: f64) -> bool {
        match self {
            Comparison::LessThan => threshold < min_value,
            Comparison::LessThanOrEqual => threshold <= min_value,
            Comparison::Equal => !(threshold >= min_value && threshold <= max_value),
            Comparison::GreaterThanOrEqual => threshold >= max_value,
            Comparison::GreaterThan => threshold > max_value,
        }
    }
}

pub struct MetadataEntry {
    file_path: PathBuf,
    metadata: ArrowReaderMetadata,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Store metadata
    let mut cached_metadata: Vec<MetadataEntry> = Vec::new();

    let file_path = PathBuf::from(INPUT_FILE_NAME);
    let mut file = File::open(&file_path).await?;
    let metadata = ArrowReaderMetadata::load_async(&mut file, Default::default()).await?;

    let metadata_entry = MetadataEntry {
        file_path,
        metadata,
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
            //TODO: Insert into cache
            &MetadataEntry {
                file_path,
                metadata,
            }
        }
    };

    let _result = smart_query_parquet_gt(COL_NAME, MEMORY_MEAN, &metadata_entry, Comparison::GreaterThan).await?;
    Ok(())
}

pub async fn smart_query_parquet_gt(
    col_name: &str,
    threshold: f64,
    metadata_entry: &MetadataEntry,
    comparison: Comparison,
) -> Result<RecordBatch, Box<dyn Error>> {
    let file = File::open(&metadata_entry.file_path).await?;

    let metadata = metadata_entry.metadata.clone();
    let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(file, metadata.clone());
    let metadata = metadata.metadata();

    let _mask = ProjectionMask::roots(metadata.file_metadata().schema_descr(), SELECT_INDICES);

    let mut row_groups: Vec<usize> = Vec::new();

    for i in 0..metadata.num_row_groups() {
        let row_group_metadata = metadata.row_group(i);
        if !can_skip_row_group(row_group_metadata, col_name, threshold, &comparison)? {
            row_groups.push(i);
        }
    }

    println!("Filtered Row Groups: {}", row_groups.len());

    // let row_filter = RowFilter::new();

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

pub fn can_skip_row_group(
    row_group: &RowGroupMetaData,
    column_name: &str,
    threshold: f64,
    comparison: &Comparison,
) -> Result<bool, Box<dyn Error>> {
    if let Some(column) = row_group
        .columns()
        .iter()
        .find(|c| c.column_path().string() == column_name)
    {
        let column_type = column.column_type().to_string();
        if let Some(stats) = column.statistics() {
            if let (Some(min_bytes), Some(max_bytes)) = (stats.min_bytes_opt(), stats.max_bytes_opt()) {
                let min_value = bytes_to_value(min_bytes, &column_type)?;
                let max_value = bytes_to_value(max_bytes, &column_type)?;
                println!("Min value: {}", min_value);
                println!("Max value: {}", max_value);

                let result = comparison.can_skip(min_value, max_value, threshold);
                println!("Comparison Result: {}", result);
                return Ok(result);
            }
        }
    }
    Err("Statistics not found".into())
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
