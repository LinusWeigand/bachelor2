use std::{error::Error, io::{Read, Seek, SeekFrom}, path::PathBuf, pin::Pin, process::exit, sync::Arc, time::Instant};

use arrow::array::RecordBatch;
use arrow2::{
    datatypes::Schema,
    io::parquet::read::{RowGroupMetaData},
};
use futures::{StreamExt};
use parquet::{
    arrow::{
        async_reader::ParquetRecordBatchStream, AsyncArrowWriter, ParquetRecordBatchStreamBuilder,
    },
    basic::Compression,
    file::{
        metadata::ParquetMetaData,
        properties::{EnabledStatistics, WriterProperties},
    },
};
use parquet2::{metadata::ColumnChunkMetaData, read::deserialize_metadata};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncSeekExt},
};

const INPUT_FILE_NAME: &str = "test_output40000.parquet";
const OUTPUT_FILE_NAME: &str = "test_output119000.parquet";
const _COLUMN_NAME: &str = "Float";
const ROWS_PER_GROUP: usize = 119000;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    println!("Printing out input sizes...");
    let file_path = PathBuf::from(INPUT_FILE_NAME);
    print_out_sizes(file_path.clone()).await?;

    println!("Changing Row Groups...");
    let start = Instant::now();
    let input_file = File::open(file_path).await?;
    let output_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(OUTPUT_FILE_NAME)
        .await?;
    change_row_groups(input_file, output_file, ROWS_PER_GROUP).await?;
    let elapsed = start.elapsed().as_millis();
    println!("Took: {:.2}", elapsed as f64 / 1000.);

    println!("Printing out output sizes...");
    let file_path = PathBuf::from(OUTPUT_FILE_NAME);
    print_out_sizes(file_path.clone()).await?;

    Ok(())
}

async fn print_out_sizes(file_path: PathBuf) -> Result<(), Box<dyn Error>>{
    let input_raw_footer = load_raw_footer(file_path.clone())?;
    let parsed_input_footer = parse_raw_footer(&input_raw_footer.raw_bytes, input_raw_footer.footer_size)?;
    let row_groups = parsed_input_footer.row_groups.len();
    let rows = parsed_input_footer.num_rows;
    let rows_per_group = rows / row_groups;
    println!("Row Groups: {}", row_groups);
    println!("Rows: {}", rows);
    println!("Rows per Group: {}", rows_per_group);

    let mut input_file = File::open(&file_path).await?;
    let file_size = input_file.seek(SeekFrom::End(0)).await?;
    let file_size_mb = file_size as f32 / 1000. / 1000.;
    let footer_size_mb = input_raw_footer.footer_size as f32 / 1000. / 1000.;
    let ratio = footer_size_mb / file_size_mb * 100.;
    println!("File Size: {:.2}MB", file_size_mb);
    println!("Footer Size: {:.2}MB", footer_size_mb);
    println!("Ratio: {:.2}%", ratio);
    println!("Goal Ratio: 0,057%");
    Ok(())
}

async fn change_row_groups(
    input_file: File,
    output_file: File,
    rows_per_group: usize,
) -> Result<(), Box<dyn Error>> {
    let builder = ParquetRecordBatchStreamBuilder::new(input_file)
        .await?
        .with_batch_size(rows_per_group);

    let mut stream = builder.build()?;
    let mut pinned_stream = Pin::new(&mut stream);

    let record_batch = get_next_item_from_reader(&mut pinned_stream).await;

    if record_batch.is_none() {
        return Err("File is empty".into());
    }
    let record_batch = record_batch.unwrap();

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(rows_per_group)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let mut writer = AsyncArrowWriter::try_new(output_file, record_batch.schema(), Some(props))?;
    writer.write(&record_batch).await?;
    while let Some(record_batch) = get_next_item_from_reader(&mut pinned_stream).await {
        writer.write(&record_batch).await?;
    }

    writer.flush().await?;
    writer.close().await?;
    Ok(())
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

fn _print_out_stats(metadata: &Arc<ParquetMetaData>) -> Result<(), Box<dyn Error>> {
    for (row_group_index, row_group) in metadata.row_groups().iter().enumerate() {
        if let Some(column) = row_group
            .columns()
            .iter()
            .find(|c| c.column_path().string() == _COLUMN_NAME)
        {
            let column_type = column.column_type().to_string();
            if let Some(stats) = column.statistics() {
                if let (Some(min_bytes), Some(max_bytes)) =
                    (stats.min_bytes_opt(), stats.max_bytes_opt())
                {
                    let min_value = _bytes_to_value(min_bytes, &column_type)?;
                    let max_value = _bytes_to_value(max_bytes, &column_type)?;
                    println!(
                        "Row Group: {}, MIN: {}, MAX: {}",
                        row_group_index, min_value, max_value
                    );
                } else {
                    println!("No min or max values found");
                }
            } else {
                println!("No statistics found");
            }
        }
    }
    Ok(())
}

fn _bytes_to_value(bytes: &[u8], column_type: &str) -> Result<f64, Box<dyn Error>> {
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

fn get_schema_size(schema: &Schema) -> usize {
    let serialized = bincode::serialize(schema).unwrap();
    serialized.len()
}

fn get_row_group_size(row_group: &RowGroupMetaData) -> usize {
    let serialized = match bincode::serialize(row_group) {
        Ok(v) => v,
        Err(e) => {
            println!("Error serializing RowGroupMetadata: {:?}", e);
            exit(1);
        }
    };
    serialized.len()
}
#[derive(Serialize, Deserialize)]
struct SerializableRowGroupMetaData {
    columns: Vec<ColumnChunkMetaData>,
    num_rows: usize,
    total_byte_size: usize,
}

// Implement conversion
impl From<&RowGroupMetaData> for SerializableRowGroupMetaData {
    fn from(row_group: &RowGroupMetaData) -> Self {
        SerializableRowGroupMetaData {
            columns: row_group.columns().into(),
            num_rows: row_group.num_rows(),
            total_byte_size: row_group.total_byte_size(),
        }
    }
}
#[derive(Debug)]
struct RawFooter {
    file_path: PathBuf,
    footer_size: usize,
    raw_bytes: Vec<u8>,
}

fn load_raw_footer(
    file_path: PathBuf,
) -> Result<RawFooter, Box<dyn Error>> {
    let mut file = std::fs::File::open(&file_path)?;

    // 2) Seek to the end - 8 bytes to read metadata length
    let file_size = file.seek(SeekFrom::End(0))?;
    if file_size < 12 {
        return Err(format!("File too small to be valid parquet: {:?}", &file_path).into());
    }

    // Read last 8 bytes: 4 bytes of metadata length, 4 bytes "PAR1" magic
    file.seek(SeekFrom::End(-8))?;
    let mut trailer = [0u8; 8];
    file.read_exact(&mut trailer)?;

    // Check the last 4 bytes are "PAR1"
    let magic = &trailer[4..];
    if magic != b"PAR1" {
        return Err(format!("Invalid Parquet file magic in {:?}", &file_path).into());
    }

    // The first 4 of those 8 bytes is the "metadata_length" (little-endian)
    let metadata_len = u32::from_le_bytes(trailer[0..4].try_into().unwrap());
    let metadata_len = metadata_len as usize;

    // 3) Seek back that many bytes + 4 for the end magic
    //    So total is metadata_len bytes + "PAR1"
    let footer_start = file_size
        .checked_sub(8 + metadata_len as u64)
        .ok_or_else(|| format!("metadata_len too large in {:?}", &file_path))?;
    file.seek(SeekFrom::Start(footer_start))?;

    // 4) Read raw footer bytes
    let mut raw_bytes = vec![0u8; metadata_len + 8];
    file.read_exact(&mut raw_bytes)?;

    // Return the raw bytes
    Ok(RawFooter {
        file_path,
        footer_size: raw_bytes.len(),
        raw_bytes,
    })

}


/// (Optional) Parse raw footer bytes into a `FileMetaData` using `parquet2`
/// This shows how you'd "expand" it on demand, but we normally won't do this if
/// we want to keep memory usage minimal.
fn parse_raw_footer(raw_bytes: &[u8], max_size: usize) -> Result<parquet2::metadata::FileMetaData, Box<dyn Error>> {
    // 1) The last 4 bytes must be b"PAR1" already included
    if raw_bytes.len() < 8 || &raw_bytes[raw_bytes.len() - 4..] != b"PAR1" {
        return Err("Not a valid parquet footer".into());
    }

    // 2) parquet2's low-level read:
    //    The function we call is `parquet2::metadata::deserialize_metadata`,
    //    which expects the slice *without* the trailing "PAR1".
    //    So let's remove the last 4 bytes and pass the rest.
    let slice_without_magic = &raw_bytes[..raw_bytes.len() - 4];
    let file_meta = deserialize_metadata(slice_without_magic, max_size)?;
    Ok(file_meta)
}
