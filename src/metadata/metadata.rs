use std::{error::Error, path::PathBuf, pin::Pin, process::exit, sync::Arc};
 

use arrow::array::RecordBatch;
use arrow2::{datatypes::Schema, io::parquet::read::{infer_schema, read_metadata_async, RowGroupMetaData }};
use futures::StreamExt;
use parquet::{arrow::{async_reader::ParquetRecordBatchStream, AsyncArrowWriter, ParquetRecordBatchStreamBuilder}, basic::Compression, file::{metadata::ParquetMetaData, properties::{EnabledStatistics, WriterProperties}}};
use parquet2::metadata::ColumnChunkMetaData;
use serde::{Deserialize, Serialize};
use tokio::{fs::{File, OpenOptions}, io::BufReader};
use tokio_util::compat::TokioAsyncReadCompatExt;

const INPUT_FILE_NAME: &str = "test.parquet";
const OUTPUT_FILE_NAME: &str = "test_output.parquet";
const _COLUMN_NAME: &str = "Float";
const ROWS_PER_GROUP: usize = 550;

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let file_path = PathBuf::from(INPUT_FILE_NAME);
    let input_file = File::open(&file_path).await?;
    let output_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(OUTPUT_FILE_NAME)
        .await?;
    increase_row_groups(input_file, output_file, ROWS_PER_GROUP).await?;
    let mut file = File::open(OUTPUT_FILE_NAME).await?;


    let file_size = file.metadata().await?.len();
    println!("File size: {}", file_size);
    println!("File size: {:.2}MB", file_size as f64 / 1000. / 1000.);


    let file_bytes = file_size * 1000;
    let mut buf_reader = BufReader::new(file).compat();
    let metadata = read_metadata_async(&mut buf_reader).await?;
    let schema = infer_schema(&metadata)?;
    let row_groups = metadata.row_groups;

    let schema_bytes = get_schema_size(&schema);
    let row_group_bytes = row_groups.iter().map(|v| {
        let serializable: SerializableRowGroupMetaData = v.into();
        let serialized = bincode::serialize(&serializable).unwrap();
        serialized.len()
    }).sum::<usize>();

    let metadata_bytes: usize = schema_bytes + row_group_bytes;



    let ratio = metadata_bytes as f64 / file_bytes as f64;
    println!("Ratio: {:.4}%", ratio * 100.);
    println!("Ziel: 0.05%");
    let num_row_groups = row_groups.len();
    let num_rows = metadata.num_rows;
    let rows_per_group = num_rows as usize / num_row_groups;
    println!("Num Rows: {}, Num Groups: {}, Rows per Group: {}", num_rows, num_row_groups, rows_per_group);

    Ok(())
}

async fn increase_row_groups(
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

fn _print_out_stats(metadata: &Arc<ParquetMetaData>) -> Result<(), Box<dyn Error>>{
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
