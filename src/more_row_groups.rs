use std::{error::Error, pin::Pin};

use arrow::array::RecordBatch;
use futures::StreamExt;
use parquet::{
    arrow::{
        async_reader::ParquetRecordBatchStream, AsyncArrowWriter, ParquetRecordBatchStreamBuilder,
    },
    basic::Compression,
    file::properties::WriterProperties,
};
use tokio::fs::{File, OpenOptions};

const INPUT_FILE: &str = "test.parquet";
const OUTPUT_FILE: &str = "output.parquet";
const ROWS_PER_GROUP: usize = 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let input_file = OpenOptions::new()
        .read(true)
        .write(false)
        .append(false)
        .create(false)
        .open(INPUT_FILE)
        .await?;

    let output_file = OpenOptions::new()
        .read(true)
        .write(true)
        .append(false)
        .create(true)
        .open(OUTPUT_FILE)
        .await?;

    increase_row_groups(input_file, output_file, ROWS_PER_GROUP).await?;
    Ok(())
}

pub async fn increase_row_groups(
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
        .set_max_row_group_size(ROWS_PER_GROUP)
        .build();

    let mut writer = AsyncArrowWriter::try_new(output_file, record_batch.schema(), Some(props))?;

    writer.write(&record_batch).await?;
    writer.flush().await?;

    while let Some(record_batch) = get_next_item_from_reader(&mut pinned_stream).await {
        writer.write(&record_batch).await?;
        writer.flush().await?;
    }

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
