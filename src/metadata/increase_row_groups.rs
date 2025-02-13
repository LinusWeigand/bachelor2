use std::{error::Error, pin::Pin};

use arrow::array::RecordBatch;
use futures::StreamExt;
use parquet::{
    arrow::{
        async_reader::ParquetRecordBatchStream, AsyncArrowWriter, ParquetRecordBatchStreamBuilder,
    },
    basic::Compression,
    file::properties::{EnabledStatistics, WriterProperties},
};
use tokio::fs::{read_dir, remove_file, File, OpenOptions};

const FOLDER_PATH: &str = "./snowset-main.parquet";
const ROWS_PER_GROUP: usize = 550;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut dir = read_dir(FOLDER_PATH).await?;

    let mut progress = 0;

    while let Some(entry) = dir.next_entry().await? {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let extension = match path.extension() {
            Some(v) => v,
            None => continue,
        };
        if !extension.to_string_lossy().ends_with("parquet") {
            continue;
        }
        let parent = match path.parent() {
            Some(v) => v,
            None => continue,
        };
        let stem = match path.file_stem() {
            Some(v) => v,
            None => continue,
        };

        if stem.to_string_lossy().contains("uncomp") {
            continue;
        }

        progress += 1;
        println!("Progress: {:.2}%", progress as f64 / 2002. * 100.);

        let file_name = format!(
            "{}.uncomp.{}",
            stem.to_string_lossy(),
            extension.to_string_lossy()
        );
        let file_path = parent.join(file_name);

        if file_path.exists() && file_path.is_file() {
            continue;
        }

        let input_file = File::open(&path).await?;
        let output_file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&file_path)
            .await?;

        let builder = match ParquetRecordBatchStreamBuilder::new(input_file).await {
            Ok(v) => v.with_batch_size(ROWS_PER_GROUP),
            Err(e) => {
                eprintln!("{}", e);
                println!("File: {:?}", file_path);
                remove_file(&path).await?;
                continue;
            }
        };

        let mut stream = builder.build()?;
        let mut pinned_stream = Pin::new(&mut stream);

        let record_batch = get_next_item_from_reader(&mut pinned_stream).await;

        if record_batch.is_none() {
            return Err("File is empty".into());
        }
        let record_batch = record_batch.unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_max_row_group_size(ROWS_PER_GROUP)
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();

        let mut writer =
            AsyncArrowWriter::try_new(output_file, record_batch.schema(), Some(props))?;
        writer.write(&record_batch).await?;
        while let Some(record_batch) = get_next_item_from_reader(&mut pinned_stream).await {
            writer.write(&record_batch).await?;
        }

        writer.flush().await?;
        writer.close().await?;
    }
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
