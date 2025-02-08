use std::{error::Error, pin::Pin};

use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use parquet::{
    arrow::{
        async_reader::ParquetRecordBatchStream, AsyncArrowWriter, ParquetRecordBatchStreamBuilder,
    },
    basic::Compression,
    file::properties::{EnabledStatistics, WriterProperties},
};
use tokio::{
    fs::{read_dir, File, OpenOptions},
    io::BufWriter,
};

const INPUT_FOLDER_PATH: &str = "./snowset-main.parquet";
const NUM_OUTPUT_FILES: usize = 1;
const ROWS_PER_GROUP: usize = 550;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut dir = read_dir(INPUT_FOLDER_PATH).await?;
    let mut all_parquet_paths = Vec::new();
    while let Some(entry) = dir.next_entry().await? {
        let path = entry.path();
        if path.to_string_lossy().contains("merge") {
            continue;
        }
        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext.to_string_lossy().eq_ignore_ascii_case("parquet") {
                    all_parquet_paths.push(path);
                }
            }
        }
    }

    if all_parquet_paths.is_empty() {
        eprintln!("No .parquet files found in {}", INPUT_FOLDER_PATH);
        return Ok(());
    }

    all_parquet_paths.sort(); 

    // 2) Split into 16 chunks (last chunk might be smaller if not evenly divisible)
    let chunk_size = (all_parquet_paths.len() + NUM_OUTPUT_FILES - 1) / NUM_OUTPUT_FILES;
    let mut file_chunks = all_parquet_paths.chunks(chunk_size).enumerate();

    // 3) For each chunk, create one merged output parquet file
    let mut counter = 0;
    while let Some((chunk_index, chunk_paths)) = file_chunks.next() {
        counter += 1;
        if counter < 8 && counter != 1 {
            continue;
        }
        if chunk_paths.is_empty() {
            continue;
        }

        let output_file_name = format!("merged_{:02}.parquet", chunk_index + 1);
        let output_file_path = format!("{}/{}", INPUT_FOLDER_PATH, output_file_name);

        // We'll create our AsyncArrowWriter once we know the schema,
        // which we get from the first non-empty file in this chunk.
        let mut maybe_writer = None;

        for (idx, in_path) in chunk_paths.iter().enumerate() {
            // Open the input file
            let input_file = match File::open(in_path).await {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("Skipping file {:?}: {}", in_path, e);
                    continue;
                }
            };

            // Build a ParquetRecordBatchStream
            let builder_result = ParquetRecordBatchStreamBuilder::new(input_file).await;
            let builder = match builder_result {
                Ok(b) => b.with_batch_size(ROWS_PER_GROUP),
                Err(e) => {
                    eprintln!("Skipping file {:?} due to error: {}", in_path, e);
                    continue;
                }
            };
            let mut stream = match builder.build() {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Skipping file {:?} due to error: {}", in_path, e);
                    continue;
                }
            };
            let mut pinned_stream = Pin::new(&mut stream);

            // If we haven't created a writer yet, do so after reading the first non-empty batch
            if maybe_writer.is_none() {
                // Grab the first batch (if any)
                if let Some(first_batch) = get_next_item_from_reader(&mut pinned_stream).await {
                    let schema = first_batch.schema();

                    // Configure writer properties
                    let props = WriterProperties::builder()
                        .set_compression(Compression::SNAPPY)
                        .set_max_row_group_size(ROWS_PER_GROUP)
                        .set_statistics_enabled(EnabledStatistics::Chunk)
                        .build();

                    // Open the output file (only once per chunk)
                    let output_file = OpenOptions::new()
                        .create(true)
                        .truncate(true)
                        .write(true)
                        .open(&output_file_path)
                        .await?;

                    // Wrap it with BufWriter (recommended for Async I/O)
                    let buf_writer = BufWriter::new(output_file);

                    // Create our AsyncArrowWriter
                    let mut writer = AsyncArrowWriter::try_new(buf_writer, schema, Some(props))?;

                    // Write the first batch we already retrieved
                    writer.write(&first_batch).await?;

                    // Store the writer in an Option
                    maybe_writer = Some(writer);
                } else {
                    // If the file is completely empty, just move on
                    continue;
                }
            }

            // Now we have a writer; write the remaining record batches from this file
            while let Some(batch) = get_next_item_from_reader(&mut pinned_stream).await {
                if let Some(writer) = maybe_writer.as_mut() {
                    writer.write(&batch).await?;
                }
            }

            println!(
                "File {} of chunk {} processed: {:?}",
                idx + 1,
                chunk_index + 1,
                in_path.file_name().unwrap_or_default()
            );
        } // end for each file in chunk

        // After finishing the chunk, finalize (flush + close) the writer
        if let Some(mut writer) = maybe_writer {
            writer.flush().await?;
            writer.close().await?;
            println!(
                "Finished merging chunk {} -> {}",
                chunk_index + 1,
                output_file_path
            );
        } else {
            // Means every file in this chunk was empty or had errors
            eprintln!(
                "No valid data in chunk {}, no output file created.",
                chunk_index + 1
            );
        }
    }

    println!(
        "All done: Merged files into up to {} outputs.",
        NUM_OUTPUT_FILES
    );
    Ok(())
}

/// Helper function that reads the next `RecordBatch` from a pinned stream.
/// Returns `None` if there are no more batches or we hit an error.
async fn get_next_item_from_reader(
    pinned_stream: &mut Pin<&mut ParquetRecordBatchStream<File>>,
) -> Option<RecordBatch> {
    match pinned_stream.as_mut().next().await {
        Some(Ok(batch)) => Some(batch),
        Some(Err(e)) => {
            eprintln!("Error reading batch: {:?}", e);
            None
        }
        None => None,
    }
}
