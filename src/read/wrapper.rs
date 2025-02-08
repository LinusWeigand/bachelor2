
use arrow::array::RecordBatch;
use futures::StreamExt;
use parquet::arrow::{
    arrow_reader::ArrowReaderMetadata,
    async_reader::ParquetRecordBatchStream,
    ParquetRecordBatchStreamBuilder,
};
use std::{
    env,
    io::{Cursor, SeekFrom},
    process::exit,
    time::Instant,
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
};

const FILE_PATHS: [&str; 8] = [
    "merged_01.parquet",
    "merged_02.parquet",
    "merged_03.parquet",
    "merged_04.parquet",
    "merged_05.parquet",
    "merged_06.parquet",
    "merged_07.parquet",
    "merged_08.parquet",
];

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // Parse command-line arguments.
    let mut folder = "./snowset-main.parquet".to_string();
    let mut max_counts: usize = 8;
    let args: Vec<String> = env::args().collect();
    let mut iter = args.iter().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-p" | "--path" => {
                if let Some(v) = iter.next() {
                    folder = v.to_owned();
                } else {
                    eprintln!("Error: -p/--path requires an argument.");
                    exit(1);
                }
            }
            "-c" | "--count" => {
                if let Some(v) = iter.next() {
                    max_counts = v.parse().unwrap();
                } else {
                    eprintln!("Error: -c/--count requires an argument.");
                    exit(1);
                }
            }
            _ => {
                eprintln!("Ignoring unknown argument: {}", arg);
            }
        }
    }

    // Pre-load metadata for each file.
    let mut file_data = Vec::new();
    println!("Reading Metadata...");
    for file_name in FILE_PATHS.iter().take(max_counts) {
        let path = format!("{}/{}", folder, file_name);
        let mut file = File::open(&path).await?;
        let metadata = ArrowReaderMetadata::load_async(&mut file, Default::default()).await?;
        println!(
            "Loaded {} with size: {:.2} MB",
            &path,
            metadata.metadata().memory_size() as f64 / (1024.0 * 1024.0)
        );
        file_data.push((path, metadata));
    }

    println!("Starting Benchmark...");
    let start = Instant::now();

    // Process each file sequentially.
    for (file_path, metadata) in file_data {
        println!("Processing file: {}", file_path);

        // Get full Parquet metadata.
        let full_meta = metadata.metadata();
        let num_row_groups = full_meta.num_row_groups();

        // Open the file for sequential reading.
        let mut file = File::open(&file_path).await?;
        let file_size = file.metadata().await?.len();

        // We'll compute the row group start if the metadata reports 0.
        // Assume a header size of 8 bytes (adjust if needed).
        let mut computed_offset: u64 = 4;

        // Process each row group.
        for rg_index in 0..num_row_groups {
            let rg_meta = full_meta.row_group(rg_index);
            let reported_offset = rg_meta.column(0).file_offset() as u64;
            // Use the reported offset if nonzero; otherwise, use our computed value.
            let start_offset = if reported_offset == 0 {
                computed_offset
            } else {
                reported_offset
            };

            // Get the row group's length as reported.
            let length = rg_meta.total_byte_size() as u64;
            if length == 0 {
                println!("Row group {} reports 0 bytes, skipping.", rg_index);
                continue;
            }

            // (Optional) Log if computed offset and reported offset differ.
            if reported_offset == 0 {
                println!(
                    "Row group {}: Using computed start offset {} (reported offset was 0)",
                    rg_index, start_offset
                );
            } else if start_offset != reported_offset {
                println!(
                    "Row group {}: Using reported offset {} (computed offset was {})",
                    rg_index, reported_offset, computed_offset
                );
            }

            // Ensure we don't try to read past the file end.
            let available = if start_offset + length > file_size {
                file_size.saturating_sub(start_offset)
            } else {
                length
            };

            // Seek to the proper offset if necessary.
            let current_pos = file.seek(SeekFrom::Current(0)).await?;
            if current_pos < start_offset {
                let to_skip = start_offset - current_pos;
                println!(
                    "Row group {}: Skipping {} bytes to reach offset {}",
                    rg_index, to_skip, start_offset
                );
                skip_bytes(&mut file, to_skip).await?;
            } else if current_pos > start_offset {
                println!(
                    "Row group {}: Current position {} is past expected start {}. Seeking back.",
                    rg_index, current_pos, start_offset
                );
                file.seek(SeekFrom::Start(start_offset)).await?;
            }

            println!(
                "Row group {}: Reading {} bytes sequentially...",
                rg_index, available
            );
            // Read the entire row group into a buffer.
            let mut buffer = vec![0u8; available as usize];
            match file.read_exact(&mut buffer).await {
                Ok(_) => {}
                Err(e) => {
                    eprintln!(
                        "Error reading row group {} at offset {} ({} bytes): {:?}",
                        rg_index, start_offset, available, e
                    );
                    continue;
                }
            }

            // Update our computed offset to be the end of this row group.
            computed_offset = start_offset + length;

            // Create an in-memory reader from the buffer.
            let cursor = Cursor::new(buffer);
            // Build a Parquet record batch stream that only reads this row group.
            let builder =
                ParquetRecordBatchStreamBuilder::new_with_metadata(cursor, metadata.clone())
                    .with_batch_size(64_000)
                    .with_row_groups(vec![rg_index]); // restrict to this row group
            let mut stream = match builder.build() {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("Error building stream for row group {}: {:?}", rg_index, e);
                    continue;
                }
            };

            // Process all record batches from this row group.
            while let Some(result) = stream.next().await {
                match result {
                    Ok(record_batch) => {
                        // (Process the record batch as needed; here we simply discard it.)
                        let _ = record_batch;
                    }
                    Err(e) => {
                        eprintln!("Error processing row group {}: {:?}", rg_index, e);
                        break;
                    }
                }
            }
        }
    }

    let elapsed = start.elapsed();
    println!("Finished processing in {:?}", elapsed);
    Ok(())
}

/// Reads and discards exactly `to_skip` bytes from `file` using a small buffer.
async fn skip_bytes(file: &mut File, mut to_skip: u64) -> std::io::Result<()> {
    const BUF_SIZE: usize = 8192;
    let mut buf = vec![0u8; BUF_SIZE];
    while to_skip > 0 {
        let read_size = std::cmp::min(BUF_SIZE as u64, to_skip) as usize;
        let n = file.read(&mut buf[..read_size]).await?;
        if n == 0 {
            break;
        }
        to_skip -= n as u64;
    }
    Ok(())
}
