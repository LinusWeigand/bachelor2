use arrow::array::RecordBatch;
use futures::StreamExt;
use parquet::arrow::{
    arrow_reader::ArrowReaderMetadata, async_reader::ParquetRecordBatchStream,
    ParquetRecordBatchStreamBuilder,
};
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::{
    fs::File,
    time::Instant,
};


#[tokio::main]
async fn main() -> std::io::Result<()> {
    let file_path = "./snowset-main.parquet/merged_01.parquet";
    

    println!("Reading Metadata...");
    let mut file = File::open(&file_path).await?;
    let metadata = ArrowReaderMetadata::load_async(&mut file, Default::default()).await?;
    println!(
        "Loaded {} with size: {} MB",
        &file_path,
        metadata.metadata().memory_size() as f64 / 1024. / 1024.
    );
    println!("Starting Benchmark...");

    let bytes_read = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();

    let file = File::open(&file_path).await?;

    let mut builder =
        ParquetRecordBatchStreamBuilder::new_with_metadata(file, metadata);
    builder = builder.with_batch_size(64_000);
    let mut stream = builder.build()?;
    let mut pinned_stream = Pin::new(&mut stream);

    let mut record_batch = get_next_item_from_reader_and_count_bytes(&mut pinned_stream)
        .await
        .unwrap();
    while let Some(batch) = get_next_item_from_reader_and_count_bytes(&mut pinned_stream).await
    {
        record_batch = batch;
    }

    let bytes_read = bytes_read.load(Ordering::Relaxed);
    let elapsed = start.elapsed();
    let total_mb = bytes_read as f64 / (1024.0 * 1024.0);
    let mib_s = total_mb / elapsed.as_secs_f64();
    let gb = bytes_read as f64 / 1024.0 / 1024.0 / 1024.0;

    println!("Read {:.3} GB in {:?} ({:.2} MiB/s)", gb, elapsed, mib_s);

    Ok(())
}

async fn get_next_item_from_reader_and_count_bytes(
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
