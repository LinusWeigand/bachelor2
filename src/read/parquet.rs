use std::env;
use std::error::Error;
use std::pin::Pin;
use std::process::exit;
use arrow::array::RecordBatch;
use futures::stream::{StreamExt, TryStreamExt};
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use tokio::fs::File;
use tokio::time::Instant;

const FILE_PATHS: [&str; 16] = [
    "merged_01.parquet",
    "merged_02.parquet",
    "merged_03.parquet",
    "merged_04.parquet",
    "merged_05.parquet",
    "merged_06.parquet",
    "merged_07.parquet",
    "merged_08.parquet",
    "merged_09.parquet",
    "merged_10.parquet",
    "merged_11.parquet",
    "merged_12.parquet",
    "merged_13.parquet",
    "merged_14.parquet",
    "merged_15.parquet",
    "merged_16.parquet",
];
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>>{
    let args: Vec<String> = env::args().collect();
    let mut iter = args.iter().skip(1);

    let mut folder = "./snowset-main.parquet";
    let mut read_size: usize = 4 * 1024 * 1024;
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-p" | "--path" => {
                if let Some(v) = iter.next() {
                    folder = v;
                } else {
                    eprintln!("Error: -p/--path requires an argument.");
                    exit(1);
                }
            }
            "-s" | "--size" => {
                if let Some(v) = iter.next() {
                    read_size = v.parse().unwrap();
                    read_size *= 1024 * 1024;
                } else {
                    eprintln!("Error: -s/--size requires an argument.");
                    exit(1);
                }
            }
            _ => {
                eprintln!("Unknown argument: {}", arg);
                exit(1);
            }
        }
    }
    let file_paths = load_files(folder).await?;

    println!("Starting Benchmark...");
    let start_time = Instant::now();

    let mut tasks = Vec::new();
    for (path, metadata) in file_paths {
        let task = tokio::spawn(async move {
            let file = File::open(&path).await?;

            let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(file, metadata);
            builder = builder.with_batch_size(read_size);
            let mut stream = builder.build()?;
            let mut pinned_stream = Pin::new(&mut stream);

            let mut record_batch = get_next_item_from_reader(&mut pinned_stream)
                .await
                .unwrap();

            while let Some(batch) = get_next_item_from_reader(&mut pinned_stream).await
            {
                record_batch = batch;
            }
            

            Ok::<(), std::io::Error>(())
        });
        tasks.push(task);
    }

    for t in tasks {
        t.await??;
    }

    let elapsed = start_time.elapsed();
    let size = 0.858 * FILE_PATHS.len() as f64;
    let seconds = elapsed.as_millis() as f64 / 1000.;
    let tp = size / seconds;
    println!("Time: {}", seconds);
    println!("Throughput: {}", tp);


    Ok(())
}

async fn get_next_item_from_reader(
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


async fn load_files(folder: &str) -> Result<Vec<(String, ArrowReaderMetadata)>, Box<dyn std::error::Error>> {
    let file_paths = futures::stream::iter(FILE_PATHS.iter().map(|b| {
        let folder = folder.to_string();
        async move {
            let file_path = format!("{}/{}", folder, b);
            let mut file = File::open(&file_path).await?;
            let metadata = ArrowReaderMetadata::load_async(&mut file, Default::default()).await?;
            Ok::<_, Box<dyn std::error::Error>>((file_path, metadata))
        }
    }))
    .buffer_unordered(10)
    .try_collect::<Vec<_>>()
    .await?;

    Ok(file_paths)
}
