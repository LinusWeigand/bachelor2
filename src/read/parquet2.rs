use arrow::array::RecordBatch;
use arrow2::datatypes::Schema;
use arrow2::io::parquet::read::{infer_schema, read_metadata_async, FileReader};
use futures::stream::{StreamExt, TryStreamExt};
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use std::env;
use std::error::Error;
use std::pin::Pin;
use std::process::exit;
use tokio::fs::File;
use tokio::io::BufReader;
use tokio::time::Instant;
use tokio_util::compat::TokioAsyncReadCompatExt;

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
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args: Vec<String> = env::args().collect();
    let mut iter = args.iter().skip(1);

    let mut folder = "./snowset-main.parquet";
    let mut read_size: usize = 4 * 1024 * 1024;
    let mut count: usize = 16;
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
            "-c" | "--count" => {
                if let Some(v) = iter.next() {
                    count = v.parse().unwrap();
                } else {
                    eprintln!("Error: -c/--count requires an argument.");
                    exit(1);
                }
            }
            _ => {
                eprintln!("Unknown argument: {}", arg);
                exit(1);
            }
        }
    }
    count += 1;
    let file_paths: Vec<_> = (1..count)
        .map(|i| format!("{}/merged_{:02}.parquet", folder, i))
        .collect();

    let file_paths = load_files(file_paths).await?;

    println!("Starting Benchmark...");
    let start_time = Instant::now();

    let mut tasks = Vec::new();

    for (path, metadata) in file_paths {
        let read_size = read_size;
        let task =
            tokio::task::spawn_blocking(move || -> Result<(), Box<dyn Error + Send + Sync>> {
                let schema: Schema = infer_schema(&metadata)?;
                let file = std::fs::File::open(&path)?;
                let reader = FileReader::new(
                    file,
                    metadata.row_groups,
                    schema,
                    Some(read_size),
                    None,
                    None,
                );

                for maybe_batch in reader {
                    let batch = maybe_batch?;
                }

                Ok(())
            });
        tasks.push(task);
    }

    for t in tasks {
        t.await??;
    }

    let elapsed = start_time.elapsed();
    let size = 0.864 * count as f64;
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

async fn load_files(
    file_paths: Vec<String>,
) -> Result<Vec<(String, arrow2::io::parquet::read::FileMetaData)>, Box<dyn Error + Send + Sync>> {
    let result = futures::stream::iter(file_paths.into_iter().map(|path| async move {
        let file = File::open(&path).await?;
        let mut buf_reader = BufReader::new(file).compat();
        let metadata = read_metadata_async(&mut buf_reader).await?;
        Ok::<_, Box<dyn Error + Send + Sync>>((path, metadata))
    }))
    .buffer_unordered(10)
    .try_collect::<Vec<_>>()
    .await?;

    Ok(result)
}
