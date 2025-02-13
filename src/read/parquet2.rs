use arrow2::datatypes::Schema;
use arrow2::io::parquet::read::{infer_schema, read_metadata, read_metadata_async, FileReader};
use futures::stream::{StreamExt, TryStreamExt};
use parquet2::metadata::RowGroupMetaData;
use tokio::task::spawn_blocking;
use std::env;
use std::error::Error;
use std::process::exit;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::BufReader;
use tokio::time::{sleep, Instant};

use tokio_util::compat::TokioAsyncReadCompatExt;
use futures::AsyncRead;


#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

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
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();
    let args: Vec<String> = env::args().collect();
    let mut iter = args.iter().skip(1);

    let mut folder = "./merged";
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

    println!("Sleep");
    sleep(Duration::from_secs(600)).await;
    // println!("Starting Benchmark...");
    // let start_time = Instant::now();
    //
    // let mut tasks = Vec::new();
    //
    // for (path, schema, row_groups) in file_paths {
    //     let read_size = read_size;
    //     let task =
    //         tokio::task::spawn_blocking(move || -> Result<(), Box<dyn Error + Send + Sync>> {
    //             let file = std::fs::File::open(&path)?;
    //             let reader = FileReader::new(
    //                 file,
    //                 row_groups,
    //                 schema,
    //                 Some(read_size),
    //                 None,
    //                 None,
    //             );
    //
    //             for maybe_batch in reader {
    //                 let batch = maybe_batch?;
    //             }
    //
    //             Ok(())
    //         });
    //     tasks.push(task);
    // }
    //
    // for t in tasks {
    //     t.await??;
    // }
    //
    // let elapsed = start_time.elapsed();
    // let size = 0.858 * std::cmp::min(count, FILE_PATHS.len()) as f64;
    // let seconds = elapsed.as_millis() as f64 / 1000.;
    // let tp = size / seconds;
    // println!("Time: {}", seconds);
    // println!("Throughput: {}", tp);
    // //
    Ok(())
}

async fn load_files(
    file_paths: Vec<String>,
) -> Result<Vec<(String, Schema, Vec<RowGroupMetaData>)>, Box<dyn Error + Send + Sync>> {
    let concurrency = file_paths.len(); // or pick a smaller concurrency limit

    // Create a stream of tasks. Each task spawns blocking I/O for one file:
    let results = futures::stream::iter(file_paths.into_iter().map(|path| {
        tokio::spawn(async move {
            // We jump into a blocking thread.
            // Everything inside here can safely use std::fs::File, read_metadata, etc.
            let result = spawn_blocking(move || {
                // 1) Open file with std::fs
                let file = std::fs::File::open(&path)?;
                let mut reader = std::io::BufReader::new(file);

                // 2) Use blocking read_metadata
                let meta = read_metadata(&mut reader)?;
                let schema = infer_schema(&meta)?;
                let row_groups = meta.row_groups;
                Ok::<_, Box<dyn Error + Send + Sync>>((path, schema, row_groups))
            })
            .await; // Wait for the blocking task to finish

            // This `result` is a Result<Ok(...) or Err(JoinError), ...>.
            // If the spawn_blocking had an error, propagate it:
            match result {
                Ok(inner_res) => inner_res,
                Err(e) => Err(Box::new(e) as Box<dyn Error + Send + Sync>),
            }
        })
    }))
    .buffer_unordered(concurrency)
    // Flatten the JoinHandle<Result<_, _>> into a single Result:
    .then(|res| async move { 
        match res {
            Ok(task_res) => task_res, 
            Err(e) => Err(Box::new(e) as Box<dyn Error + Send + Sync>),
        }
    })
    .try_collect::<Vec<_>>()
    .await?;

    Ok(results)
}

