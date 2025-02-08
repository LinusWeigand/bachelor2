use std::env;
use std::process::exit;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::time::Instant;

const FILE_PATHS: [&str; 8] = [
    "merged_01.parquet",
    "merged_02.parquet",
    "merged_03.parquet",
    "merged_04.parquet",
    "merged_05.parquet",
    "merged_06.parquet",
    "merged_07.parquet",
    "merged_08.parquet",
    // "merged_09.parquet",
    // "merged_10.parquet",
    // "merged_11.parquet",
    // "merged_12.parquet",
    // "merged_13.parquet",
    // "merged_14.parquet",
    // "merged_15.parquet",
    // "merged_16.parquet",
];
#[tokio::main]
async fn main() -> std::io::Result<()> {
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
    let file_paths: Vec<String> = FILE_PATHS
        .iter()
        .map(|b| {
            let file_path = format!("{}/{}", folder, b);
            file_path
        })
        .collect();

    let total_bytes_read = Arc::new(AtomicU64::new(0));
    println!("Starting Benchmark...");
    let start_time = Instant::now();

    let mut tasks = Vec::new();
    for path in file_paths {
        let total_bytes_read = total_bytes_read.clone();
        let task = tokio::spawn(async move {
            let mut file = File::open(&path).await?;
            let mut buffer = vec![0u8; read_size];
            let mut local_bytes = 0u64;

            loop {
                let n = file.read(&mut buffer).await?;
                if n == 0 {
                    break;
                }
                local_bytes += n as u64;
            }

            total_bytes_read.fetch_add(local_bytes, Ordering::Relaxed);
            Ok::<(), std::io::Error>(())
        });
        tasks.push(task);
    }

    for t in tasks {
        t.await??;
    }

    let elapsed = start_time.elapsed();
    let total_bytes = total_bytes_read.load(Ordering::Relaxed);
    let mib_per_sec = (total_bytes as f64 / 1024.0 / 1024.0) / elapsed.as_secs_f64();
    let gb = total_bytes as f64 / 1024. / 1024.;

    println!("Read {} total bytes in {:?}", total_bytes, elapsed);
    println!("Overall throughput: {:.2} MiB/s", mib_per_sec);

    Ok(())
}
