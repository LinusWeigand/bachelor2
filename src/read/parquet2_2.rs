use arrow2::io::parquet::read::{infer_schema, FileReader};
use parquet2::read::deserialize_metadata;
use tokio::time::Instant;
use std::env;
use std::error::Error;
use std::io::{Read, Seek, SeekFrom};
use std::process::exit;

use futures::stream::{StreamExt, TryStreamExt};
use tokio::task::spawn_blocking;

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

#[derive(Debug)]
struct RawFooter {
    path: String,
    footer_size: usize,
    raw_bytes: Vec<u8>,
}

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

    println!("Loading metadata...");
    let raw_footers = load_files(file_paths).await?;

    println!("Starting Benchmark...");
    let start_time = Instant::now();

    let mut tasks = Vec::new();

    for raw_footer in raw_footers {
        let read_size = read_size;
        let task =
            tokio::task::spawn_blocking(move || -> Result<(), Box<dyn Error + Send + Sync>> {
                let parsed_footer = parse_raw_footer(&raw_footer.raw_bytes, raw_footer.footer_size)?;
                let file = std::fs::File::open(&raw_footer.path)?;
                let schema = infer_schema(&parsed_footer)?;
                let row_groups = parsed_footer.row_groups;
                let reader = FileReader::new(
                    file,
                    row_groups,
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
    let size = 0.858 * std::cmp::min(count, FILE_PATHS.len()) as f64;
    let seconds = elapsed.as_millis() as f64 / 1000.;
    let tp = size / seconds;
    println!("Time: {:.2}s", seconds);
    println!("Throughput: {:02}MB", tp);

    Ok(())
}

async fn load_files(
    file_paths: Vec<String>,
) -> Result<Vec<RawFooter>, Box<dyn Error + Send + Sync>> {
    let concurrency = file_paths.len();

    let results = futures::stream::iter(file_paths.into_iter().map(|path| {
        tokio::spawn(async move {
            let result = spawn_blocking(move || {
                let mut file = std::fs::File::open(&path)?;

                let file_size = file.seek(SeekFrom::End(0))?;
                if file_size < 12 {
                    return Err(format!("File too small to be valid parquet: {}", path).into());
                }

                file.seek(SeekFrom::End(-8))?;
                let mut trailer = [0u8; 8];
                file.read_exact(&mut trailer)?;

                let magic = &trailer[4..];
                if magic != b"PAR1" {
                    return Err(format!("Invalid Parquet file magic in {}", path).into());
                }

                let metadata_len = u32::from_le_bytes(trailer[0..4].try_into().unwrap());
                let metadata_len = metadata_len as usize;

                let footer_start = file_size
                    .checked_sub(8 + metadata_len as u64)
                    .ok_or_else(|| format!("metadata_len too large in {}", path))?;
                file.seek(SeekFrom::Start(footer_start))?;

                let mut raw_bytes = vec![0u8; metadata_len + 8];
                file.read_exact(&mut raw_bytes)?;

                Ok(RawFooter {
                    path,
                    footer_size: raw_bytes.len(),
                    raw_bytes,
                })
            })
            .await;

            match result {
                Ok(inner_res) => inner_res,
                Err(e) => Err(Box::new(e) as Box<dyn Error + Send + Sync>),
            }
        })
    }))
    .buffer_unordered(concurrency)
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

fn parse_raw_footer(raw_bytes: &[u8], max_size: usize) -> Result<parquet2::metadata::FileMetaData, Box<dyn Error + Send + Sync>> {
    if raw_bytes.len() < 8 || &raw_bytes[raw_bytes.len() - 4..] != b"PAR1" {
        return Err("Not a valid parquet footer".into());
    }

    let slice_without_magic = &raw_bytes[..raw_bytes.len() - 4];
    let file_meta = deserialize_metadata(slice_without_magic, max_size)?;
    Ok(file_meta)
}
