use arrow::array::RecordBatch;
use futures::StreamExt;
use parquet::arrow::{
    arrow_reader::ArrowReaderMetadata, async_reader::ParquetRecordBatchStream,
    ParquetRecordBatchStreamBuilder,
};
use std::process::exit;
use std::{
    env,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncSeek, ReadBuf},
    time::Instant,
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
    let mut folder = "./snowset-main.parquet".to_string();
    let args: Vec<String> = env::args().collect();
    let mut iter = args.iter().skip(1);
    let mut max_counts: usize = 8;
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

    let mut file_data = Vec::new();
    println!("Reading Metadata...");
    let mut counter = 0;
    for file_name in FILE_PATHS {
        counter += 1;
        if counter > max_counts {
            break;
        }
        let path = format!("{}/{}", &folder, file_name);
        let mut file = File::open(&path).await?;
        let metadata = ArrowReaderMetadata::load_async(&mut file, Default::default()).await?;
        println!(
            "Loaded {} with size: {} MB",
            &path,
            metadata.metadata().memory_size() as f64 / 1024. / 1024.
        );

        file_data.push((path, metadata));
    }

    println!("Starting Benchmark...");

    let bytes_read = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();

    for (file_path, metadata) in file_data {
        let file = File::open(&file_path).await?;

        // let counting_file = CountingReader::new(file, bytes_read.clone());
        let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(file, metadata);
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
    }

    let bytes_read = bytes_read.load(Ordering::Relaxed);
    let elapsed = start.elapsed();
    let total_mb = bytes_read as f64 / (1024.0 * 1024.0);
    let mib_s = total_mb / elapsed.as_secs_f64();
    let gb = bytes_read as f64 / 1024.0 / 1024.0 / 1024.0;

    println!("Total Time: {}", elapsed.as_millis() as f64 / 1000.);

    // println!("Read {:.3} GB in {:?} ({:.2} MiB/s)", gb, elapsed, mib_s);

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

// struct CountingReader<R> {
//     inner: R,
//     bytes_read: Arc<AtomicUsize>,
// }
//
// impl<R> CountingReader<R> {
//     pub fn new(inner: R, bytes_read: Arc<AtomicUsize>) -> Self {
//         Self { inner, bytes_read }
//     }
// }
//
// impl<R: AsyncRead + Unpin> AsyncRead for CountingReader<R> {
//     fn poll_read(
//         mut self: Pin<&mut Self>,
//         cx: &mut Context<'_>,
//         buf: &mut ReadBuf<'_>,
//     ) -> Poll<std::io::Result<()>> {
//         let start_len = buf.filled().len();
//         let inner = Pin::new(&mut self.inner);
//
//         match inner.poll_read(cx, buf) {
//             Poll::Ready(Ok(())) => {
//                 let new_len = buf.filled().len();
//                 let n = new_len - start_len;
//
//                 self.bytes_read.fetch_add(n, Ordering::Relaxed);
//
//                 Poll::Ready(Ok(()))
//             }
//             other => other,
//         }
//     }
// }
//
// impl<R: AsyncSeek + Unpin> AsyncSeek for CountingReader<R> {
//     fn start_seek(mut self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
//         Pin::new(&mut self.inner).start_seek(position)
//     }
//
//     fn poll_complete(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
//         Pin::new(&mut self.inner).poll_complete(cx)
//     }
// }
