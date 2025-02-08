use futures::StreamExt;
use memmap2::Mmap;
use parquet::arrow::{arrow_reader::ArrowReaderMetadata, ParquetRecordBatchStreamBuilder};
use std::{
    env,
    io::SeekFrom,
    pin::Pin,
    process::exit,
    sync::Arc,
    task::{Context, Poll},
    time::Instant,
};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf};

/// An async reader that wraps an Arc<Mmap> and a position index.
struct MemMapReader {
    mmap: Arc<Mmap>,
    pos: u64,
}

impl MemMapReader {
    fn new(mmap: Arc<Mmap>) -> Self {
        Self { mmap, pos: 0 }
    }
}

impl AsyncRead for MemMapReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let total_len = self.mmap.len() as u64;
        if self.pos >= total_len {
            return Poll::Ready(Ok(()));
        }
        let available = total_len - self.pos;
        let to_read = std::cmp::min(available, buf.remaining() as u64) as usize;
        let start = self.pos as usize;
        let end = start + to_read;
        buf.put_slice(&self.mmap[start..end]);
        self.pos += to_read as u64;
        Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for MemMapReader {
    fn start_seek(mut self: Pin<&mut Self>, pos: SeekFrom) -> std::io::Result<()> {
        let new_pos = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::End(offset) => {
                let end = self.mmap.len() as i64;
                (end + offset) as u64
            }
            SeekFrom::Current(offset) => (self.pos as i64 + offset) as u64,
        };
        self.pos = new_pos;
        Ok(())
    }
    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        Poll::Ready(Ok(self.pos))
    }
}

impl Unpin for MemMapReader {}
unsafe impl Send for MemMapReader {}

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
    // let mut folder = "./snowset-main.parquet".to_string();
    let mut folder = "/mnt/raid0/main.parquet".to_string();
    // let mut folder = "./snowset-main.parquet".to_string();
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
    // We open the file synchronously and use it to load metadata.
    let mut file_data: Vec<(String, ArrowReaderMetadata, Arc<Mmap>)> = Vec::new();
    println!("Reading Metadata...");
    for file_name in FILE_PATHS.iter().take(max_counts) {
        let path = format!("{}/{}", folder, file_name);
        // Open the file synchronously.
        let std_file = std::fs::File::open(&path)?;
        // Load the full metadata using the official API.
        let metadata = ArrowReaderMetadata::load(&std_file, Default::default())?;
        println!(
            "Loaded {} with size: {:.2} MB",
            &path,
            metadata.metadata().memory_size() as f64 / (1024.0 * 1024.0)
        );
        // Memory-map the file.
        let mmap = unsafe { Mmap::map(&std_file)? };
        file_data.push((path, metadata, Arc::new(mmap)));
    }

    println!("Starting Benchmark...");
    let start = Instant::now();

    // Process each file.
    for (file_path, metadata, mmap) in file_data {
        println!("Processing file: {}", file_path);
        // Create our async reader from the memory-mapped file.
        let reader = MemMapReader::new(mmap);
        // Build the Parquet record batch stream.
        let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(reader, metadata)
            .with_batch_size(64_000);
        let mut stream = builder.build()?;
        let mut pinned_stream = Pin::new(&mut stream);
        while let Some(result) = pinned_stream.as_mut().next().await {
            match result {
                Ok(record_batch) => {
                    // Process the record batch as needed.
                    let _ = record_batch;
                }
                Err(e) => {
                    eprintln!("Error: {:?}", e);
                    break;
                }
            }
        }
    }

    let elapsed = start.elapsed();
    println!("Finished processing in {:?}", elapsed);
    Ok(())
}
