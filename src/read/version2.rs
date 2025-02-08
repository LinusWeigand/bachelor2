use std::{
    env,
    fs::File,
    io::{self, BufReader, Read, Seek, SeekFrom},
    process::exit,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

use arrow2::io::parquet::read::{infer_schema, read_metadata, FileReader};

const FILE_PATHS: [&str; 8] = [
    "merged_02.parquet",
    "merged_01.parquet",
    "merged_03.parquet",
    "merged_04.parquet",
    "merged_05.parquet",
    "merged_06.parquet",
    "merged_07.parquet",
    "merged_08.parquet",
];

fn main() -> std::io::Result<()> {
    // Parse command-line arguments.
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

    // Create an atomic counter to track bytes read.
    let bytes_read_counter = Arc::new(AtomicUsize::new(0));

    // Load Metadata for each file.
    let mut file_data = Vec::new();
    for file_name in FILE_PATHS.iter().take(max_counts) {
        let path = format!("{}/{}", folder, file_name);
        let file = File::open(&path)?;
        let mut buf_reader = BufReader::new(file);

        let metadata = read_metadata(&mut buf_reader).map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Metadata error in {}: {:?}", path, e),
            )
        })?;
        println!("Metadata loaded for {}", path);
        let schema = infer_schema(&metadata).map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Schema inference error in {}: {:?}", path, e),
            )
        })?;
        let row_groups = metadata.row_groups;
        file_data.push((path, row_groups, schema));
    }

    println!("Starting benchmark and printing record batches...");
    let start = Instant::now();

    for (path, row_groups, schema) in file_data {
        let file = File::open(&path)?;
        let counting_reader = CountingReader::new(file, bytes_read_counter.clone());
        let buf_reader = BufReader::new(counting_reader);
        let reader = FileReader::new(buf_reader, row_groups, schema, None, None, None);
        for maybe_batch in reader {
            let batch = maybe_batch.map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("Error reading batch from {}: {:?}", path, e),
                )
            })?;
        }
    }

    let total_bytes = bytes_read_counter.load(Ordering::Relaxed);
    let elapsed = start.elapsed();
    let total_mb = total_bytes as f64 / (1024.0 * 1024.0);
    let mib_s = total_mb / elapsed.as_secs_f64();
    let gb = total_bytes as f64 / (1024.0 * 1024.0 * 1024.0);

    println!("Read {:.3} GB in {:?} ({:.2} MiB/s)", gb, elapsed, mib_s);

    Ok(())
}

// A wrapper that counts the number of bytes read.
struct CountingReader<R> {
    inner: R,
    counter: Arc<AtomicUsize>,
}

impl<R> CountingReader<R> {
    fn new(inner: R, counter: Arc<AtomicUsize>) -> Self {
        Self { inner, counter }
    }
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.counter.fetch_add(n, Ordering::Relaxed);
        Ok(n)
    }
}

impl<R: Seek> Seek for CountingReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }
}
