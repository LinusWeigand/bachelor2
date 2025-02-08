use arrow2::datatypes::Schema;
use arrow2::error::Result;
use arrow2::io::parquet::read::{infer_schema, FileReader};
use parquet2::read::read_metadata;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::time::Instant;

// For HDDs in RAID 0, we want very large buffers to minimize seeks
// With 8 drives, we'll use 32MB buffer (4MB per drive)
const BUFFER_SIZE: usize = 32 * 1024 * 1024;

struct DirectReader {
    file: File,
    buffer: Vec<u8>,
    buffer_pos: usize,
    buffer_filled: usize,
    file_pos: u64,
}

impl DirectReader {
    fn new(path: &str) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new().read(true).open(path)?;

        // Pre-allocate large buffer
        let buffer = vec![0; BUFFER_SIZE];

        Ok(Self {
            file,
            buffer,
            buffer_pos: 0,
            buffer_filled: 0,
            file_pos: 0,
        })
    }

    fn fill_buffer(&mut self) -> std::io::Result<usize> {
        self.buffer_pos = 0;
        // Try to fill entire buffer at once to maximize sequential reads
        let mut filled = 0;
        while filled < self.buffer.len() {
            match self.file.read(&mut self.buffer[filled..]) {
                Ok(0) => break, // EOF
                Ok(n) => filled += n,
                Err(e) => return Err(e),
            }
        }
        self.buffer_filled = filled;
        Ok(filled)
    }
}

impl Read for DirectReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.buffer_pos >= self.buffer_filled {
            self.fill_buffer()?;
            if self.buffer_filled == 0 {
                return Ok(0);
            }
        }

        let available = self.buffer_filled - self.buffer_pos;
        let to_copy = std::cmp::min(available, buf.len());

        buf[..to_copy].copy_from_slice(&self.buffer[self.buffer_pos..self.buffer_pos + to_copy]);
        self.buffer_pos += to_copy;
        self.file_pos += to_copy as u64;

        Ok(to_copy)
    }
}

impl Seek for DirectReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::Current(offset) => (self.file_pos as i64 + offset) as u64,
            SeekFrom::End(offset) => {
                let end = self.file.seek(SeekFrom::End(0))?;
                ((end as i64) + offset) as u64
            }
        };

        self.buffer_pos = 0;
        self.buffer_filled = 0;
        self.file_pos = self.file.seek(SeekFrom::Start(new_pos))?;
        Ok(self.file_pos)
    }
}

fn main() -> Result<()> {
    let file_path = "/mnt/raid0/merged_01.parquet";
    println!("Reading Metadata");

    let mut reader = DirectReader::new(file_path)?;

    let metadata = read_metadata(&mut reader)?;
    println!("Metadata read successfully");
    println!("Number of row groups: {}", metadata.row_groups.len());

    // For HDDs, use larger batch size to minimize seeking
    let batch_size = 8 * 1024 * 1024; // 8MB batches
    let schema: Schema = infer_schema(&metadata)?;

    println!("Starting benchmark");
    let start = Instant::now();

    let reader = FileReader::new(
        reader,
        metadata.row_groups,
        schema,
        Some(batch_size),
        None,
        None,
    );

    let mut batch_count = 0;
    let mut last_report = Instant::now();

    for maybe_batch in reader {
        maybe_batch?;
        batch_count += 1;

        // Report progress every 5 seconds
        if last_report.elapsed().as_secs() >= 5 {
            println!("Processed {} batches", batch_count);
            last_report = Instant::now();
        }
    }

    let elapsed = start.elapsed();
    println!("Time: {}s", elapsed.as_secs());
    println!("Processed {} batches total", batch_count);
    println!(
        "Average batch time: {}ms",
        (elapsed.as_millis() as f64) / (batch_count as f64)
    );

    Ok(())
}

