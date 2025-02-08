use arrow2::datatypes::Schema;
use arrow2::error::Result;
use arrow2::io::parquet::read::{infer_schema, FileReader};
use memmap2::MmapOptions;
use parquet2::read::read_metadata;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::time::Instant;

struct SeekableReader {
    data: memmap2::Mmap,
    pos: usize,
}

impl SeekableReader {
    fn new(file: &File) -> std::io::Result<Self> {
        Ok(Self {
            data: unsafe { MmapOptions::new().map(file)? },
            pos: 0,
        })
    }
}

impl Read for SeekableReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let available = self.data.len() - self.pos;
        let to_read = std::cmp::min(available, buf.len());
        if to_read == 0 {
            return Ok(0);
        }

        buf[..to_read].copy_from_slice(&self.data[self.pos..self.pos + to_read]);
        self.pos += to_read;
        Ok(to_read)
    }
}

impl Seek for SeekableReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(offset) => offset as usize,
            SeekFrom::Current(offset) => (self.pos as i64 + offset) as usize,
            SeekFrom::End(offset) => (self.data.len() as i64 + offset) as usize,
        };

        if new_pos > self.data.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "attempted to seek beyond end of file",
            ));
        }

        self.pos = new_pos;
        Ok(self.pos as u64)
    }
}

fn main() -> Result<()> {
    // let file_path = "./snowset-main.parquet/merged_01.parquet";
    let file_path = "/mnt/raid0/merged_01.parquet";
    println!("Reading Metadata");

    // Open the file
    let file = File::open(&file_path)?;

    // Create seekable reader
    let mut reader = SeekableReader::new(&file)?;
    println!(
        "File mapped successfully, size: {} MB",
        reader.data.len() / 1024 / 1024
    );

    // Read metadata
    let metadata = read_metadata(&mut reader)?;
    println!("Metadata read successfully");

    let batch_size = 1024 * 1024;
    let schema: Schema = infer_schema(&metadata)?;

    println!("Starting benchmark");
    let start = Instant::now();

    // Create file reader
    let reader = FileReader::new(
        reader,
        metadata.row_groups,
        schema,
        Some(batch_size),
        None,
        None,
    );

    let mut batch_count = 0;
    for maybe_batch in reader {
        maybe_batch?;
    }

    let elapsed = start.elapsed();
    println!("Time: {}s", elapsed.as_secs());
    println!("Processed {} batches total", batch_count);
    println!(
        "Average batch time: {}ms",
        (elapsed.as_millis() as f64) / 1000.
    );

    Ok(())
}
