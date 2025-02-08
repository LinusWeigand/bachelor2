use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use arrow2::io::parquet::read::{infer_schema, FileReader};
use parquet2::read::read_metadata;
use std::time::Instant;

const BUFFER_SIZE: usize = 8 * 1024 * 1024; // 8MB buffer

struct BufferedParquetReader<R: Read + Seek> {
    inner: BufReader<R>,
    buffer: Vec<u8>,
    buffer_pos: usize,
    buffer_filled: usize,
    file_pos: u64,
}

impl<R: Read + Seek> BufferedParquetReader<R> {
    fn new(reader: R) -> Self {
        Self {
            inner: BufReader::with_capacity(BUFFER_SIZE, reader),
            buffer: vec![0; BUFFER_SIZE],
            buffer_pos: 0,
            buffer_filled: 0,
            file_pos: 0,
        }
    }

    fn fill_buffer(&mut self) -> std::io::Result<usize> {
        self.buffer_pos = 0;
        self.buffer_filled = self.inner.read(&mut self.buffer)?;
        Ok(self.buffer_filled)
    }
}

impl<R: Read + Seek> Read for BufferedParquetReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // If buffer is empty or fully consumed, fill it
        if self.buffer_pos >= self.buffer_filled {
            self.fill_buffer()?;
            if self.buffer_filled == 0 {
                return Ok(0); // EOF
            }
        }

        // Copy data from our buffer to the output buffer
        let available = self.buffer_filled - self.buffer_pos;
        let to_copy = std::cmp::min(available, buf.len());
        buf[..to_copy].copy_from_slice(&self.buffer[self.buffer_pos..self.buffer_pos + to_copy]);
        
        self.buffer_pos += to_copy;
        self.file_pos += to_copy as u64;
        
        Ok(to_copy)
    }
}

impl<R: Read + Seek> Seek for BufferedParquetReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        // Calculate the new position
        let new_pos = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::Current(offset) => (self.file_pos as i64 + offset) as u64,
            SeekFrom::End(offset) => {
                let file_size = self.inner.seek(SeekFrom::End(0))?;
                self.inner.seek(SeekFrom::Start(0))?; // Reset position
                ((file_size as i64) + offset) as u64
            }
        };

        // Clear our buffer
        self.buffer_pos = 0;
        self.buffer_filled = 0;
        
        // Perform the seek on inner reader
        self.file_pos = self.inner.seek(SeekFrom::Start(new_pos))?;
        Ok(self.file_pos)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting Program");
    // let file_path = "./snowset-main.parquet/merged_01.parquet";
    let file_path = "/mnt/raid0/merged_01.parquet";
    let file = File::open(file_path)?;
    println!("File opened successfully");
    
    // Create buffered reader with large buffer
    let mut reader = BufferedParquetReader::new(file);
    println!("Reader created successfully");
    
    println!("Reading Metadata...");
    // Read metadata with debug prints
    let metadata = match read_metadata(&mut reader) {
        Ok(m) => {
            println!("Metadata read successfully");
            m
        },
        Err(e) => {
            println!("Failed to read metadata: {:?}", e);
            return Err(e.into());
        }
    };
    
    let batch_size = 1024 * 1024;
    let schema = infer_schema(&metadata)?;
    println!("Schema inferred successfully");
    
    println!("Starting benchmark");
    let start = Instant::now();
    
    let mut file_reader = FileReader::new(
        reader,
        metadata.row_groups,
        schema,
        Some(batch_size),
        None,
        None,
    );
    println!("FileReader created successfully");

    let mut batch_count = 0;
    for maybe_batch in file_reader {
        match maybe_batch {
            Ok(_) => batch_count += 1,
            Err(e) => {
                println!("Error reading batch {}: {:?}", batch_count, e);
                return Err(e.into());
            }
        }
        if batch_count % 1000 == 0 {
            println!("Processed {} batches", batch_count);
        }
    }

    println!("Processed {} batches total", batch_count);
    println!("Elapsed: {:?}", start.elapsed());
    Ok(())
}
