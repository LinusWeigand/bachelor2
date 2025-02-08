use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::error::Result;
use arrow2::io::parquet::read::{infer_schema, FileReader, RowGroupMetaData};
use arrow2::array::Array;
use parquet2::read::read_metadata;

/// A reader that coalesces multiple small reads into larger sequential reads
pub struct CoalescingReader<R: Read + Seek> {
    inner: R,
    buffer: Vec<u8>,
    buffer_start: u64,
    buffer_size: usize,
    pending_reads: BTreeMap<u64, Vec<PendingRead>>,
    min_coalesce_size: usize,
}

struct PendingRead {
    offset: u64,
    len: usize,
    callback: Box<dyn FnOnce(&[u8]) -> io::Result<()> + Send>,
}

impl<R: Read + Seek> CoalescingReader<R> {
    pub fn new(inner: R, buffer_size: usize, min_coalesce_size: usize) -> Self {
        Self {
            inner,
            buffer: Vec::with_capacity(buffer_size),
            buffer_start: 0,
            buffer_size,
            pending_reads: BTreeMap::new(),
            min_coalesce_size,
        }
    }

    /// Queue a read request to be coalesced with others
    pub fn queue_read<F>(&mut self, offset: u64, len: usize, callback: F) -> io::Result<()>
    where
        F: FnOnce(&[u8]) -> io::Result<()> + Send + 'static,
    {
        let bucket = offset / self.buffer_size as u64;
        self.pending_reads
            .entry(bucket)
            .or_default()
            .push(PendingRead {
                offset,
                len,
                callback: Box::new(callback),
            });

        // If we have enough pending reads in this bucket, process them
        if let Some(reads) = self.pending_reads.get(&bucket) {
            if reads.len() >= self.min_coalesce_size {
                self.process_bucket(bucket)?;
            }
        }

        Ok(())
    }

    /// Process all pending reads in a given bucket
    fn process_bucket(&mut self, bucket: u64) -> io::Result<()> {
        if let Some(reads) = self.pending_reads.remove(&bucket) {
            if reads.is_empty() {
                return Ok(());
            }

            // Find the range we need to read
            let start_offset = reads.iter().map(|r| r.offset).min().unwrap();
            let end_offset = reads
                .iter()
                .map(|r| r.offset + r.len as u64)
                .max()
                .unwrap();

            // Read the entire range
            self.inner.seek(SeekFrom::Start(start_offset))?;
            let len = (end_offset - start_offset) as usize;
            self.buffer.resize(len, 0);
            self.inner.read_exact(&mut self.buffer)?;
            self.buffer_start = start_offset;

            // Process each read request
            for read in reads {
                let offset = (read.offset - start_offset) as usize;
                let data = &self.buffer[offset..offset + read.len];
                (read.callback)(data)?;
            }
        }

        Ok(())
    }

    /// Flush all pending reads
    pub fn flush(&mut self) -> io::Result<()> {
        let buckets: Vec<_> = self.pending_reads.keys().copied().collect();
        for bucket in buckets {
            self.process_bucket(bucket)?;
        }
        Ok(())
    }
}

// Custom read implementation that uses our coalescing reader
struct SharedReader {
    reader: Arc<Mutex<CoalescingReader<File>>>,
    position: u64,
}

impl SharedReader {
    fn new(reader: Arc<Mutex<CoalescingReader<File>>>) -> Self {
        Self {
            reader,
            position: 0,
        }
    }
}

impl Read for SharedReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = buf.len();
        let mut data = Vec::with_capacity(len);
        data.resize(len, 0);
        
        let (tx, rx) = std::sync::mpsc::channel();
        
        self.reader.lock().unwrap().queue_read(
            self.position,
            len,
            move |bytes| {
                data.copy_from_slice(bytes);
                tx.send(data).map_err(|_| {
                    io::Error::new(io::ErrorKind::Other, "Channel send failed")
                })?;
                Ok(())
            },
        )?;
        
        let data = rx.recv().map_err(|_| {
            io::Error::new(io::ErrorKind::Other, "Channel receive failed")
        })?;
        
        buf.copy_from_slice(&data);
        self.position += len as u64;
        Ok(len)
    }
}

impl Seek for SharedReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.position = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::Current(offset) => (self.position as i64 + offset) as u64,
            SeekFrom::End(_) => unimplemented!("Seek from end not implemented"),
        };
        Ok(self.position)
    }
}

pub struct BufferedFileReader {
    reader: Arc<Mutex<CoalescingReader<File>>>,
    inner_reader: FileReader<SharedReader>,
}
impl BufferedFileReader {
    pub fn new(
        file: File,
        row_groups: Vec<RowGroupMetaData>,
        schema: Schema,
        batch_size: Option<usize>,
        projection: Option<usize>,
    ) -> Self {
        let reader = Arc::new(Mutex::new(
            CoalescingReader::new(file, 1024 * 1024, 10) // 1MB buffer, coalesce after 10 reads
        ));
        
        let shared_reader = SharedReader::new(reader.clone());
        let inner_reader = FileReader::new(
            shared_reader,
            row_groups,
            schema,
            batch_size,
            projection,
            None::<Vec<Vec<_>>>, // Using a generic Vec<Vec<_>> for filters
        );
        
        Self {
            reader,
            inner_reader,
        }
    }
}

impl Iterator for BufferedFileReader {
    type Item = Result<Chunk<Box<dyn Array>>>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.inner_reader.next();
        if result.is_none() {
            // Flush any remaining reads when we're done
            if let Err(e) = self.reader.lock().unwrap().flush() {
                return Some(Err(e.into()));
            }
        }
        result
    }
}

fn main() -> Result<()> {
    let file_path = "/mnt/raid0/merged_01.parquet";
    // let file_path = "./snowset-main.parquet/merged_01.parquet";
    println!("Reading Metadata");
    
    let file = File::open(&file_path)?;
    let mut buf_reader = std::io::BufReader::new(file);
    let metadata = read_metadata(&mut buf_reader)?;
    
    let file = File::open(&file_path)?;
    let batch_size = 1024 * 1024;
    let schema: Schema = infer_schema(&metadata)?;
    
    println!("Starting benchmark");
    let start = std::time::Instant::now();
    
    let reader = BufferedFileReader::new(
        file,
        metadata.row_groups,
        schema,
        Some(batch_size),
        None,
    );
    
    for maybe_batch in reader {
        let _batch = maybe_batch?;
    }
    
    let elapsed = start.elapsed();
    println!("Time: {}", elapsed.as_secs());
    
    Ok(())
}
