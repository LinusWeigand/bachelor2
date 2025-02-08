
use arrow::array::RecordBatch;
use futures::StreamExt;
use parquet::arrow::{
    arrow_reader::ArrowReaderMetadata, async_reader::ParquetRecordBatchStream,
    ParquetRecordBatchStreamBuilder,
};
use std::{
    io,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    fs::File,
    io::{
        AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, ReadBuf, SeekFrom,
    },
    sync::{Mutex, oneshot},
    time::Instant,
};
use bytes::Bytes;

// -----------------------------------------------------------------------------
// 1. Request Descriptor
// -----------------------------------------------------------------------------

/// Each small read request contains an offset, length, and a oneshot sender
/// to later deliver the corresponding slice from the large read.
struct Request {
    offset: u64,
    length: usize,
    responder: oneshot::Sender<Result<Bytes, io::Error>>,
}

// -----------------------------------------------------------------------------
// 2. The CoalescingReader
// -----------------------------------------------------------------------------

/// This reader wraps an underlying file. It never touches the file until a flush
/// is triggered—i.e. until 100ms have passed with no new read/seek requests.
/// All incoming read requests are stored in the `pending` queue, and the
/// logical file cursor is maintained independently.
#[derive(Clone)]
struct CoalescingReader {
    /// The underlying file (used only when a large read is finally issued)
    inner: Arc<Mutex<File>>,
    /// Pending small read requests.
    pending: Arc<Mutex<Vec<Request>>>,
    /// How long to wait (i.e. “quiet period”) before flushing the queue.
    flush_delay: Duration,
    /// A flag so that only one flush is scheduled at a time.
    flush_scheduled: Arc<Mutex<bool>>,
    /// Logical file cursor tracking the “current position” (updated via poll_seek and poll_read)
    cursor: Arc<Mutex<u64>>,
}

impl CoalescingReader {
    /// Create a new CoalescingReader that wraps the given file.
    async fn new(file: File) -> Self {
        CoalescingReader {
            inner: Arc::new(Mutex::new(file)),
            pending: Arc::new(Mutex::new(Vec::new())),
            flush_delay: Duration::from_millis(100),
            flush_scheduled: Arc::new(Mutex::new(false)),
            cursor: Arc::new(Mutex::new(0)),
        }
    }

    /// Instead of immediately reading from the file, this method simply queues a
    /// request for the range `[offset, offset+length)` and schedules a flush (if not
    /// already scheduled) to occur after a quiet period.
    async fn read_range(&self, offset: u64, length: usize) -> Result<Bytes, io::Error> {
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending.lock().await;
            pending.push(Request { offset, length, responder: tx });
        }
        {
            let mut scheduled = self.flush_scheduled.lock().await;
            if !*scheduled {
                *scheduled = true;
                let this = self.clone();
                // Wait for 100ms without new requests before flushing.
                tokio::spawn(async move {
                    tokio::time::sleep(this.flush_delay).await;
                    this.flush().await;
                });
            }
        }
        rx.await.map_err(|_| {
            io::Error::new(
                io::ErrorKind::Other,
                "Internal error: flush task cancelled",
            )
        })?
    }

    /// Flushes the pending request queue. It calculates the merged (largest) byte range
    /// that covers all queued requests, then (only at this point) seeks and reads from
    /// the underlying file. Once the large read returns its data (loaded into memory),
    /// the method slices the buffer and completes each request.
    async fn flush(&self) {
        // Drain all pending requests.
        let requests = {
            let mut pending = self.pending.lock().await;
            pending.split_off(0)
        };
        {
            let mut scheduled = self.flush_scheduled.lock().await;
            *scheduled = false;
        }
        if requests.is_empty() {
            return;
        }
        // Compute the merged range.
        let min_offset = requests.iter().map(|r| r.offset).min().unwrap();
        let max_end = requests
            .iter()
            .map(|r| r.offset + r.length as u64)
            .max()
            .unwrap();
        let merged_length = (max_end - min_offset) as usize;

        // *** Now we finally touch the file ***
        let mut buffer = vec![0u8; merged_length];
        let mut file = self.inner.lock().await;
        if let Err(e) = file.seek(SeekFrom::Start(min_offset)).await {
            for req in requests {
                let _ = req.responder.send(Err(e.kind().into()));
            }
            return;
        }
        if let Err(e) = file.read_exact(&mut buffer).await {
            for req in requests {
                let _ = req.responder.send(Err(e));
            }
            return;
        }
        drop(file);
        // Slice the large buffer for each queued request.
        for req in requests {
            let start = (req.offset - min_offset) as usize;
            let end = start + req.length;
            let slice = buffer[start..end].to_vec();
            let _ = req.responder.send(Ok(Bytes::from(slice)));
        }
    }
}

// -----------------------------------------------------------------------------
// 3. Implementing AsyncSeek and AsyncRead for the CoalescingReader
// -----------------------------------------------------------------------------

/// We implement AsyncSeek so that any seek call simply updates the logical cursor
/// rather than touching the underlying file.
impl AsyncSeek for CoalescingReader {
    fn poll_seek(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        let this = self.get_mut();
        // Update the logical cursor.
        let mut cursor = futures::executor::block_on(this.cursor.lock());
        let current = *cursor;
        let new_cursor = match pos {
            SeekFrom::Start(n) => n,
            SeekFrom::Current(n) => {
                if n < 0 {
                    current.checked_sub(n.abs() as u64).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "negative seek out of bounds")
                    })?
                } else {
                    current + n as u64
                }
            }
            SeekFrom::End(_n) => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "SeekFrom::End not supported",
                )));
            }
        };
        *cursor = new_cursor;
        Poll::Ready(Ok(new_cursor))
    }
}

/// In our AsyncRead implementation, each call to `poll_read` issues a new request
/// for a range of bytes from the logical cursor. (Note that the file is not touched
/// until flush happens after 100ms.)
impl AsyncRead for CoalescingReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        // Capture the current logical offset and then update it.
        let offset = {
            let mut cursor = futures::executor::block_on(this.cursor.lock());
            let o = *cursor;
            *cursor += buf.remaining() as u64;
            o
        };
        let desired_len = buf.remaining();
        // Issue a read request that goes into the pending queue.
        let read_future = this.read_range(offset, desired_len);
        let bytes = match futures::executor::block_on(read_future) {
            Ok(b) => b,
            Err(e) => return Poll::Ready(Err(e)),
        };
        buf.put_slice(&bytes);
        Poll::Ready(Ok(()))
    }
}

// -----------------------------------------------------------------------------
// 4. Integration in main()
// -----------------------------------------------------------------------------

#[tokio::main]
async fn main() -> io::Result<()> {
    let file_path = "./snowset-main.parquet/merged_01.parquet";

    println!("Reading Metadata...");
    let mut file = File::open(&file_path).await?;
    let metadata =
        ArrowReaderMetadata::load_async(&mut file, Default::default()).await?;
    println!(
        "Loaded {} with size: {} MB",
        &file_path,
        metadata.metadata().memory_size() as f64 / 1024. / 1024.
    );
    println!("Starting Benchmark...");

    let bytes_read = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();

    // Open the file for actual reading and wrap it in our coalescing reader.
    let file = File::open(&file_path).await?;
    let coalescing_file = CoalescingReader::new(file).await;

    // Pass our coalescing reader (which implements AsyncRead + AsyncSeek) to the
    // ParquetRecordBatchStreamBuilder.
    let mut builder =
        ParquetRecordBatchStreamBuilder::new_with_metadata(coalescing_file, metadata);
    builder = builder.with_batch_size(64_000);
    let mut stream = builder.build()?;
    let mut pinned_stream = Pin::new(&mut stream);

    let mut record_batch = get_next_item_from_reader_and_count_bytes(&mut pinned_stream)
        .await
        .unwrap();
    while let Some(batch) =
        get_next_item_from_reader_and_count_bytes(&mut pinned_stream).await
    {
        record_batch = batch;
    }

    let bytes_read = bytes_read.load(Ordering::Relaxed);
    let elapsed = start.elapsed();
    let total_mb = bytes_read as f64 / (1024.0 * 1024.0);
    let mib_s = total_mb / elapsed.as_secs_f64();
    let gb = bytes_read as f64 / 1024.0 / 1024.0 / 1024.0;

    println!("Read {:.3} GB in {:?} ({:.2} MiB/s)", gb, elapsed, mib_s);

    Ok(())
}

/// This helper function is unchanged from before.
async fn get_next_item_from_reader_and_count_bytes(
    pinned_stream: &mut Pin<&mut ParquetRecordBatchStream<CoalescingReader>>,
) -> Option<RecordBatch> {
    match pinned_stream.as_mut().next().await {
        Some(Ok(record_batch)) => Some(record_batch.clone()),
        Some(Err(e)) => {
            eprintln!("Error: {:?}", e);
            None
        }
        None => None,
    }
}
