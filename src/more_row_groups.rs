use std::{error::Error, pin::Pin};

use arrow::array::RecordBatch;
use futures::StreamExt;
use parquet::{
    arrow::{
        async_reader::ParquetRecordBatchStream, AsyncArrowWriter, ParquetRecordBatchStreamBuilder,
    },
    basic::Compression,
    file::properties::{EnabledStatistics, WriterProperties},
};
use tokio::fs::File;

use crate::{bloom_filter::BloomFilter, ROWS_PER_GROUP};

// increase row groups
// add bloom filter
pub async fn prepare_file(
    input_file: File,
    output_file: File,
    rows_per_group: usize,
) -> Result<Vec<Vec<Option<BloomFilter>>>, Box<dyn Error>> {
    let mut bloom_filters = Vec::new();

    let builder = ParquetRecordBatchStreamBuilder::new(input_file)
        .await?
        .with_batch_size(rows_per_group);

    let mut stream = builder.build()?;
    let mut pinned_stream = Pin::new(&mut stream);

    let record_batch = get_next_item_from_reader(&mut pinned_stream).await;

    if record_batch.is_none() {
        return Err("File is empty".into());
    }
    let record_batch = record_batch.unwrap();

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(ROWS_PER_GROUP)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let mut writer = AsyncArrowWriter::try_new(output_file, record_batch.schema(), Some(props))?;

    let row_group_bloom_filters = get_bloom_filters_from_batch(&record_batch).await;
    bloom_filters.push(row_group_bloom_filters);

    writer.write(&record_batch).await?;
    writer.flush().await?;

    while let Some(record_batch) = get_next_item_from_reader(&mut pinned_stream).await {
        let row_group_bloom_filters = get_bloom_filters_from_batch(&record_batch).await;
        bloom_filters.push(row_group_bloom_filters);

        writer.write(&record_batch).await?;
        writer.flush().await?;
    }

    writer.close().await?;
    Ok(bloom_filters)
}

async fn get_bloom_filters_from_batch(batch: &RecordBatch) -> Vec<Option<BloomFilter>> {
    let num_cols = batch.num_columns();

    let mut bloom_filters = Vec::with_capacity(num_cols);

    for col_index in 0..num_cols {
        let column = batch.column(col_index);

        let mut bloom_filter = BloomFilter::new(10_000, 3);
        let entry = match bloom_filter.populate_from_column(column) {
            Ok(_) => Some(bloom_filter),
            Err(_) => None,
        };
        bloom_filters.insert(col_index, entry);
    }
    bloom_filters
}

async fn get_next_item_from_reader(
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
