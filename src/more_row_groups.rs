use std::{collections::HashMap, error::Error, pin::Pin};

use arrow::{array::RecordBatch, datatypes::Schema};
use futures::StreamExt;
use parquet::{
    arrow::{
        async_reader::ParquetRecordBatchStream, AsyncArrowWriter, ParquetRecordBatchStreamBuilder,
    },
    basic::Compression,
    file::{properties::{EnabledStatistics, WriterProperties}},
};
use tokio::fs::File;

use crate::{bloom_filter::BloomFilter, query::{ColumnMaps}};

#[derive(Eq, Hash, PartialEq, Debug)]
pub struct ColumnChunkLocation {
    pub row_group_index: usize,
    pub column_name: String,
}

const ROWS_PER_GROUP: usize = 2;

// increase row groups
// add bloom filter
pub async fn prepare_file(
    input_file: File,
    output_file: File,
    rows_per_group: usize,
) -> Result<HashMap<ColumnChunkLocation, Option<BloomFilter>>, Box<dyn Error>> {
    let mut bloom_filters: HashMap<ColumnChunkLocation, Option<BloomFilter>> = HashMap::new();
    let mut row_group_index = 0;

    let builder = ParquetRecordBatchStreamBuilder::new(input_file)
        .await?
        .with_batch_size(rows_per_group);

    let metadata = builder.metadata().file_metadata();
    let column_names: Vec<String> = metadata.schema_descr().columns().iter().map(|column| column.path().to_string().trim().trim_matches('"').to_string()).collect();

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

    insert_bloom_filters_from_batch(&record_batch, row_group_index, &column_names, &mut bloom_filters);

    writer.write(&record_batch).await?;
    writer.flush().await?;

    while let Some(record_batch) = get_next_item_from_reader(&mut pinned_stream).await {
        row_group_index += 1;

        insert_bloom_filters_from_batch(&record_batch, row_group_index, &column_names, &mut bloom_filters);

        writer.write(&record_batch).await?;
        writer.flush().await?;
    }

    writer.close().await?;
    Ok(bloom_filters)
}

//TODO maybe can't find because column names have "" around them?
fn get_column_maps(schema: Schema, columns: &Vec<String>) -> ColumnMaps {
    let mut index_to_name = HashMap::new();

    let fields = schema.fields();
    let name_to_index: HashMap<String, usize> = columns
        .iter()
        .filter_map(|column| {
            let field = fields.find(column);
            if let Some((index, _)) = field {
                index_to_name.insert(index, column.to_owned());
                return Some((column.to_owned(), index))
            }
            None
        })
    .collect();
    ColumnMaps { index_to_name, name_to_index}
}

fn insert_bloom_filters_from_batch(
    batch: &RecordBatch,
    row_group_index: usize,
    column_names: &Vec<String>,
    bloom_filters: &mut HashMap<ColumnChunkLocation, Option<BloomFilter>>,
) {
    let num_cols = batch.num_columns();

    let schema = batch.schema().as_ref().clone();
    let column_maps = get_column_maps(schema, &column_names);

    for column_index in 0..num_cols {
        // println!("Populating Bloom Filter Column: {}", column_index);
        let column = batch.column(column_index);

        let mut bloom_filter = BloomFilter::new(7, 2);
        let entry = match bloom_filter.populate_from_column(column) {
            Ok(_) => Some(bloom_filter),
            Err(_) => None,
        };
        if let Some(filter) = &entry {
            // println!("Insert Bloom Filter with bit array: {:?}", filter.bit_array);
        } else {
            println!("Couldn't get a bloom filter for column: {}", column_index);
        }

        if let Some(column_name) = column_maps.index_to_name.get(&column_index) {
            let column_name = column_name.to_owned();
            let column_chunk_location = ColumnChunkLocation {
                row_group_index,
                column_name: column_name.clone(),
            };
            if &column_name == "Age" {
                println!("ColumnChunkLocation: {:#?}", column_chunk_location);
                if let Some(entry) = &entry {
                    println!("Bit Array: {:?}", entry.bit_array);
                }
            }
            bloom_filters.insert(
                column_chunk_location,
                entry,
            );
        }
    }
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
