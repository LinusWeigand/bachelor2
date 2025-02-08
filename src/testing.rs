use core::f32;
use std::{env, error::Error, path::PathBuf, process::exit, sync::Arc};

use arrow::{
    array::{BooleanArray, Date64Array, Float32Array, Int8Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
    util::pretty::print_batches,
};
use parquet::{
    arrow::{arrow_reader::ArrowReaderMetadata, AsyncArrowWriter},
    basic::Compression,
    file::properties::{EnabledStatistics, WriterProperties},
};

use query::MetadataEntry;
use tokio::fs::{File, OpenOptions};
const ROWS_PER_GROUP: usize = 2;
const INPUT_FILE_PATH: &str = "testing_input.parquet";
const OUTPUT_FILE_PATH: &str = "testing_output.parquet";

pub mod aggregation;
pub mod bloom_filter;
pub mod more_row_groups;
pub mod parse;
pub mod query;
pub mod row_filter;
pub mod row_group_filter;
pub mod utils;

#[derive(PartialEq)]
pub enum Mode {
    Base,
    Group,
    Bloom,
    Row,
    Column,
    Aggr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    write_example_file().await?;

    let input_file = File::open(INPUT_FILE_PATH).await?;
    let output_file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(OUTPUT_FILE_PATH)
        .await?;

    // Get Bloom Filters
    let bloom_filters =
        more_row_groups::prepare_file(input_file, output_file, ROWS_PER_GROUP).await?;

    // Get Metadata
    let file_path = PathBuf::from(OUTPUT_FILE_PATH);
    let mut file = File::open(&file_path).await?;
    let metadata = ArrowReaderMetadata::load_async(&mut file, Default::default()).await?;

    //Aggregation
    let aggregation = Some(vec![parse::aggregation::parse_aggregation("SUM(Age)")?]);

    let metadata_entry = MetadataEntry {
        file_path,
        metadata,
        bloom_filters,
    };

    // Query
    let select_columns = Some(vec!["Graduated".to_owned()]);
    let input = "Age < -40";
    let expression = if input.is_empty() {
        None
    } else {
        Some(parse::expression::parse_expression(input)?)
    };
    // let (results, bytes_read) =
    //     query::smart_query_parquet(&metadata_entry, expression, select_columns, aggregation, Mode::Base)
    //         .await?;
    //
    // print_batches(&results)?;
    // println!("Bytes read: {:?}", bytes_read);

    Ok(())
}

async fn write_example_file() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Example Parquet
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(INPUT_FILE_PATH)
        .await?;
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .set_max_row_group_size(ROWS_PER_GROUP)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();
    let name = Field::new("Name", DataType::Utf8, false);
    let age = Field::new("Age", DataType::Int8, false);
    let birthday = Field::new("Birthday", DataType::Date64, false);
    let graduated = Field::new("Graduated", DataType::Boolean, false);
    let float = Field::new("Float", DataType::Float32, false);

    let schema = Schema::new(vec![name, age, birthday, graduated, float]);
    let schema = Arc::new(schema);
    let mut writer = AsyncArrowWriter::try_new(file, schema.clone(), Some(props))?;
    let array_names = Arc::new(StringArray::from(vec![
        "Alice", "Bob", "Charlie", "Dave", "Eve", "Frank",
    ]));
    let array_age = Arc::new(Int8Array::from(vec![10, 60, 30, -30, 50, 0]));
    let array_birthday = Arc::new(Date64Array::from(vec![
        1420074061000,
        1104541261000,
        788922061000,
        473389261000,
        157770061,
        1735693261000,
    ]));
    let array_graduated = Arc::new(BooleanArray::from(vec![
        false, false, true, false, true, true,
    ]));
    let array_float = Arc::new(Float32Array::from(vec![
        11.11,
        -22.22,
        0.,
        44.44,
        f32::MIN,
        f32::MAX,
    ]));
    let record_batch = RecordBatch::try_new(
        schema,
        vec![
            array_names,
            array_age,
            array_birthday,
            array_graduated,
            array_float,
        ],
    )?;
    writer.write(&record_batch).await?;
    writer.close().await?;
    Ok(())
}
