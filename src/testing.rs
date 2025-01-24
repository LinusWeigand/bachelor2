use core::f32;
use std::{error::Error, path::PathBuf, sync::Arc};

use arrow::{
    array::{BooleanArray, Date64Array, Float32Array, Int32Array, Int64Array, RecordBatch, StringArray},
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

pub mod bloom_filter;
pub mod more_row_groups;
mod parse;
pub mod query;
// pub mod row_filter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
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

    let schema = Arc::new(build_schema());
    let mut writer = AsyncArrowWriter::try_new(file, schema.clone(), Some(props))?;

    let array_names = Arc::new(StringArray::from(vec![
        "Alice", "Bob", "Charlie", "Dave", "Eve", "Frank",
    ]));
    let array_age = Arc::new(Int64Array::from(vec![10, i64::MAX, 30, i64::MIN, 50, 0]));
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

    // Get Bloom Filters
    let input_file = File::open(INPUT_FILE_PATH).await?;
    let output_file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(OUTPUT_FILE_PATH)
        .await?;

    let bloom_filters =
        more_row_groups::prepare_file(input_file, output_file, ROWS_PER_GROUP).await?;

    // Get Metadata

    let file_path = PathBuf::from(OUTPUT_FILE_PATH);
    let mut file = File::open(&file_path).await?;
    let metadata = ArrowReaderMetadata::load_async(&mut file, Default::default()).await?;
    let file_metadata = metadata.metadata().file_metadata();
    let column_maps = query::get_column_maps(&file_metadata);

    let metadata_entry = MetadataEntry {
        file_path,
        metadata,
        column_maps,
    };

    // Query

    let input = "Birthday == 2025-1-1-12:1:1";
    // let input = "";

    let expression = if input.is_empty() {
        None
    } else {
        Some(parse::parse_expression(input)?)
    };
    // println!("{:#?}", expression);

    // let select_columns = Some(vec!["Graduated".to_owned()]);
    let select_columns = None;

    let results =
        query::smart_query_parquet(&metadata_entry, bloom_filters, expression, select_columns)
            .await?;

    println!("INPUT: {}", input);
    print_batches(&results)?;

    Ok(())
}

fn build_schema() -> Schema {
    let name = Field::new("Name", DataType::Utf8, false);
    let age = Field::new("Age", DataType::Int64, false);
    let birthday = Field::new("Birthday", DataType::Date64, false);
    let graduated = Field::new("Graduated", DataType::Boolean, false);
    let float = Field::new("Float", DataType::Float32, false);

    let schema = Schema::new(vec![name, age, birthday, graduated, float]);

    schema
}
