use core::f32;
use std::{error::Error, sync::Arc};

use arrow::{
    array::{BooleanArray, Date64Array, Float32Array, Int64Array, RecordBatch, StringArray},
    datatypes::{DataType, Field, Schema},
};
use parquet::{
    arrow::AsyncArrowWriter,
    basic::Compression,
    file::properties::{EnabledStatistics, WriterProperties},
};
use tokio::fs::OpenOptions;
const ROWS_PER_GROUP: usize = 2;
const OUTPUT_FILE_PATH: &str = "testing.parquet";

mod parse;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(OUTPUT_FILE_PATH)
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
        false, false, true, false, true, false,
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

    // Query

    let input = "age > 25";
    let expression = parse::parse_expression(input)?;

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
