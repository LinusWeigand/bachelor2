use core::f32;
use arrow::{array::{BooleanArray, Date64Array, Float32Array, Int8Array, RecordBatch, StringArray}, datatypes::{DataType, Field, Schema}};
use parquet::{arrow::AsyncArrowWriter, basic::Compression, file::properties::{EnabledStatistics, WriterProperties}};
use tokio::{fs::OpenOptions, io::BufReader};
use std::{error::Error, path::PathBuf, sync::Arc};

use arrow2::{io::parquet::{read::{infer_schema, read_metadata_async}}};

use query::MetadataItem;
use tokio::{
    fs::{File}
};
use tokio_util::compat::TokioAsyncReadCompatExt;
const ROWS_PER_GROUP: usize = 2;
const INPUT_FILE_PATH: &str = "testing_input.parquet";
const OUTPUT_FILE_PATH: &str = "testing_output.parquet";

pub mod aggregation;
pub mod bloom_filter;
pub mod parse;
pub mod query;
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
    let aggregation = "SUM(Age)  ,  SUM(Age),  SUM(Float)";
    let expression = "";
    // let expression = "Float >= 44.44";
    // let select_columns = "Name";
    let select_columns = "";
    let mode = Mode::Aggr;
    write_example_file().await?;

    // Get Metadata
    let path = PathBuf::from(OUTPUT_FILE_PATH);
    let file = File::open(&path).await?;
    let mut buf_reader = BufReader::new(file).compat();
    let metadata = read_metadata_async(&mut buf_reader).await?;
    let schema = infer_schema(&metadata)?;
    let name_to_index = utils::get_column_name_to_index(&metadata);
    let columns_to_print;

    let aggregation = match aggregation {
        "" => None,
        v => {
            Some(v.split(",").filter_map(|v| parse::aggregation::parse_aggregation(v.trim()).ok()).collect())
        },
    };
    let select_columns = match select_columns {
        "" => {
            columns_to_print = utils::get_column_names(&metadata);
            None
        }
        v => {
            let projection: Vec<String> = v.split(",").map(|v| v.to_owned()).collect();
            columns_to_print = projection.clone();
            Some(projection)
        }
    };
    let expression = match expression {
        "" => None,
        v => Some(parse::expression::parse_expression(v)?),
    };
    let row_groups = metadata.row_groups;

    let metadata_item = MetadataItem {
        path,
        schema,
        row_groups,
        name_to_index,
    };

    // Query
    let (results, bytes_read, aggr_table) = query::smart_query_parquet(
        metadata_item,
        expression,
        select_columns,
        aggregation,
        &mode,
    )
    .await?;

    println!("Bytes read: {:?}", bytes_read);

    let result = utils::aggregate_chunks(results.as_slice());
    if let Some(result) = result {
        let output = arrow2::io::print::write(&[result.clone()], &columns_to_print);
        println!("{}", output);
    }
    if let Some(aggr_table) = aggr_table {
        let output = arrow2::io::print::write(&[aggr_table.chunk], aggr_table.names.as_slice());
        println!("{}", output);
    }

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


