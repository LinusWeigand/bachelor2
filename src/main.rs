use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use query::MetadataEntry;
use std::{error::Error, path::PathBuf};
use tokio::fs::{File, OpenOptions};

pub mod aggregation;
mod bloom_filter;
mod more_row_groups;
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

#[derive(PartialEq)]
pub enum Workload {
    BestCase,
    WorstCase,
}

const INPUT_FILE_NAME: &str = "output.parquet";
const COLUMN_NAME: &str = "memoryUsed";

const INPUT_FILE: &str = "test.parquet";
const OUTPUT_FILE: &str = "output.parquet";
const ROWS_PER_GROUP: usize = 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Increase Row Groups
    let input_file = File::open(INPUT_FILE).await?;

    let output_file = OpenOptions::new()
        .read(true)
        .write(true)
        .truncate(true)
        .create(true)
        .open(OUTPUT_FILE)
        .await?;

    let bloom_filters =
        more_row_groups::prepare_file(input_file, output_file, ROWS_PER_GROUP).await?;

    // Store metadata
    let mut cached_metadata: Vec<MetadataEntry> = Vec::new();

    let file_path = PathBuf::from(INPUT_FILE_NAME);
    let mut file = File::open(&file_path).await?;
    let metadata = ArrowReaderMetadata::load_async(&mut file, Default::default()).await?;

    let metadata_entry = MetadataEntry {
        file_path,
        metadata,
        bloom_filters: bloom_filters.clone(),
    };

    cached_metadata.push(metadata_entry);

    // Retrieve Metadata
    let file_path = PathBuf::from(INPUT_FILE_NAME);
    let metadata_entry = cached_metadata
        .iter()
        .find(|entry| entry.file_path == file_path);

    let metadata_entry = match metadata_entry {
        Some(v) => v,
        None => {
            let mut file = File::open(&file_path).await?;
            let metadata = ArrowReaderMetadata::load_async(&mut file, Default::default()).await?;
            //TODO: Insert into cache
            &MetadataEntry {
                file_path,
                metadata,
                bloom_filters,
            }
        }
    };

    let _input = format!(
        "{} == {} AND {} == {}",
        COLUMN_NAME, 30_000_000 as i64, COLUMN_NAME, 100_000_000_000 as i64,
    );
    //
    let input = format!("endTime < 2018-03-03-14:23:41");
    let expression = parse::expression::parse_expression(&input)?;

    let select_columns = vec!["memoryUsed".to_owned()];

    let aggregation = parse::aggregation::parse_aggregation("SUM(Age)")?;

    // let result = query::smart_query_parquet(
    //     &metadata_entry,
    //     Some(expression),
    //     Some(select_columns),
    //     Some(vec![aggregation]),
    //     Mode::Aggr,
    // )
    // .await?;
    //
    // println!("Result: {:#?}", result);
    Ok(())
}
