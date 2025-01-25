use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use query::MetadataEntry;
use std::{error::Error, path::PathBuf};
use tokio::fs::{File, OpenOptions};

mod bloom_filter;
mod more_row_groups;
pub mod parse;
pub mod query;
pub mod row_filter;
pub mod utils;

const INPUT_FILE_NAME: &str = "output.parquet";
const COLUMN_NAME: &str = "memoryUsed";

const INPUT_FILE: &str = "test.parquet";
const OUTPUT_FILE: &str = "output.parquet";
const ROWS_PER_GROUP: usize = 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
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
    let file_metadata = metadata.metadata().file_metadata();
    let column_maps = query::get_column_maps(&file_metadata);

    let metadata_entry = MetadataEntry {
        file_path,
        metadata,
        column_maps,
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
            let file_metadata = metadata.metadata().file_metadata();
            let column_maps = query::get_column_maps(&file_metadata);
            //TODO: Insert into cache
            &MetadataEntry {
                file_path,
                metadata,
                column_maps,
            }
        }
    };

    let _input = format!(
        "{} == {} AND {} == {}",
        COLUMN_NAME, 30_000_000 as i64, COLUMN_NAME, 100_000_000_000 as i64,
    );
    //
    let input = format!("endTime < 2018-03-03-14:23:41");
    let expression = parse::parse_expression(&input)?;

    let select_columns = vec!["memoryUsed".to_owned()];

    let result = query::smart_query_parquet(
        &metadata_entry,
        bloom_filters,
        Some(expression),
        Some(select_columns),
    )
    .await?;

    println!("Result: {:#?}", result);
    Ok(())
}
