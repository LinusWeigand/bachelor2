use std::fs::File;
use std::io::BufReader;
use std::time::Instant;

use arrow2::datatypes::Schema;
use arrow2::error::Result;
use arrow2::io::parquet::read::{infer_schema, FileReader};
use parquet2::read::read_metadata;

fn main() -> Result<()> {
    let file_path = "/mnt/raid0/merged_01.parquet";
    // let file_path = "./snowset-main.parquet/merged_01.parquet";

    println!("Reading Metadata");
    let file = File::open(&file_path)?;
    let mut buf_reader = BufReader::new(file);

    let metadata: arrow2::io::parquet::read::FileMetaData = read_metadata(&mut buf_reader)?;
    // println!("Parquet Metadata:\n{:#?}", metadata);

    let file = File::open(&file_path)?;

    let batch_size = 1024 * 1024;
    let schema: Schema = infer_schema(&metadata)?;

    println!("Starting benchmark");
    let start = Instant::now();
    let reader = FileReader::new(
        file,
        metadata.row_groups,
        schema,
        Some(batch_size),
        None,
        None,
    );

    for maybe_batch in reader {
        let batch = maybe_batch?;
    }

    let elapsed = start.elapsed();
    println!("Time: {}", elapsed.as_millis() as f64 / 1000.);

    Ok(())
}
