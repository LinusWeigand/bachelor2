use std::error::Error;

use parquet::{arrow::arrow_reader::ArrowReaderMetadata, data_type::AsBytes};
use tokio::{fs::{read_dir, File}, io::{AsyncWriteExt, BufWriter}};

const FOLDER_PATH: &str = "./snowset-main.parquet";
const COLUMN_NAME: &str = "memoryUsed";

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    //read metadata of snowset dataset
    // look only at memoryUsed
    // bucket all max values and sort
    // then print out the selectivity buckets 
    //
    //
    // Goal
    // Best case
    // Worst Case
    // Workload 
    //
    
    let mut max_vec = Vec::new();
    let mut counter = 0;
    let max_counts = 100;

    let mut dir = read_dir(FOLDER_PATH).await?;
    while let Some(entry) = dir.next_entry().await? {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if !path.to_string_lossy().contains("test") {
            continue;
        }
        counter += 1;
        // if counter > max_counts {
        //     break;
        // }

        let mut file = File::open(&path).await?;
        let metadata = ArrowReaderMetadata::load_async(&mut file, Default::default()).await?;
        let metadata = metadata.metadata();
        for row_group in metadata.row_groups().iter() {
            let column = match row_group
                .columns()
                .iter()
                .find(|c| c.column_path().string() == COLUMN_NAME) 
                {
                    Some(v) => v,
                    None => continue,
                };

            let column_type = column.column_type().to_string();
            let stats = match column.statistics() 
            {
                Some(v) => v,
                None => continue,
            };
            let max_bytes = match stats.max_bytes_opt() {
                Some(v) => v,
                None => continue,
            };
            let max = match bytes_to_value(max_bytes, &column_type) {
                Ok(v) => v,
                Err(_) => continue,
            };
            max_vec.push(max);
        }
        println!("Progress: {:.2}%", counter as f64 / 2002. * 100.);
    }

    println!("Length: {}", max_vec.len());
    max_vec.sort();
    match max_vec.last() {
        Some(v) => println!("Last max: {}", v),
        None => println!("max vec emptry"),
    }

    let file = File::create("max").await?;
    let mut writer = BufWriter::new(file);
    let index = max_vec.len() as f64 / 100.;
    for i in 0..100 {
        let max_index = std::cmp::min(max_vec.len() - 1, max_vec.len() - (index * i as f64) as usize);
        let max_value = max_vec[max_index];
        let line = format!("{} {}\n", i, max_value);
        writer.write_all(line.as_bytes()).await?;
    }

    writer.flush().await?;

    Ok(())
}

fn bytes_to_value(bytes: &[u8], column_type: &str) -> Result<u64, Box<dyn Error>> {
    match column_type {
        "INT32" => {
            if bytes.len() != 4 {
                return Err("Expected 4 bytes for INT32".into());
            }
            let int_value = i32::from_le_bytes(bytes.try_into()?);
            Ok(int_value as u64)
        }
        "INT64" => {
            if bytes.len() != 8 {
                return Err("Expected 8 bytes for INT64".into());
            }
            let int_value = i64::from_le_bytes(bytes.try_into()?);
            Ok(int_value as u64)
        }
        _ => Err(format!("Unsupported column type: {}", column_type).into()),
    }
}
