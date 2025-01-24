use std::{error::Error, path::PathBuf};

use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use tokio::fs::File;

const INPUT_FILE_NAME: &str = "testing_output.parquet";
const COLUMN_NAME: &str = "Float";

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let file_path = PathBuf::from(INPUT_FILE_NAME);
    let mut file = File::open(&file_path).await?;
    let metadata = ArrowReaderMetadata::load_async(&mut file, Default::default()).await?;
    let metadata = metadata.metadata();

    for (row_group_index, row_group) in metadata.row_groups().iter().enumerate() {
        if let Some(column) = row_group
            .columns()
            .iter()
            .find(|c| c.column_path().string() == COLUMN_NAME)
        {
            let column_type = column.column_type().to_string();
            if let Some(stats) = column.statistics() {
                if let (Some(min_bytes), Some(max_bytes)) =
                    (stats.min_bytes_opt(), stats.max_bytes_opt())
                {
                    let min_value = bytes_to_value(min_bytes, &column_type)?;
                    let max_value = bytes_to_value(max_bytes, &column_type)?;
                    println!(
                        "Row Group: {}, MIN: {}, MAX: {}",
                        row_group_index, min_value, max_value
                    );
                } else {
                    println!("No min or max values found");
                }
            } else {
                println!("No statistics found");
            }
        }
    }

    println!("Finish");
    Ok(())
}

fn bytes_to_value(bytes: &[u8], column_type: &str) -> Result<f64, Box<dyn Error>> {
    match column_type {
        "INT32" => {
            if bytes.len() != 4 {
                return Err("Expected 4 bytes for INT32".into());
            }
            let int_value = i32::from_le_bytes(bytes.try_into()?);
            Ok(int_value as f64)
        }
        "INT64" => {
            if bytes.len() != 8 {
                return Err("Expected 8 bytes for INT64".into());
            }
            let int_value = i64::from_le_bytes(bytes.try_into()?);
            Ok(int_value as f64)
        }
        "FLOAT" => {
            if bytes.len() != 4 {
                return Err("Expected 4 bytes for FLOAT".into());
            }
            let float_value = f32::from_le_bytes(bytes.try_into()?);
            Ok(float_value as f64)
        }
        "DOUBLE" => {
            if bytes.len() != 8 {
                return Err("Expected 8 bytes for DOUBLE".into());
            }
            let double_value = f64::from_le_bytes(bytes.try_into()?);
            Ok(double_value)
        }
        _ => Err(format!("Unsupported column type: {}", column_type).into()),
    }
}
