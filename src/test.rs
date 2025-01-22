use std::{error::Error, path::PathBuf};

use parquet::{
    arrow::arrow_reader::ArrowReaderMetadata,
    file::{
        metadata::{RowGroupMetaData},
        page_index::index::{Index, NativeIndex},
    },
    format::PageLocation,
};
use tokio::fs::File;

use parquet::file::page_index::offset_index::OffsetIndexMetaData;

const INPUT_FILE_NAME: &str = "output.parquet";
const COLUMN_NAME: &str = "memoryUsed";

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn Error>> {
    let file_path = PathBuf::from(INPUT_FILE_NAME);
    let mut file = File::open(&file_path).await?;
    let metadata = ArrowReaderMetadata::load_async(&mut file, Default::default()).await?;
    let metadata = metadata.metadata();

    // These calls return a structure of shape [row_group][column]
    let index_pages = metadata.column_index().expect("No column index found");
    let offset_indexes = metadata.offset_index().expect("No offset index found");

    // Loop over row groups to print row-group-level stats:
    for (rg_idx, row_group) in metadata.row_groups().iter().enumerate() {
        if let Some((col_idx, column_chunk)) = row_group
            .columns()
            .iter()
            .enumerate()
            .find(|(_, c)| c.column_path().string() == COLUMN_NAME)
        {
            let column_type = column_chunk.column_type().to_string();

            // Print row-group-level min/max
            if let Some(stats) = column_chunk.statistics() {
                if let (Some(min_bytes), Some(max_bytes)) = (stats.min_bytes_opt(), stats.max_bytes_opt()) {
                    let min_value = bytes_to_value(min_bytes, &column_type)?;
                    let max_value = bytes_to_value(max_bytes, &column_type)?;
                    println!(
                        "Row Group {}: RG-level MIN={} MAX={}",
                        rg_idx, min_value, max_value
                    );
                } else {
                    println!("Row Group {}: No min or max values found", rg_idx);
                }
            } else {
                println!("Row Group {}: No statistics found", rg_idx);
            }

            // Now retrieve page-level stats + page ranges for this row group & column
            let page_index = &index_pages[rg_idx][col_idx];
            let offset_meta = &offset_indexes[rg_idx][col_idx];
            let rg_num_rows = row_group.num_rows() as u64; // total rows in this row group

            if let Some(page_infos) = get_page_infos(page_index, offset_meta, rg_num_rows) {
                // Print each data page’s min, max, and row range
                for (i, info) in page_infos.iter().enumerate() {
                    println!(
                        "  Data Page {} => range=[{}..{}), MIN={}, MAX={}",
                        i,
                        info.range_start,
                        info.range_start + info.range_length,
                        info.min,
                        info.max
                    );
                }
            } else {
                println!("  (No page-level index available for Row Group {})", rg_idx);
            }
        }
    }

    println!("Finish");
    Ok(())
}

/// A struct to hold both the min/max value and the row range of a data page.
pub struct PageInfo {
    pub min: f64,
    pub max: f64,
    pub range_start: u64,
    pub range_length: u64,
}

/// Given a `page_index` (holding min/max stats for each data page) and
/// the corresponding `offset_meta` (holding the row offsets of each page),
/// return a `Vec<PageInfo>` describing each data page’s stats & row range.
fn get_page_infos(
    page_index: &Index,
    offset_meta: &OffsetIndexMetaData,
    rg_num_rows: u64,
) -> Option<Vec<PageInfo>> {
    // 1) Filter out dictionary pages in offset_meta, which typically have first_row_index = -1.
    //    We only want data pages that have a non-negative first_row_index.
    let data_page_locations: Vec<&PageLocation> = offset_meta
        .page_locations
        .iter()
        .filter(|loc| loc.first_row_index >= 0)
        .collect();

    // 2) Match them up with the page-level stats in `page_index`.
    //    We expect the number of data pages in page_index to match data_page_locations.len().
    match page_index {
        Index::NONE
        | Index::BOOLEAN(_)
        | Index::BYTE_ARRAY(_)
        | Index::FIXED_LEN_BYTE_ARRAY(_) => None,

        Index::INT32(v) => {
            let num_pages = v.indexes.len();
            if num_pages != data_page_locations.len() {
                // The file might have some corner-case mismatch; you can decide how to handle it
                eprintln!(
                    "Warning: mismatch in #pages: offset_meta has {}, column_index has {}",
                    data_page_locations.len(),
                    num_pages
                );
                return None;
            }

            // For each page, compute min/max from the index, and the row-range from offset_meta
            let mut result = Vec::with_capacity(num_pages);
            for i in 0..num_pages {
                let idx = &v.indexes[i];
                let page_loc = data_page_locations[i];

                // row range start
                let start = page_loc.first_row_index as u64;
                // row range end is either next page's first_row_index or rg_num_rows if last page
                let end = if i + 1 < data_page_locations.len() {
                    data_page_locations[i + 1].first_row_index as u64
                } else {
                    rg_num_rows
                };

                result.push(PageInfo {
                    min: idx.min.unwrap_or(i32::MIN) as f64,
                    max: idx.max.unwrap_or(i32::MAX) as f64,
                    range_start: start,
                    range_length: end - start,
                });
            }
            Some(result)
        }

        Index::INT64(v) => {
            let num_pages = v.indexes.len();
            if num_pages != data_page_locations.len() {
                eprintln!(
                    "Warning: mismatch in #pages: offset_meta has {}, column_index has {}",
                    data_page_locations.len(),
                    num_pages
                );
                return None;
            }
            let mut result = Vec::with_capacity(num_pages);
            for i in 0..num_pages {
                let idx = &v.indexes[i];
                let page_loc = data_page_locations[i];

                let start = page_loc.first_row_index as u64;
                let end = if i + 1 < data_page_locations.len() {
                    data_page_locations[i + 1].first_row_index as u64
                } else {
                    rg_num_rows
                };

                result.push(PageInfo {
                    min: idx.min.unwrap_or(i64::MIN) as f64,
                    max: idx.max.unwrap_or(i64::MAX) as f64,
                    range_start: start,
                    range_length: end - start,
                });
            }
            Some(result)
        }

        Index::FLOAT(v) => {
            let num_pages = v.indexes.len();
            if num_pages != data_page_locations.len() {
                eprintln!(
                    "Warning: mismatch in #pages: offset_meta has {}, column_index has {}",
                    data_page_locations.len(),
                    num_pages
                );
                return None;
            }
            let mut result = Vec::with_capacity(num_pages);
            for i in 0..num_pages {
                let idx = &v.indexes[i];
                let page_loc = data_page_locations[i];

                let start = page_loc.first_row_index as u64;
                let end = if i + 1 < data_page_locations.len() {
                    data_page_locations[i + 1].first_row_index as u64
                } else {
                    rg_num_rows
                };

                result.push(PageInfo {
                    min: idx.min.unwrap_or(f32::MIN) as f64,
                    max: idx.max.unwrap_or(f32::MAX) as f64,
                    range_start: start,
                    range_length: end - start,
                });
            }
            Some(result)
        }

        Index::DOUBLE(v) => {
            let num_pages = v.indexes.len();
            if num_pages != data_page_locations.len() {
                eprintln!(
                    "Warning: mismatch in #pages: offset_meta has {}, column_index has {}",
                    data_page_locations.len(),
                    num_pages
                );
                return None;
            }
            let mut result = Vec::with_capacity(num_pages);
            for i in 0..num_pages {
                let idx = &v.indexes[i];
                let page_loc = data_page_locations[i];

                let start = page_loc.first_row_index as u64;
                let end = if i + 1 < data_page_locations.len() {
                    data_page_locations[i + 1].first_row_index as u64
                } else {
                    rg_num_rows
                };

                result.push(PageInfo {
                    min: idx.min.unwrap_or(f64::MIN),
                    max: idx.max.unwrap_or(f64::MAX),
                    range_start: start,
                    range_length: end - start,
                });
            }
            Some(result)
        }

        Index::INT96(v) => {
            // INT96 is usually timestamps. We'll convert to i64 as a fallback.
            let num_pages = v.indexes.len();
            if num_pages != data_page_locations.len() {
                eprintln!(
                    "Warning: mismatch in #pages: offset_meta has {}, column_index has {}",
                    data_page_locations.len(),
                    num_pages
                );
                return None;
            }
            let mut result = Vec::with_capacity(num_pages);
            for i in 0..num_pages {
                let idx = &v.indexes[i];
                let page_loc = data_page_locations[i];

                let start = page_loc.first_row_index as u64;
                let end = if i + 1 < data_page_locations.len() {
                    data_page_locations[i + 1].first_row_index as u64
                } else {
                    rg_num_rows
                };

                result.push(PageInfo {
                    min: idx.min.map(|x| x.to_i64() as f64).unwrap_or(f64::MIN),
                    max: idx.max.map(|x| x.to_i64() as f64).unwrap_or(f64::MAX),
                    range_start: start,
                    range_length: end - start,
                });
            }
            Some(result)
        }
    }
}

/// Converts the row‐group‐level min/max bytes into a f64, depending on the parquet physical type.
/// This is needed for row-group statistics (not for page-level, which we already get typed in `Index`).
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
        _ => Err(format!("Unsupported column type for row-group stats: {}", column_type).into()),
    }
}
