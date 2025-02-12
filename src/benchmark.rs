use std::{
    collections::HashMap,
    env,
    error::Error,
    path::PathBuf,
    process::exit,
    sync::{atomic::AtomicUsize, Arc},
};

use parquet::arrow::arrow_reader::ArrowReaderMetadata;

use tokio::{
    fs::{read_dir, File},
    io::{AsyncBufReadExt, BufReader},
    time::Instant,
};
const ROWS_PER_GROUP: usize = 2;

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

#[derive(PartialEq)]
pub enum Workload {
    BestCase,
    WorstCase,
    Quarter,
    Half,
    ThreeQuarter,
    Real,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    //Arguments
    let args: Vec<String> = env::args().collect();
    let mut mode = Mode::Base;
    let mut workload = Workload::WorstCase;
    let mut iter = args.iter().skip(1);
    let mut max_counts = 10000;
    let mut folder_path = "merged";
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-m" | "--mode" => {
                if let Some(v) = iter.next() {
                    mode = match v.as_str() {
                        "base" => Mode::Base,
                        "group" => Mode::Group,
                        "bloom" => Mode::Bloom,
                        "row" => Mode::Row,
                        "column" => Mode::Column,
                        "aggr" => Mode::Aggr,
                        _ => {
                            eprintln!("Error: Unknown Mode: {}", v);
                            exit(1);
                        }
                    };
                } else {
                    eprintln!("Error: -m/--mode requires an argument.");
                    exit(1);
                }
            }
            "-w" | "--workload" => {
                if let Some(v) = iter.next() {
                    workload = match v.as_str() {
                        "best-case" => Workload::BestCase,
                        "worst-case" => Workload::WorstCase,
                        "real" => Workload::Real,
                        "quarter" => Workload::Quarter,
                        "half" => Workload::Half,
                        "3-quarter" => Workload::ThreeQuarter,
                        _ => {
                            eprintln!("Error: Unknown Workload: {}", v);
                            exit(1);
                        }
                    };
                } else {
                    eprintln!("Error: -w/--workload requires an argument.");
                    exit(1);
                }
            }
            "-c" | "--counter" => {
                if let Some(v) = iter.next() {
                    max_counts = match v.parse::<usize>() {
                        Ok(x) => x,
                        Err(_) => {
                            eprintln!("Error: Unknown counter value {}", v);
                            exit(1);
                        }
                    };
                } else {
                    eprintln!("Error: -c/--counter requires an argument.");
                    exit(1);
                }
            }
            "-p" | "--path" => {
                if let Some(v) = iter.next() {
                    folder_path = v;
                } else {
                    eprintln!("Error: -p/--path requires an argument.");
                    exit(1);
                }
            }
            _ => {
                eprintln!("Unknown argument: {}", arg);
                exit(1);
            }
        }
    }
    let mode = Arc::new(mode);
    let workload_map = prepare_workload().await?;
    let expression = match workload {
        Workload::WorstCase => workload_map.get(&99),
        Workload::BestCase => workload_map.get(&0),
        Workload::Real => workload_map.get(&1),
        Workload::Quarter => workload_map.get(&25),
        Workload::Half => workload_map.get(&50),
        Workload::ThreeQuarter => workload_map.get(&75),
    };
    let expression = Arc::new(expression.unwrap().to_owned());

    // Get Metadata
    println!("Reading Metadata");
    let mut metadata_vec = Vec::new();
    let mut dir = read_dir(folder_path).await?;
    let mut counter = 0;
    while let Some(entry) = dir.next_entry().await? {
        let path = entry.path();
        if counter > max_counts {
            break;
        }
        counter += 1;
        let mut file = File::open(&path).await?;
        let metadata = ArrowReaderMetadata::load_async(&mut file, Default::default()).await?;
        metadata_vec.push((path, metadata));
    }

    println!("Starting benchmark...");
    let start = Instant::now();
    let queries: Vec<_> = metadata_vec
        .into_iter()
        .map(|(path, metadata)| {
            let mode = Arc::clone(&mode);
            let expression = Arc::clone(&expression);
            tokio::spawn(async move {
                let result = make_query(&path, metadata, &expression, &mode).await;
                result
            })
        })
        .collect();

    let bytes_read = futures::future::join_all(queries).await;
    let sum = bytes_read
        .into_iter()
        .filter_map(|result| result.ok())
        .filter_map(|result| result.ok())
        .map(|arc_atomic| arc_atomic.load(std::sync::atomic::Ordering::Relaxed))
        .sum::<usize>();

    let duration = start.elapsed();

    let kb = sum as f64 / 1_000.;
    let mb = kb / 1_000.;
    let gb = mb / 1_000.;
    println!("+----------------------------------------------+");
    println!("+----------------------------------------------+");
    println!(
        "Disk Throughput: {:.2}MB/s",
        mb / duration.as_millis() as f64 * 1000.
    );
    println!(
        "Qps: {:.2}",
        max_counts as f64 / duration.as_millis() as f64 * 1000.
    );
    println!("+----------------------------------------------+");
    println!("GB read: {:.2}GB", gb);
    println!("Time taken: {:.2?}", duration);
    println!("+----------------------------------------------+");
    println!("+----------------------------------------------+");

    Ok(())
}

async fn make_query(
    file_path: &PathBuf,
    metadata: ArrowReaderMetadata,
    expression: &str,
    mode: &Mode,
) -> Result<Arc<AtomicUsize>, Box<dyn Error + Send + Sync>> {
    let aggregation = Some(vec![parse::aggregation::parse_aggregation("SUM(Age)")?]);

    // Query
    let select_columns = Some(vec!["memoryUsed".to_owned()]);
    let expression = if expression.is_empty() {
        None
    } else {
        Some(parse::expression::parse_expression(expression)?)
    };
    let (_, bytes_read) = query::smart_query_parquet(
        file_path,
        metadata,
        expression,
        select_columns,
        aggregation,
        mode,
    )
    .await?;
    Ok(bytes_read)
}

async fn prepare_workload() -> Result<HashMap<usize, String>, Box<dyn Error + Send + Sync>> {
    let mut result = HashMap::new();
    let file = File::open("max").await?;

    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    while let Some(line) = lines.next_line().await? {
        let parts: Vec<&str> = line.split_whitespace().collect();
        let selectivity = parts[0].parse::<usize>()?;
        let max_value = parts[1];
        let expression = format!("memoryUsed > {}", max_value);
        result.insert(selectivity, expression);
    }
    Ok(result)
}
