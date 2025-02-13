use arrow2::io::parquet::read::{infer_schema, FileReader};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use std::{env, error::Error, process::exit};
use parquet2::read::deserialize_metadata;
use tokio::time::Instant;
use std::io::{Read, Seek, SeekFrom};

use futures::stream::{StreamExt, TryStreamExt};
use tokio::task::spawn_blocking;
use std::path::PathBuf;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

const FILE_PATHS: [&str; 16] = [
    "merged_01.parquet",
    "merged_02.parquet",
    "merged_03.parquet",
    "merged_04.parquet",
    "merged_05.parquet",
    "merged_06.parquet",
    "merged_07.parquet",
    "merged_08.parquet",
    "merged_09.parquet",
    "merged_10.parquet",
    "merged_11.parquet",
    "merged_12.parquet",
    "merged_13.parquet",
    "merged_14.parquet",
    "merged_15.parquet",
    "merged_16.parquet",
];

#[derive(Debug)]
struct RawFooter {
    path: PathBuf,
    footer_size: usize,
    raw_bytes: Vec<u8>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    let args: Vec<String> = env::args().collect();
    let mut iter = args.iter().skip(1);

    let mut folder = "./merged";
    let mut read_size: usize = 4 * 1024 * 1024;
    let mut count: usize = 16;

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-p" | "--path" => {
                if let Some(v) = iter.next() {
                    folder = v;
                } else {
                    eprintln!("Error: -p/--path requires an argument.");
                    exit(1);
                }
            }
            "-s" | "--size" => {
                if let Some(v) = iter.next() {
                    read_size = v.parse().unwrap();
                    read_size *= 1024 * 1024;
                } else {
                    eprintln!("Error: -s/--size requires an argument.");
                    exit(1);
                }
            }
            "-c" | "--count" => {
                if let Some(v) = iter.next() {
                    count = v.parse().unwrap();
                } else {
                    eprintln!("Error: -c/--count requires an argument.");
                    exit(1);
                }
            }
            _ => {
                eprintln!("Unknown argument: {}", arg);
                exit(1);
            }
        }
    }

    count += 1;
    let file_paths: Vec<_> = (1..count)
        .map(|i| PathBuf::from(format!("{}/merged_{:02}.parquet", folder, i)))
        .collect();

    println!("Loading metadata...");
    let raw_footers = load_files(file_paths).await?;

    println!("Starting Benchmark...");
    let start_time = Instant::now();

    let mut tasks = Vec::new();

    for raw_footer in raw_footers {
        let read_size = read_size;
        let task =
            tokio::task::spawn(async move {
                let bytes_read = make_query(raw_footer, read_size).await?;
                // let parsed_footer = parse_raw_footer(&raw_footer.raw_bytes, raw_footer.footer_size)?;
                // let file = std::fs::File::open(&raw_footer.path)?;
                // let schema = infer_schema(&parsed_footer)?;
                // let row_groups = parsed_footer.row_groups;
                // let reader = FileReader::new(
                //     file,
                //     row_groups,
                //     schema,
                //     Some(read_size),
                //     None,
                //     None,
                // );
                //
                // for maybe_batch in reader {
                //     let batch = maybe_batch?;
                // }

                Ok::<Arc<AtomicUsize>, Box<dyn Error + Send + Sync>>(bytes_read)
            });
        tasks.push(task);
    }

    for t in tasks {
        t.await??;
    }

    let elapsed = start_time.elapsed();
    let size = 0.858 * std::cmp::min(count, FILE_PATHS.len()) as f64;
    let seconds = elapsed.as_millis() as f64 / 1000.;
    let tp = size / seconds;
    println!("Time: {:.2}s", seconds);
    println!("Throughput: {:02}MB", tp);

    Ok(())
}

async fn make_query(
    raw_footer: RawFooter,
    read_size: usize,
) -> Result<Arc<AtomicUsize>, Box<dyn Error + Send + Sync>> {
    // Query
    // let select_columns = Some(vec!["memoryUsed".to_owned()]);
    // let expression = "memoryUsed > 16685759632";
    // let expression = if expression.is_empty() {
    //     None
    // } else {
    //     Some(parse_expression(expression)?)
    // };

    let metadata = parse_raw_footer(&raw_footer.raw_bytes, raw_footer.footer_size)?;
    let schema = infer_schema(&metadata)?;
    let row_groups = metadata.row_groups;
    let path = raw_footer.path;

    let file = std::fs::File::open(&path)?;
    let bytes_read = Arc::new(AtomicUsize::new(0));
    let counting_file = CountingReader::new(file, bytes_read.clone());

    // Row Group Filter
    let mut row_groups = row_groups;
    // if let Some(expression) = &expression {
    //         row_groups = row_groups
    //             .into_iter()
    //             .filter_map(
    //                 |md| match keep_row_group(&md, &None, &expression, false) {
    //                     Ok(false) => None,
    //                     Ok(true) | _ => Some(md),
    //                 },
    //             )
    //             .collect();
    // }

    let reader = FileReader::new(
        counting_file,
        row_groups,
        schema,
        Some(read_size),
        None,
        None,
    );
    for maybe_batch in reader {
        let batch = maybe_batch?;
    }

    Ok(bytes_read)
}


async fn load_files(
    file_paths: Vec<PathBuf>,
) -> Result<Vec<RawFooter>, Box<dyn Error + Send + Sync>> {
    let concurrency = file_paths.len();

    let results = futures::stream::iter(file_paths.into_iter().map(|path| {
        tokio::spawn(async move {
            let result = spawn_blocking(move || {
                let mut file = std::fs::File::open(&path)?;

                let file_size = file.seek(SeekFrom::End(0))?;
                if file_size < 12 {
                    return Err(format!("File too small to be valid parquet: {:?}", path).into());
                }

                file.seek(SeekFrom::End(-8))?;
                let mut trailer = [0u8; 8];
                file.read_exact(&mut trailer)?;

                let magic = &trailer[4..];
                if magic != b"PAR1" {
                    return Err(format!("Invalid Parquet file magic in {:?}", path).into());
                }

                let metadata_len = u32::from_le_bytes(trailer[0..4].try_into().unwrap());
                let metadata_len = metadata_len as usize;

                let footer_start = file_size
                    .checked_sub(8 + metadata_len as u64)
                    .ok_or_else(|| format!("metadata_len too large in {:?}", path))?;
                file.seek(SeekFrom::Start(footer_start))?;

                let mut raw_bytes = vec![0u8; metadata_len + 8];
                file.read_exact(&mut raw_bytes)?;

                Ok(RawFooter {
                    path,
                    footer_size: raw_bytes.len(),
                    raw_bytes,
                })
            })
            .await;

            match result {
                Ok(inner_res) => inner_res,
                Err(e) => Err(Box::new(e) as Box<dyn Error + Send + Sync>),
            }
        })
    }))
    .buffer_unordered(concurrency)
    .then(|res| async move {
        match res {
            Ok(task_res) => task_res,
            Err(e) => Err(Box::new(e) as Box<dyn Error + Send + Sync>),
        }
    })
    .try_collect::<Vec<_>>()
    .await?;

    Ok(results)
}

pub enum Feature {
    Group,
    Bloom,
    Row,
    Column,
    Aggr,
}

#[derive(Clone)]
pub struct Condition {
    pub column_name: String,
    pub threshold: ThresholdValue,
    pub comparison: Comparison,
}

#[derive(Clone)]
pub enum ThresholdValue {
    Int64(i64),
    Float64(f64),
    Boolean(bool),
    Utf8String(String),
}

#[derive(Clone)]
pub enum Expression {
    Condition(Condition),
    And(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
}

#[derive(PartialEq, Clone)]
pub enum Comparison {
    LessThan,
    LessThanOrEqual,
    Equal,
    GreaterThanOrEqual,
    GreaterThan,
}

impl Comparison {
    pub fn from_str(input: &str) -> Option<Self> {
        match input {
            "<" => Some(Comparison::LessThan),
            "<=" => Some(Comparison::LessThanOrEqual),
            "==" => Some(Comparison::Equal),
            ">=" => Some(Comparison::GreaterThanOrEqual),
            ">" => Some(Comparison::GreaterThan),
            _ => None,
        }
    }
}

pub fn tokenize(input: &str) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
    let mut tokens = Vec::new();
    let mut current = String::new();

    for c in input.chars() {
        match c {
            '(' | ')' | ' ' => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
                if c != ' ' {
                    tokens.push(c.to_string());
                }
            }
            _ => current.push(c),
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    Ok(tokens)
}

fn parse_raw_footer(raw_bytes: &[u8], max_size: usize) -> Result<parquet2::metadata::FileMetaData, Box<dyn Error + Send + Sync>> {
    if raw_bytes.len() < 8 || &raw_bytes[raw_bytes.len() - 4..] != b"PAR1" {
        return Err("Not a valid parquet footer".into());
    }

    let slice_without_magic = &raw_bytes[..raw_bytes.len() - 4];
    let file_meta = deserialize_metadata(slice_without_magic, max_size)?;
    Ok(file_meta)
}

pub fn parse_expression(input: &str) -> Result<Expression, Box<dyn Error + Send + Sync>> {
    let tokens = tokenize(input)?;
    let mut pos = 0;
    parse_or(&tokens, &mut pos)
}

pub fn parse_or(
    tokens: &[String],
    pos: &mut usize,
) -> Result<Expression, Box<dyn Error + Send + Sync>> {
    let mut expr = parse_and(tokens, pos)?;

    while *pos < tokens.len() && tokens[*pos] == "OR" {
        *pos += 1;
        let right = parse_and(tokens, pos)?;
        expr = Expression::Or(Box::new(expr), Box::new(right));
    }

    Ok(expr)
}

pub fn parse_and(
    tokens: &[String],
    pos: &mut usize,
) -> Result<Expression, Box<dyn Error + Send + Sync>> {
    let mut expr = parse_not(tokens, pos)?;

    while *pos < tokens.len() && tokens[*pos] == "AND" {
        *pos += 1;
        let right = parse_not(tokens, pos)?;
        expr = Expression::And(Box::new(expr), Box::new(right));
    }

    Ok(expr)
}

pub fn parse_not(
    tokens: &[String],
    pos: &mut usize,
) -> Result<Expression, Box<dyn Error + Send + Sync>> {
    if *pos < tokens.len() && tokens[*pos] == "NOT" {
        *pos += 1;
        let expr = parse_primary(tokens, pos)?;
        return Ok(Expression::Not(Box::new(expr)));
    }

    parse_primary(tokens, pos)
}

pub fn parse_primary(
    tokens: &[String],
    pos: &mut usize,
) -> Result<Expression, Box<dyn Error + Send + Sync>> {
    if *pos >= tokens.len() {
        return Err("Unexpected end of input".into());
    }

    if tokens[*pos] == "(" {
        *pos += 1;
        let expr = parse_or(tokens, pos)?;
        if *pos >= tokens.len() || tokens[*pos] != ")" {
            return Err("Expected closing parenthesis".into());
        }
        *pos += 1;
        return Ok(expr);
    }

    // Parse condition
    let column_name = tokens[*pos].clone();
    *pos += 1;

    if *pos >= tokens.len() {
        return Err("Expected comparison operator".into());
    }

    let comparison = Comparison::from_str(&tokens[*pos]).ok_or("Invalid comparison operator")?;
    *pos += 1;

    if *pos >= tokens.len() {
        return Err("Expected threshold value".into());
    }

    let threshold_token = &tokens[*pos];
    *pos += 1;

    let threshold = if let Ok(datetime) = parse_iso_datetime(threshold_token) {
        ThresholdValue::Int64(datetime)
    } else if let Ok(bool) = threshold_token.parse::<bool>() {
        ThresholdValue::Boolean(bool)
    } else if threshold_token.contains('.') {
        if let Ok(num) = threshold_token.parse::<f64>() {
            ThresholdValue::Float64(num)
        } else {
            ThresholdValue::Utf8String(threshold_token.to_owned())
        }
    } else if let Ok(num) = threshold_token.parse::<i64>() {
        ThresholdValue::Int64(num)
    } else if let Ok(datetime) = parse_iso_datetime(threshold_token) {
        ThresholdValue::Int64(datetime)
    } else {
        ThresholdValue::Utf8String(threshold_token.to_owned())
    };

    Ok(Expression::Condition(Condition {
        column_name,
        comparison,
        threshold,
    }))
}

pub fn parse_iso_datetime(s: &str) -> Result<i64, chrono::ParseError> {
    let naive_date_time = NaiveDateTime::parse_from_str(s, "%Y-%m-%d-%H:%M:%S")?;
    let utc_date_time: DateTime<Utc> = Utc.from_utc_datetime(&naive_date_time);
    let timestamp = utc_date_time.timestamp_millis();
    println!("Parsed timestamp: {}", timestamp);
    Ok(timestamp)
}

pub struct CountingReader<R> {
    inner: R,
    bytes_read: Arc<AtomicUsize>,
}

impl<R> CountingReader<R> {
    pub fn new(inner: R, bytes_read: Arc<AtomicUsize>) -> Self {
        Self { inner, bytes_read }
    }

    pub fn bytes_read(&self) -> usize {
        self.bytes_read.load(Ordering::Relaxed)
    }
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.bytes_read.fetch_add(n, Ordering::Relaxed);
        Ok(n)
    }
}

impl<R: Seek> Seek for CountingReader<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}
