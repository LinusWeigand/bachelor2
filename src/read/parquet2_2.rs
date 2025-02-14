use arrow2::io::parquet::read::{infer_schema, FileReader};
use arrow2::types::NativeType;
use arrow2::{
    array::{
        Array, BooleanArray, Float16Array, PrimitiveArray, Utf8Array
    }, chunk::Chunk, compute::{cast::CastOptions, comparison::{boolean, primitive, utf8}}, datatypes::DataType, error::Error as ArrowError
};
use arrow2::compute;
use std::{collections::HashMap, error::Error, marker::PhantomData, cmp::{max, min}};
use arrow2::datatypes::Schema;
use parquet2::statistics::{
    BinaryStatistics, BooleanStatistics, FixedLenStatistics, PrimitiveStatistics, Statistics,
};
use parquet2::metadata::RowGroupMetaData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use std::{env, process::exit};
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
                Ok::<Arc<AtomicUsize>, Box<dyn Error + Send + Sync>>(bytes_read)
            });
        tasks.push(task);
    }

    let mut bytes_read = 0;
    for t in tasks {
        let bytes = t.await??;
        bytes_read += bytes.load(std::sync::atomic::Ordering::Relaxed);
    }

    let elapsed = start_time.elapsed();
    let size = 0.858 * std::cmp::min(count, FILE_PATHS.len()) as f64;
    let seconds = elapsed.as_millis() as f64 / 1000.;
    let tp = size / seconds;
    let bytes_read_gb: f64 = bytes_read as f64 / 1000. / 1000. / 1000.;
    println!("Bytes read: {:?} GB", bytes_read_gb);
    println!("Time: {:.2}s", seconds);
    println!("Throughput: {:02}MB", tp);

    Ok(())
}

async fn make_query(
    raw_footer: RawFooter,
    read_size: usize,
) -> Result<Arc<AtomicUsize>, Box<dyn Error + Send + Sync>> {
    // Query
    let expression = parse_expression("memoryUsed > 97525233984")?;
    let aggregation_str = vec!["SUM(Age)", "AVG(Age)", "MIN(Age)", "MAX(Age)", "COUNT(Age)", "SUM(Float)", "AVG(Float)", "MIN(Float)", "MAX(Float)", "COUNT(Float)"];
    let mut aggregations = Vec::new();
    for a in aggregation_str {
        let aggregation = parse_aggregation(a)?;
        aggregations.push(aggregation);
    }

    let select_columns = vec!["memoryUsed".to_owned()];

    let metadata = parse_raw_footer(&raw_footer.raw_bytes, raw_footer.footer_size)?;
    let mut schema = infer_schema(&metadata)?;
    let mut name_to_index = get_column_name_to_index(&schema);
    let row_groups = metadata.row_groups;
    let path = raw_footer.path;

    let file = std::fs::File::open(&path)?;
    let bytes_read = Arc::new(AtomicUsize::new(0));
    let counting_file = CountingReader::new(file, bytes_read.clone());

    // Early Projection
    let mut early_select = select_columns.clone();
    let filter_col_names = get_column_projection_from_expression(&expression);
    for col_name in filter_col_names {
        if !early_select.contains(&col_name) {
            early_select.push(col_name);
        }
    }
    let aggr_col_names = get_column_projection_from_aggregations(&aggregations);
    for col_name in aggr_col_names {
        if !early_select.contains(&col_name) {
            early_select.push(col_name);
        }
    }
    let num_fields_before = &schema.fields.len();
    schema = schema.filter(|_, field| early_select.contains(&field.name));
    let num_fields_after = &schema.fields.len();
    println!("Before: {}, After {}", num_fields_before, num_fields_after);
    name_to_index = get_column_name_to_index(&schema);
    // Row Group Filter
    let mut row_groups = row_groups;
    row_groups = row_groups
        .into_iter()
        .filter_map(
            |md| match keep_row_group(&md, &expression, false) {
                Ok(false) => None,
                Ok(true) | _ => Some(md),
            },
        )
        .collect();

    // Aggregation
    let mut aggregators = Vec::new();
    for aggregation in aggregations {
        let column_name = aggregation.column_name.clone();
        let aggregation_op = aggregation.aggregation_op.clone();

        let column = match schema
            .fields
            .iter()
            .find(|field| field.name == column_name)
        {
            Some(v) => v,
            None => continue,
        };

        let column_index = match name_to_index.get(&column_name) {
            Some(v) => *v,
            None => continue,
        };
        let data_type = column.data_type();
        aggregators.push(build_aggregator(
            column_index,
            column_name,
            aggregation_op,
            data_type,
        ));
    }

    let reader = FileReader::new(
        counting_file,
        row_groups,
        schema.clone(),
        Some(read_size),
        None,
        None,
    );
    for maybe_batch in reader {
        let mut batch = maybe_batch?;
        let mask = build_filter_mask(&batch, &expression, &name_to_index)?;
        batch = arrow2::compute::filter::filter_chunk(&batch, &mask)?;

        aggregate_batch(&mut aggregators, &batch)?;

        // Late Projection
        if select_columns.len() < schema.fields.len() {
            let selected_indices: Vec<usize> = schema.fields.iter().enumerate().filter_map(|(i, field)| {

            match select_columns.contains(&field.name) {
                false => None,
                true => Some(i)
            }}).collect();
            batch = filter_columns(&batch, &selected_indices);
        }

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

pub fn keep_row_group(
    row_group_metadata: &RowGroupMetaData,
    expression: &Expression,
    not: bool,
) -> Result<bool, Box<dyn Error + Send + Sync>> {
    match expression {
        Expression::Condition(condition) => {
            if let Some((column_index, column, _)) = row_group_metadata
                .columns()
                .iter()
                .enumerate()
                .filter_map(|(i, c)| match c.descriptor().path_in_schema.first() {
                    Some(v) => Some((i, c, v)),
                    None => None,
                })
                .find(|(_, _, column_name)| *column_name == &condition.column_name)
            {
                let stats = match column.statistics() {
                    Some(Ok(v)) => v,
                    _ => return Ok(true),
                };
                let (min_value, max_value) = match get_min_max_threshold(&stats) {
                    Some((min, max)) => (min, max),
                    _ => return Ok(true),
                };

                let mut result = condition.comparison.keep_row_group(
                    &min_value,
                    &max_value,
                    &condition.threshold,
                    not,
                );

                return Ok(result);
            }
            Ok(true)
        }
        Expression::And(left, right) => Ok(match not {
            true => {
                keep_row_group(row_group_metadata, left, true)?
                    || keep_row_group(row_group_metadata, right, true)?
            }
            false => {
                keep_row_group(row_group_metadata, left, false)?
                    && keep_row_group(row_group_metadata, right, false)?
            }
        }),
        Expression::Or(left, right) => Ok(match not {
            true => {
                keep_row_group(row_group_metadata, left, true)?
                    && keep_row_group(row_group_metadata, right, true)?
            }
            false => {
                keep_row_group(row_group_metadata, left, false)?
                    || keep_row_group(row_group_metadata, right, false)?
            }
        }),
        Expression::Not(inner) => Ok(keep_row_group(
            row_group_metadata,
            inner,
            !not,
        )?),
    }
}
pub fn compare<T: Ord>(min: T, max: T, v: T, comparison: &Comparison, not: bool) -> bool {
    match comparison {
        Comparison::LessThan => match not {
            false => min < v,
            true => max >= v,
        },
        Comparison::LessThanOrEqual => match not {
            false => min <= v,
            true => max > v,
        },
        Comparison::Equal => match not {
            false => v >= min && v <= max,
            true => !(v == min && v == max),
        },
        Comparison::GreaterThanOrEqual => match not {
            false => max >= v,
            true => min < v,
        },
        Comparison::GreaterThan => match not {
            false => max > v,
            true => min <= v,
        },
    }
}

pub trait Float: Copy + PartialOrd {
    fn abs(self) -> Self;
    fn equal(self, other: Self) -> bool;
}

impl Float for f32 {
    fn abs(self) -> Self {
        self.abs()
    }
    fn equal(self, other: Self) -> bool {
        (self - other).abs() < f32::EPSILON
    }
}

impl Float for f64 {
    fn abs(self) -> Self {
        self.abs()
    }
    fn equal(self, other: Self) -> bool {
        (self - other).abs() < f64::EPSILON
    }
}

pub fn compare_floats<T: Float>(
    min: T,
    max: T,
    v: T,
    comparison: &Comparison,
    not: bool,
) -> bool {
    match comparison {
        Comparison::LessThan => match not {
            false => min < v,
            true => max >= v,
        },
        Comparison::LessThanOrEqual => match not {
            false => min <= v,
            true => max > v,
        },
        Comparison::Equal => match not {
            false => v >= min && v <= max,
            true => !(v.equal(min) && v.equal(max)),
        },
        Comparison::GreaterThanOrEqual => match not {
            false => max >= v,
            true => min < v,
        },
        Comparison::GreaterThan => match not {
            false => max > v,
            true => min <= v,
        },
    }
}

impl Comparison {
    pub fn keep_row_group(
        &self,
        row_group_min: &ThresholdValue,
        row_group_max: &ThresholdValue,
        user_threshold: &ThresholdValue,
        not: bool,
    ) -> bool {
        match (row_group_min, row_group_max, user_threshold) {
            (ThresholdValue::Int64(min), ThresholdValue::Int64(max), ThresholdValue::Int64(v)) => {
                compare(min, max, v, self, not)
            }
            (
                ThresholdValue::Float64(min),
                ThresholdValue::Float64(max),
                ThresholdValue::Float64(v),
            ) => compare_floats(*min, *max, *v, self, not),
            (
                ThresholdValue::Boolean(min),
                ThresholdValue::Boolean(max),
                ThresholdValue::Boolean(v),
            ) => match self {
                Comparison::LessThan => true,
                Comparison::LessThanOrEqual => true,
                Comparison::Equal => match not {
                    false => v == min || v == max,
                    true => !(v == min && v == max),
                },
                Comparison::GreaterThanOrEqual => true,
                Comparison::GreaterThan => true,
            },
            (
                ThresholdValue::Utf8String(min),
                ThresholdValue::Utf8String(max),
                ThresholdValue::Utf8String(v),
            ) => compare(min, max, v, self, not),
            _ => true,
        }
    }
}

pub fn get_min_max_threshold(
    stats: &Arc<dyn Statistics>,
) -> Option<(ThresholdValue, ThresholdValue)> {
    if let Some(typed_stats) = stats.as_any().downcast_ref::<BinaryStatistics>() {
        let min_str = String::from_utf8(typed_stats.min_value.clone()?).ok()?;
        let max_str = String::from_utf8(typed_stats.max_value.clone()?).ok()?;
        return Some((
            ThresholdValue::Utf8String(min_str),
            ThresholdValue::Utf8String(max_str),
        ));
    }

    if let Some(typed_stats) = stats.as_any().downcast_ref::<BooleanStatistics>() {
        return Some((
            ThresholdValue::Boolean(typed_stats.min_value?),
            ThresholdValue::Boolean(typed_stats.max_value?),
        ));
    }

    if let Some(typed_stats) = stats.as_any().downcast_ref::<FixedLenStatistics>() {
        let min_str = String::from_utf8(typed_stats.min_value.clone()?).ok()?;
        let max_str = String::from_utf8(typed_stats.max_value.clone()?).ok()?;
        return Some((
            ThresholdValue::Utf8String(min_str),
            ThresholdValue::Utf8String(max_str),
        ));
    }

    if let Some(typed_stats) = stats.as_any().downcast_ref::<PrimitiveStatistics<i64>>() {
        return Some((
            ThresholdValue::Int64(typed_stats.min_value?),
            ThresholdValue::Int64(typed_stats.max_value?),
        ));
    }
    if let Some(typed_stats) = stats.as_any().downcast_ref::<PrimitiveStatistics<i32>>() {
        return Some((
            ThresholdValue::Int64(typed_stats.min_value? as i64),
            ThresholdValue::Int64(typed_stats.max_value? as i64),
        ));
    }
    if let Some(typed_stats) = stats.as_any().downcast_ref::<PrimitiveStatistics<f32>>() {
        return Some((
            ThresholdValue::Float64(typed_stats.min_value? as f64),
            ThresholdValue::Float64(typed_stats.max_value? as f64),
        ));
    }
    if let Some(typed_stats) = stats.as_any().downcast_ref::<PrimitiveStatistics<f64>>() {
        return Some((
            ThresholdValue::Float64(typed_stats.min_value?),
            ThresholdValue::Float64(typed_stats.max_value?),
        ));
    }
    println!("No Downcast :(");

    None
}

pub fn get_column_name_to_index(schema: &Schema) -> HashMap<String, usize> {
    schema
        .fields
        .iter()
        .enumerate()
        .map(|(i, field)| (field.name.clone(), i))
        .collect()
}


pub fn build_filter_mask(
    chunk: &Chunk<Box<dyn Array>>,
    expression: &Expression,
    name_to_index: &HashMap<String, usize>,
) -> Result<BooleanArray, ArrowError> {
    let cast_options = CastOptions {
        wrapped: true,
        partial: true,
    };
    match expression {
        Expression::Condition(cond) => {
            let col_index = *name_to_index
                .get(&cond.column_name)
                .ok_or_else(|| ArrowError::InvalidArgumentError(
                    format!("Column '{}' not found", &cond.column_name)
                ))?;
            let array = &chunk.columns()[col_index];
            let data_type = array.data_type();
            let comparison = &cond.comparison;

            let bool_arr = match (data_type, &cond.threshold) {
                (DataType::Int8, ThresholdValue::Int64(v)) => {
                    let int_array = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i8>>()
                        .ok_or_else(|| downcast_err("Int8"))?;
                    let int_array = compute::cast::cast(int_array, &DataType::Int64, cast_options).map_err(|_| {
                        downcast_err("Int8")
                    })?;
                    let int_array = int_array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i64>>()
                        .ok_or_else(|| downcast_err("Int64"))?;
                    match comparison {
                        Comparison::Equal => primitive::eq_scalar(int_array, *v),
                        Comparison::LessThan => primitive::lt_scalar(int_array, *v),
                        Comparison::LessThanOrEqual => primitive::lt_eq_scalar(int_array, *v),
                        Comparison::GreaterThan => primitive::gt_scalar(int_array, *v),
                        Comparison::GreaterThanOrEqual => primitive::gt_eq_scalar(int_array, *v),
                    }
                }
                (DataType::Int16, ThresholdValue::Int64(v)) => {
                    let int_array = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i16>>()
                        .ok_or_else(|| downcast_err("Int16"))?;
                    let int_array = compute::cast::cast(int_array, &DataType::Int64, cast_options).map_err(|_| {
                        downcast_err("Int16")
                    })?;
                    let int_array = int_array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i64>>()
                        .ok_or_else(|| downcast_err("Int64"))?;
                    match comparison {
                        Comparison::Equal => primitive::eq_scalar(int_array, *v),
                        Comparison::LessThan => primitive::lt_scalar(int_array, *v),
                        Comparison::LessThanOrEqual => primitive::lt_eq_scalar(int_array, *v),
                        Comparison::GreaterThan => primitive::gt_scalar(int_array, *v),
                        Comparison::GreaterThanOrEqual => primitive::gt_eq_scalar(int_array, *v),
                    }
                }
                (DataType::Int32, ThresholdValue::Int64(v)) => {
                    let int_array = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i32>>()
                        .ok_or_else(|| downcast_err("Int32"))?;
                    let int_array = compute::cast::cast(int_array, &DataType::Int64, cast_options).map_err(|_| {
                        downcast_err("Int32")
                    })?;
                    let int_array = int_array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i64>>()
                        .ok_or_else(|| downcast_err("Int64"))?;
                    match comparison {
                        Comparison::Equal => primitive::eq_scalar(int_array, *v),
                        Comparison::LessThan => primitive::lt_scalar(int_array, *v),
                        Comparison::LessThanOrEqual => primitive::lt_eq_scalar(int_array, *v),
                        Comparison::GreaterThan => primitive::gt_scalar(int_array, *v),
                        Comparison::GreaterThanOrEqual => primitive::gt_eq_scalar(int_array, *v),
                    }
                }
                (DataType::Int64, ThresholdValue::Int64(v)) => {
                    let int_array = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i64>>()
                        .ok_or_else(|| downcast_err("Int64"))?;
                    match comparison {
                        Comparison::Equal => primitive::eq_scalar(int_array, *v),
                        Comparison::LessThan => primitive::lt_scalar(int_array, *v),
                        Comparison::LessThanOrEqual => primitive::lt_eq_scalar(int_array, *v),
                        Comparison::GreaterThan => primitive::gt_scalar(int_array, *v),
                        Comparison::GreaterThanOrEqual => primitive::gt_eq_scalar(int_array, *v),
                    }
                }
                (DataType::UInt8, ThresholdValue::Int64(v)) => {
                    let int_array = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<u8>>()
                        .ok_or_else(|| downcast_err("UInt8"))?;
                    let int_array = compute::cast::cast(int_array, &DataType::Int64, cast_options).map_err(|_| {
                        downcast_err("UInt8")
                    })?;
                    let int_array = int_array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i64>>()
                        .ok_or_else(|| downcast_err("Int64"))?;
                    match comparison {
                        Comparison::Equal => primitive::eq_scalar(int_array, *v),
                        Comparison::LessThan => primitive::lt_scalar(int_array, *v),
                        Comparison::LessThanOrEqual => primitive::lt_eq_scalar(int_array, *v),
                        Comparison::GreaterThan => primitive::gt_scalar(int_array, *v),
                        Comparison::GreaterThanOrEqual => primitive::gt_eq_scalar(int_array, *v),
                    }
                }
                (DataType::UInt16, ThresholdValue::Int64(v)) => {
                    let int_array = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<u16>>()
                        .ok_or_else(|| downcast_err("UInt16"))?;
                    let int_array = compute::cast::cast(int_array, &DataType::Int64, cast_options).map_err(|_| {
                        downcast_err("UInt16")
                    })?;
                    let int_array = int_array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i64>>()
                        .ok_or_else(|| downcast_err("Int64"))?;
                    match comparison {
                        Comparison::Equal => primitive::eq_scalar(int_array, *v),
                        Comparison::LessThan => primitive::lt_scalar(int_array, *v),
                        Comparison::LessThanOrEqual => primitive::lt_eq_scalar(int_array, *v),
                        Comparison::GreaterThan => primitive::gt_scalar(int_array, *v),
                        Comparison::GreaterThanOrEqual => primitive::gt_eq_scalar(int_array, *v),
                    }
                }
                (DataType::UInt32, ThresholdValue::Int64(v)) => {
                    let int_array = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<u32>>()
                        .ok_or_else(|| downcast_err("UInt32"))?;
                    let int_array = compute::cast::cast(int_array, &DataType::Int64, cast_options).map_err(|_| {
                        downcast_err("UInt32")
                    })?;
                    let int_array = int_array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i64>>()
                        .ok_or_else(|| downcast_err("Int64"))?;
                    match comparison {
                        Comparison::Equal => primitive::eq_scalar(int_array, *v),
                        Comparison::LessThan => primitive::lt_scalar(int_array, *v),
                        Comparison::LessThanOrEqual => primitive::lt_eq_scalar(int_array, *v),
                        Comparison::GreaterThan => primitive::gt_scalar(int_array, *v),
                        Comparison::GreaterThanOrEqual => primitive::gt_eq_scalar(int_array, *v),
                    }
                }
                (DataType::UInt64, ThresholdValue::Int64(v)) => {
                    let int_array = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<u64>>()
                        .ok_or_else(|| downcast_err("UInt64"))?;
                    let int_array = compute::cast::cast(int_array, &DataType::Int64, cast_options).map_err(|_| {
                        downcast_err("UInt64")
                    })?;
                    let int_array = int_array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i64>>()
                        .ok_or_else(|| downcast_err("Int64"))?;
                    match comparison {
                        Comparison::Equal => primitive::eq_scalar(int_array, *v),
                        Comparison::LessThan => primitive::lt_scalar(int_array, *v),
                        Comparison::LessThanOrEqual => primitive::lt_eq_scalar(int_array, *v),
                        Comparison::GreaterThan => primitive::gt_scalar(int_array, *v),
                        Comparison::GreaterThanOrEqual => primitive::gt_eq_scalar(int_array, *v),
                    }
                }
                (DataType::Date32, ThresholdValue::Int64(v)) => {
                    let int_array = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i32>>()
                        .ok_or_else(|| downcast_err("Date32"))?;
                    let int_array = compute::cast::cast(int_array, &DataType::Int64, cast_options).map_err(|_| {
                        downcast_err("UInt64")
                    })?;
                    let int_array = int_array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i64>>()
                        .ok_or_else(|| downcast_err("Int64"))?;
                    match comparison {
                        Comparison::Equal => primitive::eq_scalar(int_array, *v),
                        Comparison::LessThan => primitive::lt_scalar(int_array, *v),
                        Comparison::LessThanOrEqual => primitive::lt_eq_scalar(int_array, *v),
                        Comparison::GreaterThan => primitive::gt_scalar(int_array, *v),
                        Comparison::GreaterThanOrEqual => primitive::gt_eq_scalar(int_array, *v),
                    }
                }
                (DataType::Date64, ThresholdValue::Int64(v)) => {
                    let int_array = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<i64>>()
                        .ok_or_else(|| downcast_err("Date64"))?;
                    match comparison {
                        Comparison::Equal => primitive::eq_scalar(int_array, *v),
                        Comparison::LessThan => primitive::lt_scalar(int_array, *v),
                        Comparison::LessThanOrEqual => primitive::lt_eq_scalar(int_array, *v),
                        Comparison::GreaterThan => primitive::gt_scalar(int_array, *v),
                        Comparison::GreaterThanOrEqual => primitive::gt_eq_scalar(int_array, *v),
                    }
                }
                (DataType::Float16, ThresholdValue::Float64(v)) => {
                    let int_array = array
                        .as_any()
                        .downcast_ref::<Float16Array>()
                        .ok_or_else(|| downcast_err("f16"))?;
                    let int_array = compute::cast::cast(int_array, &DataType::Float64, cast_options).map_err(|_| {
                        downcast_err("f16")
                    })?;
                    let int_array = int_array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<f64>>()
                        .ok_or_else(|| downcast_err("f64"))?;
                    match comparison {
                        Comparison::Equal => primitive::eq_scalar(int_array, *v),
                        Comparison::LessThan => primitive::lt_scalar(int_array, *v),
                        Comparison::LessThanOrEqual => primitive::lt_eq_scalar(int_array, *v),
                        Comparison::GreaterThan => primitive::gt_scalar(int_array, *v),
                        Comparison::GreaterThanOrEqual => primitive::gt_eq_scalar(int_array, *v),
                    }
                }
                (DataType::Float32, ThresholdValue::Float64(v)) => {
                    let int_array = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<f32>>()
                        .ok_or_else(|| downcast_err("f32"))?;
                    let int_array = compute::cast::cast(int_array, &DataType::Float64, cast_options).map_err(|_| {
                        downcast_err("f32")
                    })?;
                    let int_array = int_array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<f64>>()
                        .ok_or_else(|| downcast_err("f64"))?;
                    match comparison {
                        Comparison::Equal => primitive::eq_scalar(int_array, *v),
                        Comparison::LessThan => primitive::lt_scalar(int_array, *v),
                        Comparison::LessThanOrEqual => primitive::lt_eq_scalar(int_array, *v),
                        Comparison::GreaterThan => primitive::gt_scalar(int_array, *v),
                        Comparison::GreaterThanOrEqual => primitive::gt_eq_scalar(int_array, *v),
                    }
                }
                (DataType::Float64, ThresholdValue::Float64(v)) => {
                    let int_array = array
                        .as_any()
                        .downcast_ref::<PrimitiveArray<f64>>()
                        .ok_or_else(|| downcast_err("f64"))?;
                    match comparison {
                        Comparison::Equal => primitive::eq_scalar(int_array, *v),
                        Comparison::LessThan => primitive::lt_scalar(int_array, *v),
                        Comparison::LessThanOrEqual => primitive::lt_eq_scalar(int_array, *v),
                        Comparison::GreaterThan => primitive::gt_scalar(int_array, *v),
                        Comparison::GreaterThanOrEqual => primitive::gt_eq_scalar(int_array, *v),
                    }
                }
                (DataType::Boolean, ThresholdValue::Boolean(v)) => {
                    let bool_array = array
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| downcast_err("bool"))?;
                    match comparison {
                        Comparison::Equal => boolean::eq_scalar(bool_array, *v),
                        Comparison::LessThan => boolean::lt_scalar(bool_array, *v),
                        Comparison::LessThanOrEqual => boolean::lt_eq_scalar(bool_array, *v),
                        Comparison::GreaterThan => boolean::gt_scalar(bool_array, *v),
                        Comparison::GreaterThanOrEqual => boolean::gt_eq_scalar(bool_array, *v),
                    }
                }
                (DataType::Utf8, ThresholdValue::Utf8String(v)) => {
                    let utf8_array = array
                        .as_any()
                        .downcast_ref::<Utf8Array<i32>>()
                        .ok_or_else(|| downcast_err("utf8<i32>"))?;
                    match comparison {
                        Comparison::Equal => utf8::eq_scalar(utf8_array, v),
                        Comparison::LessThan => utf8::lt_scalar(utf8_array, v),
                        Comparison::LessThanOrEqual => utf8::lt_eq_scalar(utf8_array, v),
                        Comparison::GreaterThan => utf8::gt_scalar(utf8_array, v),
                        Comparison::GreaterThanOrEqual => utf8::gt_eq_scalar(utf8_array, v),
                    }
                }
                _ => {
                    return Err(ArrowError::InvalidArgumentError(
                        format!("Unsupported comparison/data_type for '{}'", cond.column_name)
                    ));
                }
            };
            Ok(bool_arr)
        }

        Expression::And(left, right) => {
            let left_mask = build_filter_mask(chunk, left, name_to_index)?;
            let right_mask = build_filter_mask(chunk, right, name_to_index)?;
            Ok(compute::boolean::and(&left_mask, &right_mask))
        }
        Expression::Or(left, right) => {
            let left_mask = build_filter_mask(chunk, left, name_to_index)?;
            let right_mask = build_filter_mask(chunk, right, name_to_index)?;
            Ok(compute::boolean::or(&left_mask, &right_mask))
        }
        Expression::Not(inner) => {
            let mask = build_filter_mask(chunk, inner, name_to_index)?;
            Ok(compute::boolean::not(&mask))
        }
    }
}

fn downcast_err(t: &str) -> ArrowError {
    ArrowError::InvalidArgumentError(format!("Could not downcast array to {}", t))
}
pub fn get_column_projection_from_expression(expression: &Expression) -> Vec<String> {
    let mut column_projection = Vec::new();

    fn get_column_projection(expr: &Expression, cols: &mut Vec<String>) {
        match expr {
            Expression::Condition(cond) => {
                if !cols.contains(&cond.column_name) {
                    cols.push(cond.column_name.clone());
                }
            }
            Expression::And(left, right) | Expression::Or(left, right) => {
                get_column_projection(left, cols);
                get_column_projection(right, cols);
            }
            Expression::Not(inner) => get_column_projection(inner, cols),
        }
    }

    get_column_projection(expression, &mut column_projection);
    column_projection
}

pub fn filter_columns(
    batch: &Chunk<Box<dyn Array>>,
    selected_indices: &[usize],
) -> Chunk<Box<dyn Array>> {
    let filtered_columns: Vec<_> = selected_indices
        .iter()
        .filter_map(|&index| batch.columns().get(index).cloned())
        .collect();

    Chunk::new(filtered_columns)
}

pub fn parse_aggregation(input: &str) -> Result<Aggregation, Box<dyn Error + Send + Sync>> {
    let tokens = tokenize(input)?;
    let aggregation_op = match tokens[0].as_str() {
        "SUM" => AggregationOp::SUM,
        "AVG" => AggregationOp::AVG,
        "COUNT" => AggregationOp::COUNT,
        "MIN" => AggregationOp::MIN,
        "MAX" => AggregationOp::MAX,
        _ => {
            return Err(format!("Invalid Operation: {}", tokens[0]).into());
        }
    };

    if tokens[1] != "(" || tokens[3] != ")" {
        return Err("Expected format: SUM(column_name)".into());
    }

    let column_name = (&tokens[2]).to_owned();

    Ok(Aggregation {
        column_name,
        aggregation_op,
    })
}

#[derive(Debug, Clone)]
pub enum ScalarValue {
    Null,
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    Boolean(bool),
    String(String),
    USize(usize),
    Date(NaiveDateTime),
}

pub trait Aggregator: Send + Sync {
    fn aggregate_batch(&mut self, batch: &Chunk<Box<dyn Array>>) -> Result<(), ArrowError>;
    fn get_result(&self) -> ScalarValue;
    fn get_name(&self) -> String;
}

#[derive(PartialEq, Clone, Debug)]
pub enum AggregationOp {
    SUM,
    AVG,
    COUNT,
    MIN,
    MAX,
}

#[derive(Debug, Clone)]
pub struct Aggregation {
    pub column_name: String,
    pub aggregation_op: AggregationOp,
}

impl std::fmt::Display for AggregationOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                AggregationOp::SUM => "SUM",
                AggregationOp::AVG => "AVG",
                AggregationOp::COUNT => "COUNT",
                AggregationOp::MIN => "MIN",
                AggregationOp::MAX => "MAX",
            }
        )
    }
}

pub fn build_aggregator(
    column_index: usize,
    column_name: String,
    aggregation_op: AggregationOp,
    data_type: &DataType,
) -> Option<Box<dyn Aggregator>> {
    let aggregation_op = aggregation_op.clone();
    match data_type {
        DataType::Int8 => Some(Box::new(IntegerAggregator::<i8>::new(
            column_index,
            column_name,
            aggregation_op,
        ))),
        DataType::Int16 => Some(Box::new(IntegerAggregator::<i16>::new(
            column_index,
            column_name,
            aggregation_op,
        ))),
        DataType::Int32 => Some(Box::new(IntegerAggregator::<i32>::new(
            column_index,
            column_name,
            aggregation_op,
        ))),
        DataType::Int64 => Some(Box::new(IntegerAggregator::<i64>::new(
            column_index,
            column_name,
            aggregation_op,
        ))),
        DataType::UInt8 => Some(Box::new(UIntegerAggregator::<u8>::new(
            column_index,
            column_name,
            aggregation_op,
        ))),
        DataType::UInt16 => Some(Box::new(UIntegerAggregator::<u16>::new(
            column_index,
            column_name,
            aggregation_op,
        ))),
        DataType::UInt32 => Some(Box::new(UIntegerAggregator::<u32>::new(
            column_index,
            column_name,
            aggregation_op,
        ))),
        DataType::UInt64 => Some(Box::new(UIntegerAggregator::<u64>::new(
            column_index,
            column_name,
            aggregation_op,
        ))),
        DataType::Float32 => Some(Box::new(FloatAggregator::<f32>::new(
            column_index,
            column_name,
            aggregation_op,
        ))),
        DataType::Float64 => Some(Box::new(FloatAggregator::<f64>::new(
            column_index,
            column_name,
            aggregation_op,
        ))),
        _ => None,
    }
}

pub struct IntegerAggregator<T> {
    column_index: usize,
    column_name: String,
    aggregation_op: AggregationOp,
    sum: i64,
    count: usize,
    min: i64,
    max: i64,

    // marker for compiler that we need T but not at runtime
    phantom: PhantomData<fn() -> T>,
}

pub struct UIntegerAggregator<T> {
    column_index: usize,
    column_name: String,
    aggregation_op: AggregationOp,
    sum: u64,
    count: usize,
    min: u64,
    max: u64,

    // marker for compiler that we need T but not at runtime
    phantom: PhantomData<fn() -> T>,
}

pub struct FloatAggregator<T> {
    column_index: usize,
    column_name: String,
    aggregation_op: AggregationOp,
    sum: f64,
    count: usize,
    min: f64,
    max: f64,

    phantom: PhantomData<fn() -> T>,
}

impl<T> IntegerAggregator<T>
where
    T: NativeType + Into<i64>,
{
    pub fn new(column_index: usize, column_name: String, aggregation_op: AggregationOp) -> Self {
        Self {
            column_index,
            column_name,
            aggregation_op,
            sum: 0,
            count: 0,
            min: i64::MAX,
            max: i64::MIN,
            phantom: PhantomData,
        }
    }

    fn process_value(&mut self, v: i64) {
        match self.aggregation_op {
            AggregationOp::SUM | AggregationOp::AVG => {
                self.sum += v;
                self.count += 1;
            }
            AggregationOp::COUNT => {
                self.count += 1;
            }
            AggregationOp::MIN => {
                self.min = min(self.min, v);
                self.count += 1;
            }
            AggregationOp::MAX => {
                self.max = max(self.max, v);
                self.count += 1;
            }
        }
    }
}

impl<T> UIntegerAggregator<T>
where
    T: NativeType + Into<u64>,
{
    pub fn new(column_index: usize, column_name: String, aggregation_op: AggregationOp) -> Self {
        Self {
            column_index,
            column_name,
            aggregation_op,
            sum: 0,
            count: 0,
            min: u64::MAX,
            max: u64::MIN,
            phantom: PhantomData,
        }
    }

    fn process_value(&mut self, v: u64) {
        match self.aggregation_op {
            AggregationOp::SUM | AggregationOp::AVG => {
                self.sum += v;
                self.count += 1;
            }
            AggregationOp::COUNT => {
                self.count += 1;
            }
            AggregationOp::MIN => {
                self.min = min(self.min, v);
                self.count += 1;
            }
            AggregationOp::MAX => {
                self.max = max(self.max, v);
                self.count += 1;
            }
        }
    }
}

impl<T> FloatAggregator<T>
where
    T: NativeType + Into<f64>,
{
    pub fn new(column_index: usize, column_name: String, aggregation_op: AggregationOp) -> Self {
        Self {
            column_index,
            column_name,
            aggregation_op,
            sum: 0.,
            count: 0,
            min: f64::MAX,
            max: f64::MIN,
            phantom: PhantomData,
        }
    }

    pub fn process_value(&mut self, v: f64) {
        match self.aggregation_op {
            AggregationOp::SUM | AggregationOp::AVG => {
                self.sum += v;
                self.count += 1;
            }
            AggregationOp::COUNT => {
                self.count += 1;
            }
            AggregationOp::MIN => {
                self.min = self.min.min(v);
                self.count += 1;
            }
            AggregationOp::MAX => {
                self.max = self.max.max(v);
                self.count += 1;
            }
        }
    }
}

impl<T> Aggregator for IntegerAggregator<T>
where
    T: NativeType + Into<i64>,
{
    fn aggregate_batch(&mut self, batch: &Chunk<Box<dyn Array>>) -> Result<(), ArrowError> {
        let column = batch.columns().get(self.column_index).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "Column index {} out of bounds",
                self.column_index
            ))
        })?;
        let array = column
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!(
                    "Downcast to PrimitiveArray<{}> failed!",
                    std::any::type_name::<T>()
                ))
            })?;

        for val in array.iter().flatten() {
            let value: i64 = (*val).into();
            self.process_value(value);
        }
        Ok(())
    }

    fn get_result(&self) -> ScalarValue {
        match self.aggregation_op {
            AggregationOp::SUM => ScalarValue::Int64(self.sum),
            AggregationOp::AVG => {
                if self.count == 0 {
                    ScalarValue::Null
                } else {
                    ScalarValue::Float64(self.sum as f64 / self.count as f64)
                }
            }
            AggregationOp::COUNT => ScalarValue::USize(self.count),
            AggregationOp::MIN => {
                if self.count == 0 {
                    ScalarValue::Null
                } else {
                    ScalarValue::Int64(self.min)
                }
            }
            AggregationOp::MAX => {
                if self.count == 0 {
                    ScalarValue::Null
                } else {
                    ScalarValue::Int64(self.max)
                }
            }
        }
    }

    fn get_name(&self) -> String {
        format!("{}({})", self.aggregation_op.to_string(), self.column_name)
    }
}

impl<T> Aggregator for UIntegerAggregator<T>
where
    T: NativeType + Into<u64>,
{
    fn aggregate_batch(&mut self, batch: &Chunk<Box<dyn Array>>) -> Result<(), ArrowError> {
        let column = batch.columns().get(self.column_index).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "Column index {} out of bounds",
                self.column_index
            ))
        })?;
        let array = column
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!(
                    "Downcast to PrimitiveArray<{}> failed!",
                    std::any::type_name::<T>()
                ))
            })?;

        for val in array.iter().flatten() {
            let value: u64 = (*val).into();
            self.process_value(value);
        }
        Ok(())
    }

    fn get_result(&self) -> ScalarValue {
        match self.aggregation_op {
            AggregationOp::SUM => ScalarValue::UInt64(self.sum),
            AggregationOp::AVG => {
                if self.count == 0 {
                    ScalarValue::Null
                } else {
                    ScalarValue::Float64(self.sum as f64 / self.count as f64)
                }
            }
            AggregationOp::COUNT => ScalarValue::USize(self.count),
            AggregationOp::MIN => {
                if self.count == 0 {
                    ScalarValue::Null
                } else {
                    ScalarValue::UInt64(self.min)
                }
            }
            AggregationOp::MAX => {
                if self.count == 0 {
                    ScalarValue::Null
                } else {
                    ScalarValue::UInt64(self.max)
                }
            }
        }
    }

    fn get_name(&self) -> String {
        format!("{}({})", self.aggregation_op.to_string(), self.column_name)
    }
}

impl<T> Aggregator for FloatAggregator<T>
where
    T: NativeType + Into<f64>,
{
    fn aggregate_batch(&mut self, batch: &Chunk<Box<dyn Array>>) -> Result<(), ArrowError> {
        let column = batch.columns().get(self.column_index).ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!(
                "Column index {} out of bounds",
                self.column_index
            ))
        })?;
        let array = column
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!(
                    "Downcast to PrimitiveArray<{}> failed!",
                    std::any::type_name::<T>()
                ))
            })?;

        for val in array.iter().flatten() {
            self.process_value((*val).into());
        }
        Ok(())
    }
    fn get_result(&self) -> ScalarValue {
        match self.aggregation_op {
            AggregationOp::SUM => ScalarValue::Float64(self.sum),
            AggregationOp::AVG => {
                if self.count == 0 {
                    ScalarValue::Null
                } else {
                    ScalarValue::Float64(self.sum / self.count as f64)
                }
            }
            AggregationOp::COUNT => ScalarValue::USize(self.count),
            AggregationOp::MIN => {
                if self.count > 0 {
                    ScalarValue::Float64(self.min)
                } else {
                    ScalarValue::Null
                }
            }
            AggregationOp::MAX => {
                if self.count > 0 {
                    ScalarValue::Float64(self.max)
                } else {
                    ScalarValue::Null
                }
            }
        }
    }

    fn get_name(&self) -> String {
        format!("{}({})", self.aggregation_op.to_string(), self.column_name)
    }
}

pub fn get_column_projection_from_aggregations(aggregations: &Vec<Aggregation>) -> Vec<String> {
    let mut result: Vec<String> = Vec::new();
    for aggregation in aggregations {
        let col_name = &aggregation.column_name;
        if !result.contains(&col_name) {
            result.push(col_name.to_owned());
        }
    }
    result
}

pub fn aggregate_batch(
    aggregators: &mut Vec<Option<Box<dyn Aggregator>>>,
    batch: &Chunk<Box<dyn Array>>,
) -> Result<(), ArrowError> {
    for aggregator in aggregators {
        if let Some(aggregator) = aggregator {
            aggregator.aggregate_batch(batch)?;
        }
    }
    Ok(())
}

