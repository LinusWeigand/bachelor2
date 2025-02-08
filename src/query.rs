use std::{
    error::Error,
    path::PathBuf,
    pin::Pin,
    sync::{atomic::AtomicUsize, Arc},
};

use arrow::{
    array::{Array, Float64Array, Int64Array, RecordBatch},
    datatypes::{DataType, Field},
};
use parquet::arrow::{
    arrow_reader::{ArrowPredicate, ArrowPredicateFn, ArrowReaderMetadata, RowFilter},
    ParquetRecordBatchStreamBuilder, ProjectionMask,
};

use crate::{
    aggregation::{build_aggregator, Aggregation, Aggregator, ScalarValue},
    bloom_filter::BloomFilter,
    row_filter,
    row_group_filter::keep_row_group,
    utils::{self, Expression},
    Mode,
};

pub struct MetadataEntry {
    pub file_path: PathBuf,
    pub metadata: ArrowReaderMetadata,
    pub bloom_filters: Vec<Vec<Option<BloomFilter>>>,
}

pub async fn smart_query_parquet(
    file_path: &PathBuf,
    metadata: ArrowReaderMetadata,
    expression: Option<Expression>,
    select_columns: Option<Vec<String>>,
    aggregations: Option<Vec<Aggregation>>,
    mode: &Mode,
) -> Result<(Vec<RecordBatch>, Arc<AtomicUsize>), Box<dyn Error + Send + Sync>> {
    let file = tokio::fs::File::open(file_path).await?;
    let bytes_read = Arc::new(AtomicUsize::new(0));
    let counting_file = utils::CountingReader::new(file, bytes_read.clone());
    let parquet_metadata = metadata.metadata();
    let name_to_index = utils::get_column_name_to_index(&parquet_metadata.file_metadata());

    let mask_opt = if let Some(select_columns) = select_columns {
        let column_indices: Vec<usize> = select_columns
            .iter()
            .filter_map(|column| name_to_index.get(column).map(|&x| x))
            .collect();

        if mode == &Mode::Column || mode == &Mode::Aggr {
            let mask = ProjectionMask::roots(
                parquet_metadata.file_metadata().schema_descr(),
                column_indices,
            );
            Some(mask)
        } else {
            None
        }
    } else {
        None
    };

    let mut row_groups_opt = None;
    let mut row_filter_opt = None;
    if let Some(expression) = expression {
        // Filter Row Groups
        if mode != &Mode::Base {
            let mut row_groups: Vec<usize> = Vec::new();
            for i in 0..parquet_metadata.num_row_groups() {
                let row_group_metadata = parquet_metadata.row_group(i);
                let is_keep = keep_row_group(row_group_metadata, &None, &expression, false, &mode)?;
                if is_keep {
                    row_groups.push(i);
                }
            }
            row_groups_opt = Some(row_groups);
        }

        // Filter Rows
        if mode == &Mode::Row || mode == &Mode::Column || mode == &Mode::Aggr {
            let filter_columns: Vec<String> =
                utils::get_column_projection_from_expression(&expression);
            let column_indices: Vec<usize> = filter_columns
                .iter()
                .filter_map(|column| name_to_index.get(column).map(|&x| x))
                .collect();
            let projection = ProjectionMask::roots(
                parquet_metadata.file_metadata().schema_descr(),
                column_indices,
            );
            let predicate: Box<dyn ArrowPredicate> = Box::new(ArrowPredicateFn::new(
                projection,
                row_filter::predicate_function(expression),
            ));
            let predicates: Vec<Box<dyn ArrowPredicate>> = vec![predicate];
            let row_filter: RowFilter = RowFilter::new(predicates);
            row_filter_opt = Some(row_filter);
        }
    }

    let mut builder = ParquetRecordBatchStreamBuilder::new_with_metadata(counting_file, metadata);
    if let Some(mask) = mask_opt {
        builder = builder.with_projection(mask);
    }
    if let Some(row_groups) = row_groups_opt {
        builder = builder.with_row_groups(row_groups);
    }
    if let Some(row_filter) = row_filter_opt {
        builder = builder.with_row_filter(row_filter);
    }
    builder = builder.with_batch_size(64_000);

    let mut stream = builder.build()?;

    let mut pinned_stream = Pin::new(&mut stream);

    let record_batch = utils::get_next_item_from_reader_and_count_bytes(&mut pinned_stream).await;
    if record_batch.is_none() {
        return Ok((Vec::new(), bytes_read));
    }
    let record_batch = record_batch.unwrap();
    let mut result = Vec::new();

    // Aggregation
    if mode == &Mode::Aggr {
        if let Some(aggregations) = aggregations {
            let mut aggregators: Vec<Option<Box<dyn Aggregator>>> = Vec::new();
            for aggregation in aggregations {
                let column_name = aggregation.column_name;
                let aggregation_op = aggregation.aggregation_op;
                if let Some(column_index) = name_to_index.get(&column_name) {
                    let column = record_batch.column(*column_index);
                    let data_type = column.data_type().clone();
                    aggregators.push(build_aggregator(
                        *column_index,
                        column_name,
                        aggregation_op,
                        data_type,
                    ));
                } else {
                    return Err(format!("Could not find column with name: {}", column_name).into());
                }
            }
            utils::aggregate_batch(&mut aggregators, &record_batch)?;
            while let Some(record_batch) =
                utils::get_next_item_from_reader_and_count_bytes(&mut pinned_stream).await
            {
                utils::aggregate_batch(&mut aggregators, &record_batch)?;
            }
            let mut aggregation_values: Vec<Arc<dyn Array>> = Vec::new();
            let mut aggregation_fields = Vec::new();
            for aggregator in aggregators {
                if let Some(aggregator) = aggregator.as_ref() {
                    let name = aggregator.get_name();
                    match aggregator.get_result() {
                        ScalarValue::Int128(v) => {
                            let value: i64 = match v.try_into() {
                                Ok(v) => v,
                                Err(_) => {
                                    return Err(format!(
                                        "Could not cast aggregation result to Int64: {}",
                                        &name
                                    )
                                    .into())
                                }
                            };
                            let value = Arc::new(Int64Array::from(vec![value]));
                            aggregation_values.push(value);
                            let field = Field::new(name, DataType::Int64, false);
                            aggregation_fields.push(field);
                        }
                        ScalarValue::Float64(v) => {
                            let value: f64 = match v.try_into() {
                                Ok(v) => v,
                                Err(_) => {
                                    return Err(format!(
                                        "Could not cast aggregation result to Float64: {}",
                                        &name
                                    )
                                    .into())
                                }
                            };
                            let value = Arc::new(Float64Array::from(vec![value]));
                            aggregation_values.push(value);
                            let field = Field::new(name, DataType::Float64, false);
                            aggregation_fields.push(field);
                        }
                        _ => return Err("Aggregation Type not supported".into()),
                    }

                    println!("Result: {:?}", result);
                }
            }
            // TODO: Stream to client;
            return Ok((result, bytes_read));
        }
    }

    result.push(record_batch);
    while let Some(record_batch) =
        utils::get_next_item_from_reader_and_count_bytes(&mut pinned_stream).await
    {
        //TODO: Stream to client;
        result.push(record_batch);
    }
    Ok((result, bytes_read))
}
