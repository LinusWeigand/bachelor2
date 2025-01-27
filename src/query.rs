use std::{error::Error, path::PathBuf, pin::Pin};

use arrow::{array::RecordBatch};
use parquet::{
    arrow::{
        arrow_reader::{ArrowPredicate, ArrowPredicateFn, ArrowReaderMetadata, RowFilter},
        ParquetRecordBatchStreamBuilder, ProjectionMask,
    },
};
use tokio::fs::File;

use crate::{aggregation::{build_aggregator, Aggregation, AggregationOp, Aggregator}, bloom_filter::BloomFilter, row_filter, row_group_filter::keep_row_group, utils::{self, Expression}};


pub struct MetadataEntry {
    pub file_path: PathBuf,
    pub metadata: ArrowReaderMetadata,
    pub bloom_filters: Vec<Vec<Option<BloomFilter>>>,
}

pub async fn smart_query_parquet(
    metadata_entry: &MetadataEntry,
    expression: Option<Expression>,
    select_columns: Option<Vec<String>>,
    aggregations: Option<Vec<Aggregation>>
) -> Result<Vec<RecordBatch>, Box<dyn Error>> {
    let file = File::open(&metadata_entry.file_path).await?;

    let metadata = metadata_entry.metadata.clone();
    let builder = ParquetRecordBatchStreamBuilder::new_with_metadata(file, metadata.clone());
    let metadata = metadata.metadata();
    let name_to_index = utils::get_column_name_to_index(&metadata.file_metadata());

    let mut stream = builder;

    if let Some(select_columns) = select_columns {
        
        let column_indices: Vec<usize> = select_columns
            .iter()
            .filter_map(|column| {
                name_to_index
                    .get(column)
                    .map(|&x| x)
            })
            .collect();

        let mask = ProjectionMask::roots(metadata.file_metadata().schema_descr(), column_indices);

        stream = stream.with_projection(mask);
    }

    if let Some(expression) = expression {
        // Filter Row Groups
        let mut row_groups: Vec<usize> = Vec::new();

        for i in 0..metadata.num_row_groups() {
            let row_group_metadata = metadata.row_group(i);
            let is_keep = keep_row_group(
                row_group_metadata,
                &metadata_entry.bloom_filters[i],
                &expression,
                false,
            )?;
            if is_keep {
                row_groups.push(i);
            }
        }

        stream = stream.with_row_groups(row_groups);

        // Filter Rows
        let filter_columns: Vec<String> = utils::get_column_projection_from_expression(&expression);
        let column_indices: Vec<usize> = filter_columns
            .iter()
            .filter_map(|column| name_to_index.get(column).map(|&x| x))
            .collect();
        let projection =
            ProjectionMask::roots(metadata.file_metadata().schema_descr(), column_indices);
        let predicate: Box<dyn ArrowPredicate> = Box::new(ArrowPredicateFn::new(
            projection,
            row_filter::predicate_function(expression),
        ));
        let predicates: Vec<Box<dyn ArrowPredicate>> = vec![predicate];
        let row_filter: RowFilter = RowFilter::new(predicates);
        stream = stream
        .with_row_filter(row_filter);
    }

    let mut stream = stream.build()?;
    let mut pinned_stream = Pin::new(&mut stream);

    let record_batch = utils::get_next_item_from_reader(&mut pinned_stream).await;
    if record_batch.is_none() {
        return Err("File is empty".into());
    }
    let record_batch = record_batch.unwrap();



    // Aggregation
    let mut aggregators: Vec<Option<Box<dyn Aggregator>>> = Vec::new();
    if let Some(aggregations) = aggregations {
        for aggregation in aggregations {
            let column_name = aggregation.column_name;
            let aggregation_op = aggregation.aggregation_op;
            if let Some(column_index) = name_to_index.get(&column_name) {
                let column = record_batch.column(*column_index);
                let data_type = column.data_type().clone();
                aggregators.push(build_aggregator(*column_index, aggregation_op, data_type));
            } else {
                return Err(format!("Could not find column with name: {}", column_name).into());
            }
        }
    }
    utils::aggregate_batch(&mut aggregators, &record_batch)?;

    let mut result = Vec::new();
    result.push(record_batch);

    while let Some(record_batch) = utils::get_next_item_from_reader(&mut pinned_stream).await {
        utils::aggregate_batch(&mut aggregators, &record_batch)?;
        result.push(record_batch);
    }

    aggregators
        .iter()
        .filter_map(|a| a.as_ref())
        .for_each(|aggregator| {
            let result = aggregator.get_result();
            println!("Result: {:?}", result)
        });

    Ok(result)
}




