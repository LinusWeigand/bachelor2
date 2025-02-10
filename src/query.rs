use std::{
    collections::HashMap, error::Error, path::PathBuf, sync::{atomic::AtomicUsize, Arc}
};

use arrow2::{array::Array, chunk::Chunk, datatypes::Schema, io::parquet::read::FileReader};
use parquet2::metadata::RowGroupMetaData;
use crate::{
    aggregation::{Aggregation}, row_group_filter::keep_row_group, utils::{self, Expression}, Mode
};

pub struct MetadataItem {
    pub path: PathBuf,
    pub schema: Schema,
    pub row_groups: Vec<RowGroupMetaData>,
    pub name_to_index: HashMap<String, usize>,
}

pub async fn smart_query_parquet(
    metadata: MetadataItem,
    expression: Option<Expression>,
    select_columns: Option<Vec<String>>,
    aggregations: Option<Vec<Aggregation>>,
    mode: &Mode,
) -> Result<(Vec<Chunk<Box<dyn Array>>>, Arc<AtomicUsize>), Box<dyn Error + Send + Sync>> {
    let file = std::fs::File::open(&metadata.path)?;
    let bytes_read = Arc::new(AtomicUsize::new(0));
    let counting_file = utils::CountingReader::new(file, bytes_read.clone());
    let mut schema: Schema = metadata.schema;

    // Projection
    if let Some(select_columns) = select_columns {
        schema = schema.filter(|_, field| select_columns.contains(&field.name));
    } 

    // Row Group Filter
    let mut row_groups = metadata.row_groups;
    if let Some(expression) = expression {
        if mode != &Mode::Base {
            row_groups = row_groups.into_iter().filter_map(|md| {
                match keep_row_group(&md, &None, &expression, false, mode) {
                    Ok(false) => None,
                    Ok(true) | _  => Some(md),
                }
            }).collect();
        }
    }

    let batch_size = Some(550 * 128);
    let limit = None;
    let page_indexes = None;

    let reader = FileReader::new(counting_file, row_groups, schema, batch_size, limit, page_indexes);

    let mut result = Vec::new();
    for maybe_batch in reader {
        let batch = match maybe_batch {
            Ok(v) => v,
            Err(_) => continue,
        };
        result.push(batch);
    }

    // Aggregation
    // if mode == &Mode::Aggr {
    //     if let Some(aggregations) = aggregations {
    //         let mut aggregators: Vec<Option<Box<dyn Aggregator>>> = Vec::new();
    //         for aggregation in aggregations {
    //             let column_name = aggregation.column_name;
    //             let aggregation_op = aggregation.aggregation_op;
    //             if let Some(column_index) = name_to_index.get(&column_name) {
    //                 let column = record_batch.column(*column_index);
    //                 let data_type = column.data_type().clone();
    //                 aggregators.push(build_aggregator(
    //                     *column_index,
    //                     column_name,
    //                     aggregation_op,
    //                     data_type,
    //                 ));
    //             } else {
    //                 return Err(format!("Could not find column with name: {}", column_name).into());
    //             }
    //         }
    //         utils::aggregate_batch(&mut aggregators, &record_batch)?;
    //         while let Some(record_batch) =
    //             utils::get_next_item_from_reader_and_count_bytes(&mut pinned_stream).await
    //         {
    //             utils::aggregate_batch(&mut aggregators, &record_batch)?;
    //         }
    //         let mut aggregation_values: Vec<Arc<dyn Array>> = Vec::new();
    //         let mut aggregation_fields = Vec::new();
    //         for aggregator in aggregators {
    //             if let Some(aggregator) = aggregator.as_ref() {
    //                 let name = aggregator.get_name();
    //                 match aggregator.get_result() {
    //                     ScalarValue::Int128(v) => {
    //                         let value: i64 = match v.try_into() {
    //                             Ok(v) => v,
    //                             Err(_) => {
    //                                 return Err(format!(
    //                                     "Could not cast aggregation result to Int64: {}",
    //                                     &name
    //                                 )
    //                                 .into())
    //                             }
    //                         };
    //                         let value = Arc::new(Int64Array::from(vec![value]));
    //                         aggregation_values.push(value);
    //                         let field = Field::new(name, DataType::Int64, false);
    //                         aggregation_fields.push(field);
    //                     }
    //                     ScalarValue::Float64(v) => {
    //                         let value: f64 = match v.try_into() {
    //                             Ok(v) => v,
    //                             Err(_) => {
    //                                 return Err(format!(
    //                                     "Could not cast aggregation result to Float64: {}",
    //                                     &name
    //                                 )
    //                                 .into())
    //                             }
    //                         };
    //                         let value = Arc::new(Float64Array::from(vec![value]));
    //                         aggregation_values.push(value);
    //                         let field = Field::new(name, DataType::Float64, false);
    //                         aggregation_fields.push(field);
    //                     }
    //                     _ => return Err("Aggregation Type not supported".into()),
    //                 }
    //
    //                 println!("Result: {:?}", result);
    //             }
    //         }
    //         // TODO: Stream to client;
    //         return Ok((result, bytes_read));
    //     }
    // }

    Ok((result, bytes_read))
}
