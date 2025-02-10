use std::{
    collections::HashMap, error::Error, path::PathBuf, sync::{atomic::AtomicUsize, Arc}
};

use arrow2::{array::{Array}, chunk::Chunk, datatypes::Schema, io::parquet::read::FileReader};
use parquet2::metadata::RowGroupMetaData;
use crate::{
    aggregation::{build_aggregator, Aggregation, Aggregator, ScalarValue}, row_group_filter::keep_row_group, utils::{self, Expression}, Mode
};

pub struct MetadataItem {
    pub path: PathBuf,
    pub schema: Schema,
    pub row_groups: Vec<RowGroupMetaData>,
    pub name_to_index: HashMap<String, usize>,
}

pub struct AggregationTable {
    pub names: Vec<String>,
    pub chunk: Chunk<Arc<dyn Array>>,
}

pub async fn smart_query_parquet(
    metadata: MetadataItem,
    expression: Option<Expression>,
    select_columns: Option<Vec<String>>,
    aggregations: Option<Vec<Aggregation>>,
    mode: &Mode,
) -> Result<(Vec<Chunk<Box<dyn Array>>>, Arc<AtomicUsize>, Option<AggregationTable>), Box<dyn Error + Send + Sync>> {
    let file = std::fs::File::open(&metadata.path)?;
    let bytes_read = Arc::new(AtomicUsize::new(0));
    let counting_file = utils::CountingReader::new(file, bytes_read.clone());

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



    // Aggregation
    let mut aggregators = Vec::new();
    if mode == &Mode::Aggr {
        if let Some(aggregations) = aggregations {
            let mut aggregators: Vec<Option<Box<dyn Aggregator>>> = Vec::new();
            for aggregation in aggregations {
                let column_name = aggregation.column_name;
                let aggregation_op = aggregation.aggregation_op;

                let column = match metadata.schema.fields.iter().find(|field| field.name == column_name) {
                    Some(v) => v,
                    None => continue,
                };

                let column_index = match metadata.name_to_index.get(&column_name) {
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
        }
    }

    let batch_size = Some(550 * 128);
    let limit = None;
    let page_indexes = None;

    let mut schema: Schema = metadata.schema;

    // Projection
    if let Some(select_columns) = select_columns {
        schema = schema.filter(|_, field| select_columns.contains(&field.name));
    } 

    let reader = FileReader::new(counting_file, row_groups, schema, batch_size, limit, page_indexes);

    let mut result = Vec::new();
    for maybe_batch in reader {
        let batch = match maybe_batch {
            Ok(v) => v,
            Err(_) => continue,
        };
        utils::aggregate_batch(&mut aggregators, &batch)?;
        result.push(batch);
    }

    let mut aggregation_values: Vec<Arc<dyn Array>> = Vec::new();
    let mut aggregation_names: Vec<String> = Vec::new();
    for aggregator in aggregators {
        if let Some(aggregator) = aggregator.as_ref() {
            let name = aggregator.get_name();

            match aggregator.get_result() {
                ScalarValue::Int64(v) => {
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
                    let array = Arc::new(arrow2::array::Int64Array::from_slice([value]));
                    aggregation_values.push(array);
                    aggregation_names.push(name);

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
                    let value = Arc::new(arrow2::array::Float64Array::from_slice([value]));
                    aggregation_values.push(value);
                    aggregation_names.push(name);
                }
                _ => return Err("Aggregation Type not supported".into()),
            }

        }
    }
    let aggr_table = match mode {
        Mode::Aggr => {
            let chunk = Chunk::new(aggregation_values);
            let names = aggregation_names;
            Some(AggregationTable { names, chunk })
        },
        _ => None
    };

    Ok((result, bytes_read, aggr_table))
}


