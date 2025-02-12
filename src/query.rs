use std::{
    collections::HashMap,
    error::Error,
    path::PathBuf,
    sync::{atomic::AtomicUsize, Arc},
};

use crate::{
    aggregation::{build_aggregator, Aggregation, ScalarValue},
    row_group_filter::keep_row_group,
    row_filter,
    utils::{self, Expression},
    Feature
};
use arrow2::{array::Array, chunk::Chunk, datatypes::{DataType, Schema}, io::parquet::read::FileReader};
use parquet2::metadata::RowGroupMetaData;

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
    mut metadata: MetadataItem,
    expression: Option<Expression>,
    select_columns: Option<Vec<String>>,
    aggregations: Option<Vec<Aggregation>>,
    features: &Vec<Feature>,
) -> Result<
    (
        Vec<Chunk<Box<dyn Array>>>,
        Arc<AtomicUsize>,
        Option<AggregationTable>,
    ),
    Box<dyn Error + Send + Sync>,
> {
    let file = std::fs::File::open(&metadata.path)?;
    let bytes_read = Arc::new(AtomicUsize::new(0));
    let counting_file = utils::CountingReader::new(file, bytes_read.clone());

    // Row Group Filter
    let mut row_groups = metadata.row_groups;
    if let Some(expression) = &expression {
        if features.contains(&Feature::Group) {
            row_groups = row_groups
                .into_iter()
                .filter_map(
                    |md| match keep_row_group(&md, &None, &expression, false, features) {
                        Ok(false) => None,
                        Ok(true) | _ => Some(md),
                    },
                )
                .collect();
        }
    }

    

    let batch_size = Some(550 * 128);
    let limit = None;
    let page_indexes = None;

    // Early Projection
    if features.contains(&Feature::Column) {
        if let Some(mut early_select) = select_columns.clone() {
            let filter_col_names = match &expression {
                Some(v) => utils::get_column_projection_from_expression(&v),
                None => Vec::new()
            };
            for col_name in filter_col_names {
                if !early_select.contains(&col_name) {
                    early_select.push(col_name);
                }
            }
            if features.contains(&Feature::Aggr) {
                if let Some(ref aggregations) = aggregations {
                    let aggr_col_names = utils::get_column_projection_from_aggregations(&aggregations);
                    for col_name in aggr_col_names {
                        if !early_select.contains(&col_name) {
                            early_select.push(col_name);
                        }
                    }
                }
            }
            metadata.schema = metadata.schema.filter(|_, field| early_select.contains(&field.name));
            metadata.name_to_index = utils::get_column_name_to_index(&metadata.schema);
        }
    }
    
    // Aggregation
    let mut aggregators = Vec::new();
    if features.contains(&Feature::Aggr) {
        if let Some(aggregations) = &aggregations {
            for aggregation in aggregations {
                let column_name = aggregation.column_name.clone();
                let aggregation_op = aggregation.aggregation_op.clone();

                let column = match metadata
                    .schema
                    .fields
                    .iter()
                    .find(|field| field.name == column_name)
                {
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


    let reader = FileReader::new(
        counting_file,
        row_groups,
        metadata.schema.clone(),
        batch_size,
        limit,
        page_indexes,
    );

    let mut result = Vec::new();
    for maybe_batch in reader {
        let mut batch = match maybe_batch {
            Ok(v) => v,
            Err(_) => continue,
        };

        if features.contains(&Feature::Row) {
            if let Some(expression) = &expression {
                let mask = row_filter::build_filter_mask(&batch, expression, &metadata.name_to_index)?;
                batch = arrow2::compute::filter::filter_chunk(&batch, &mask)?;
            }
        }

        utils::aggregate_batch(&mut aggregators, &batch)?;
        // Late Projection
        if features.contains(&Feature::Column) {
            if let Some(select_columns) = &select_columns {
                println!("select_columns: {:?}", select_columns);
                if select_columns.len() < metadata.schema.fields.len() {
                    let selected_indices: Vec<usize> = metadata.schema.fields.iter().enumerate().filter_map(|(i, field)| {
                        match select_columns.contains(&field.name) {
                            false => None,
                            true => Some(i)
                        }}).collect();
                    batch = utils::filter_columns(&batch, &selected_indices);

                    metadata.schema = metadata.schema.filter(|_, field| select_columns.contains(&field.name));
                    metadata.name_to_index = utils::get_column_name_to_index(&metadata.schema);
                }
            }
        }
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
                ScalarValue::USize(v) => {
                    let value: i64 = match v.try_into() {
                        Ok(v) => v,
                        Err(_) => {
                            return Err(format!(
                                "Could not cast aggregation result to Float64: {}",
                                &name
                            )
                            .into())
                        }
                    };
                    let value = Arc::new(arrow2::array::Int64Array::from_slice([value]));
                    aggregation_values.push(value);
                    aggregation_names.push(name);
                }
                ScalarValue::Null => {
                    let value = Arc::new(arrow2::array::NullArray::new(DataType::Null, 1));
                    aggregation_values.push(value);
                    aggregation_names.push(name);
                }
                
                v => {
                    println!("Aggregation: {:?}", v);
                    return Err("Aggregation Type not supported".into());
                }
            }
        }
    }

    let aggr_table = if features.contains(&Feature::Aggr) {
        let chunk = Chunk::new(aggregation_values);
        let names = aggregation_names;
        Some(AggregationTable { names, chunk })
    } else {
        None
    };

    Ok((result, bytes_read, aggr_table))
}
