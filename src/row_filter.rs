use arrow::{
    array::{
        Array, BooleanArray, BooleanBuilder, Date32Array, Date64Array, Float16Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array
        
    },
    datatypes::DataType,
    error::ArrowError,
};
use std::{collections::HashMap};

use crate::utils::{Comparison, Condition, Expression, Float, ThresholdValue};


pub fn predicate_function(
    expression: Expression,
) -> Box<dyn FnMut(RecordBatch) -> Result<BooleanArray, ArrowError> + Send + 'static> {
    let column_names = get_column_projection_from_expression(&expression);
    println!("predicate function: column names: {:?}", column_names);

    Box::new(
        move |batch: RecordBatch| -> Result<BooleanArray, ArrowError> {
            let num_rows = batch.num_rows();
            let mut builder = BooleanBuilder::with_capacity(num_rows);

            let schema = batch.schema();
            let mut col_index_map = HashMap::new();
            for col_name in &column_names {
                if let Some(field_index) = schema.index_of(col_name).ok() {
                    col_index_map.insert(col_name.clone(), field_index);
                }
            }

            for row_idx in 0..num_rows {
                let satisfied =
                    evaluate_expression_on_row(&expression, &batch, row_idx, &col_index_map)?;
                builder.append_value(satisfied);
            }

            Ok(builder.finish())
        },
    )
}

fn get_column_projection_from_expression(expression: &Expression) -> Vec<String> {
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

fn evaluate_expression_on_row(
    expr: &Expression,
    batch: &RecordBatch,
    row_idx: usize,
    col_index_map: &HashMap<String, usize>,
) -> Result<bool, ArrowError> {
    match expr {
        Expression::Condition(cond) => evaluate_condition(cond, batch, row_idx, col_index_map),
        Expression::And(left, right) => {
            let lhs = evaluate_expression_on_row(left, batch, row_idx, col_index_map)?;
            let rhs = evaluate_expression_on_row(right, batch, row_idx, col_index_map)?;
            Ok(lhs && rhs)
        }
        Expression::Or(left, right) => {
            let lhs = evaluate_expression_on_row(left, batch, row_idx, col_index_map)?;
            let rhs = evaluate_expression_on_row(right, batch, row_idx, col_index_map)?;
            Ok(lhs || rhs)
        }
        Expression::Not(inner) => {
            let val = evaluate_expression_on_row(inner, batch, row_idx, col_index_map)?;
            Ok(!val)
        }
    }
}

fn evaluate_condition(
    cond: &Condition,
    batch: &RecordBatch,
    row_idx: usize,
    col_index_map: &HashMap<String, usize>,
) -> Result<bool, ArrowError> {
    let col_index = match col_index_map.get(&cond.column_name) {
        Some(idx) => *idx,
        None => {
            return Ok(false);
        }
    };

    let column = batch.column(col_index);

    Ok(match column.data_type() {
        DataType::Int8 => {
            let col = column.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                ArrowError::SchemaError("Failed to downcast Int8Array".to_string())
            })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx) as i64;
            cond.comparison.keep_row(&ThresholdValue::Int64(value), &cond.threshold)
        }
        DataType::Int16 => {
            let col = column.as_any().downcast_ref::<Int16Array>().ok_or_else(|| {
                ArrowError::SchemaError("Failed to downcast Int16Array".to_string())
            })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx) as i64;
            cond.comparison.keep_row(&ThresholdValue::Int64(value), &cond.threshold)
        }
        DataType::Int32 => {
            let col = column.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                ArrowError::SchemaError("Failed to downcast Int32Array".to_string())
            })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx) as i64;
            cond.comparison.keep_row(&ThresholdValue::Int64(value), &cond.threshold)
        }
        DataType::Int64 => {
            let col = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                ArrowError::SchemaError("Failed to downcast Int64Array".to_string())
            })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx);
            cond.comparison.keep_row(&ThresholdValue::Int64(value), &cond.threshold)
        }
        DataType::UInt8 => {
            let col = column.as_any().downcast_ref::<UInt8Array>().ok_or_else(|| {
                ArrowError::SchemaError("Failed to downcast UInt8Array".to_string())
            })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx) as i64;
            cond.comparison.keep_row(&ThresholdValue::Int64(value), &cond.threshold)
        }
        DataType::UInt16 => {
            let col = column.as_any().downcast_ref::<UInt16Array>().ok_or_else(|| {
                ArrowError::SchemaError("Failed to downcast UInt16Array".to_string())
            })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx) as i64;
            cond.comparison.keep_row(&ThresholdValue::Int64(value), &cond.threshold)
        }
        DataType::UInt32 => {
            let col = column.as_any().downcast_ref::<UInt32Array>().ok_or_else(|| {
                ArrowError::SchemaError("Failed to downcast UInt32Array".to_string())
            })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx) as i64;
            cond.comparison.keep_row(&ThresholdValue::Int64(value), &cond.threshold)
        }
        DataType::UInt64 => {
            let col = column.as_any().downcast_ref::<UInt64Array>().ok_or_else(|| {
                ArrowError::SchemaError("Failed to downcast UInt64Array".to_string())
            })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx) as i64;
            cond.comparison.keep_row(&ThresholdValue::Int64(value), &cond.threshold)
        }
        DataType::Float16 => {
            let col = column.as_any().downcast_ref::<Float16Array>().ok_or_else(|| {
                ArrowError::SchemaError("Failed to downcast Float16Array".to_string())
            })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx).to_f64();
            cond.comparison.keep_row(&ThresholdValue::Float64(value), &cond.threshold)
        }
        DataType::Float32 => {
            let col = column.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                ArrowError::SchemaError("Failed to downcast Float32Array".to_string())
            })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx) as f64;
            cond.comparison.keep_row(&ThresholdValue::Float64(value), &cond.threshold)
        }
        DataType::Float64 => {
            let col = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                ArrowError::SchemaError("Failed to downcast Float64Array".to_string())
            })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx);
            cond.comparison.keep_row(&ThresholdValue::Float64(value), &cond.threshold)
        }
        DataType::Date32 => {
            let col = column.as_any().downcast_ref::<Date32Array>().ok_or_else(|| {
                ArrowError::SchemaError("Failed to downcast Date32Array".to_string())
            })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx) as i64;
            cond.comparison.keep_row(&ThresholdValue::Int64(value), &cond.threshold)
        }
        DataType::Date64 => {
            let col = column.as_any().downcast_ref::<Date64Array>().ok_or_else(|| {
                ArrowError::SchemaError("Failed to downcast Date64Array".to_string())
            })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx) as i64;
            cond.comparison.keep_row(&ThresholdValue::Int64(value), &cond.threshold)
        }
        DataType::Boolean => {
            let col = column.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                ArrowError::SchemaError("Failed to downcast Date64Array".to_string())
            })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx);
            cond.comparison.keep_row(&ThresholdValue::Boolean(value), &cond.threshold)
        }
        DataType::Utf8 => {
            let col = column.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                ArrowError::SchemaError("Failed to downcast Date64Array".to_string())
            })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx).to_owned();
            cond.comparison.keep_row(&ThresholdValue::Utf8String(value), &cond.threshold)
        }
        DataType::LargeUtf8 => {
            let col = column.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                ArrowError::SchemaError("Failed to downcast Date64Array".to_string())
            })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx).to_owned();
            cond.comparison.keep_row(&ThresholdValue::Utf8String(value), &cond.threshold)
        }
        DataType::Utf8View => {
            let col = column.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                ArrowError::SchemaError("Failed to downcast Date64Array".to_string())
            })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx).to_owned();
            cond.comparison.keep_row(&ThresholdValue::Utf8String(value), &cond.threshold)
        }
        _ => false,
    })
}

impl Comparison {
    pub fn keep_row(
        &self,
        row_value: &ThresholdValue,
        v: &ThresholdValue,
    ) -> bool {
        match (row_value, v) {
            (ThresholdValue::Int64(row_value), ThresholdValue::Int64(v)) => {
                compare(row_value, v, &self)
            },
            (ThresholdValue::Boolean(row_value), ThresholdValue::Boolean(v)) => {
                compare(row_value, v, &self)
            },
            (ThresholdValue::Utf8String(row_value), ThresholdValue::Utf8String(v)) => {
                compare(row_value, v, &self)
            },
            (ThresholdValue::Float64(row_value), ThresholdValue::Float64(v)) => {
                compare_floats(*row_value, *v, &self)
            },
            _ => false,
        }
    }
}

fn compare<T: Ord>(
    row_value: T,
    v: T,
    comparison: &Comparison,
) ->bool {
    match comparison {
        Comparison::LessThan => row_value < v,
        Comparison::LessThanOrEqual => row_value <= v,
        Comparison::Equal => row_value == (v),
        Comparison::GreaterThanOrEqual => row_value >= v,
        Comparison::GreaterThan => row_value > v,
    }
}

fn compare_floats<T: Float>(
    row_value: T,
    v: T,
    comparison: &Comparison,
) -> bool {
    match comparison {
        Comparison::LessThan => row_value < v,
        Comparison::LessThanOrEqual => row_value <= v,
        Comparison::Equal => row_value.equal(v),
        Comparison::GreaterThanOrEqual => row_value >= v,
        Comparison::GreaterThan => row_value > v,
    }
}
