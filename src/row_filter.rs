use arrow::{
    array::{
        Array, BooleanArray, BooleanBuilder, Date32Array, Date64Array, Float16Array, Float32Array,
        Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray,
        StringViewArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::DataType,
    error::ArrowError,
};
use std::collections::HashMap;

use crate::query::{Comparison, Condition, Expression, ThresholdValue};

pub fn predicate_function(
    expression: Expression,
) -> Box<dyn FnMut(RecordBatch) -> Result<BooleanArray, ArrowError> + Send + 'static> {
    let column_names = get_column_projection_from_expression(&expression);

    Box::new(
        move |batch: RecordBatch| -> Result<BooleanArray, ArrowError> {
            let num_rows = batch.num_rows();
            let mut builder = BooleanBuilder::with_capacity(num_rows);

            let schema = batch.schema();
            let mut col_index_map = std::collections::HashMap::new();
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
    col_index_map: &std::collections::HashMap<String, usize>,
) -> Result<bool, ArrowError> {
    let col_index = match col_index_map.get(&cond.column_name) {
        Some(idx) => *idx,
        None => {
            return Ok(false);
        }
    };

    let column = batch.column(col_index);

    match column.data_type() {
        DataType::Int8 => {
            let col = column.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                ArrowError::SchemaError("Failed to downcast Int8Array".to_string())
            })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx);
            compare_numeric(value as f64, &cond.comparison, &cond.threshold)
        }
        DataType::Int16 => {
            let col = column
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| {
                    ArrowError::SchemaError("Failed to downcast Int16Array".to_string())
                })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx);
            compare_numeric(value as f64, &cond.comparison, &cond.threshold)
        }
        DataType::Int32 => {
            let col = column
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| {
                    ArrowError::SchemaError("Failed to downcast Int32Array".to_string())
                })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx);
            compare_numeric(value as f64, &cond.comparison, &cond.threshold)
        }
        DataType::Int64 => {
            let col = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    ArrowError::SchemaError("Failed to downcast Int64Array".to_string())
                })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx);
            compare_numeric(value as f64, &cond.comparison, &cond.threshold)
        }
        DataType::UInt8 => {
            let col = column
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| {
                    ArrowError::SchemaError("Failed to downcast Int8Array".to_string())
                })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx);
            compare_numeric(value as f64, &cond.comparison, &cond.threshold)
        }
        DataType::UInt16 => {
            let col = column
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| {
                    ArrowError::SchemaError("Failed to downcast Int16Array".to_string())
                })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx);
            compare_numeric(value as f64, &cond.comparison, &cond.threshold)
        }
        DataType::UInt32 => {
            let col = column
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| {
                    ArrowError::SchemaError("Failed to downcast Int32Array".to_string())
                })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx);
            compare_numeric(value as f64, &cond.comparison, &cond.threshold)
        }
        DataType::UInt64 => {
            let col = column
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| {
                    ArrowError::SchemaError("Failed to downcast Int64Array".to_string())
                })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }

            let value = col.value(row_idx);
            compare_numeric(value as f64, &cond.comparison, &cond.threshold)
        }
        DataType::Float16 => {
            let col = column
                .as_any()
                .downcast_ref::<Float16Array>()
                .ok_or_else(|| {
                    ArrowError::SchemaError("Failed to downcast Int64Array".to_string())
                })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }

            let value = col.value(row_idx);
            compare_numeric(f64::from(value), &cond.comparison, &cond.threshold)
        }
        DataType::Float32 => {
            let col = column
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    ArrowError::SchemaError("Failed to downcast Int64Array".to_string())
                })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }

            let value = col.value(row_idx);
            compare_numeric(f64::from(value), &cond.comparison, &cond.threshold)
        }
        DataType::Float64 => {
            let col = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    ArrowError::SchemaError("Failed to downcast Int64Array".to_string())
                })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }

            let value = col.value(row_idx);
            compare_numeric(f64::from(value), &cond.comparison, &cond.threshold)
        }
        DataType::Date32 => {
            let col = column
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    ArrowError::SchemaError("Failed to downcast Int64Array".to_string())
                })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }

            let value = col.value(row_idx);
            compare_numeric(f64::from(value), &cond.comparison, &cond.threshold)
        }
        DataType::Date64 => {
            let col = column
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| {
                    ArrowError::SchemaError("Failed to downcast Int64Array".to_string())
                })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }

            let value = col.value(row_idx);
            compare_numeric(value as f64, &cond.comparison, &cond.threshold)
        }
        DataType::Boolean => {
            let col = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    ArrowError::SchemaError("Failed to downcast Int64Array".to_string())
                })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }

            let value = col.value(row_idx);
            compare_boolean(value, &cond.comparison, &cond.threshold)
        }
        DataType::Utf8 => {
            let col = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    ArrowError::SchemaError("Failed to downcast StringArray".to_string())
                })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx);
            compare_utf8(value, &cond.comparison, &cond.threshold)
        }
        DataType::LargeUtf8 => {
            let col = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    ArrowError::SchemaError("Failed to downcast StringArray".to_string())
                })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx);
            compare_utf8(value, &cond.comparison, &cond.threshold)
        }
        DataType::Utf8View => {
            let col = column
                .as_any()
                .downcast_ref::<StringViewArray>()
                .ok_or_else(|| {
                    ArrowError::SchemaError("Failed to downcast StringArray".to_string())
                })?;
            if col.is_null(row_idx) {
                return Ok(false);
            }
            let value = col.value(row_idx);
            compare_utf8(value, &cond.comparison, &cond.threshold)
        }
        _ => Ok(false),
    }
}

fn compare_numeric(
    row_value: f64,
    comparison: &Comparison,
    threshold: &ThresholdValue,
) -> Result<bool, ArrowError> {
    match threshold {
        ThresholdValue::Number(n) => match comparison {
            Comparison::LessThan => Ok(row_value < *n),
            Comparison::LessThanOrEqual => Ok(row_value <= *n),
            Comparison::Equal => Ok((row_value - *n).abs() < f64::EPSILON),
            Comparison::GreaterThanOrEqual => Ok(row_value >= *n),
            Comparison::GreaterThan => Ok(row_value > *n),
        },
        _ => Ok(false),
    }
}

fn compare_utf8(
    row_value: &str,
    comparison: &Comparison,
    threshold: &ThresholdValue,
) -> Result<bool, ArrowError> {
    match threshold {
        ThresholdValue::Utf8String(s) => match comparison {
            Comparison::LessThan => Ok(row_value < s.as_str()),
            Comparison::LessThanOrEqual => Ok(row_value <= s.as_str()),
            Comparison::Equal => Ok(row_value == s),
            Comparison::GreaterThanOrEqual => Ok(row_value >= s.as_str()),
            Comparison::GreaterThan => Ok(row_value > s.as_str()),
        },
        _ => Ok(false),
    }
}

fn compare_boolean(
    row_value: bool,
    comparison: &Comparison,
    threshold: &ThresholdValue,
) -> Result<bool, ArrowError> {
    match threshold {
        ThresholdValue::Boolean(n) => match comparison {
            Comparison::LessThan => Ok(row_value < *n),
            Comparison::LessThanOrEqual => Ok(row_value <= *n),
            Comparison::Equal => Ok(row_value == *n),
            Comparison::GreaterThanOrEqual => Ok(row_value >= *n),
            Comparison::GreaterThan => Ok(row_value > *n),
        },
        _ => Ok(false),
    }
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
