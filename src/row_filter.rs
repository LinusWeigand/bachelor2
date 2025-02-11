use arrow::array::{Date32Array, Date64Array};
use rayon::prelude::*;
use arrow2::{
    array::{
        Array, BooleanArray, Float16Array, Float32Array, Float64Array, PrimitiveArray, Utf8Array
    },
    chunk::Chunk,
    datatypes::DataType,
    error::Error as ArrowError,
};
use std::collections::HashMap;
use crate::utils::{Comparison, Condition, Expression, ThresholdValue};



impl Comparison {
    pub fn keep_row(&self, row_value: &ThresholdValue, v: &ThresholdValue) -> bool {
        match (row_value, v) {
            (ThresholdValue::Int64(row_value), ThresholdValue::Int64(th)) => {
                compare(*row_value, *th, self)
            }
            (ThresholdValue::Float64(row_value), ThresholdValue::Float64(th)) => {
                compare(*row_value, *th, self)
            }
            (ThresholdValue::Boolean(row_value), ThresholdValue::Boolean(th)) => {
                compare(*row_value, *th, self)
            }
            (ThresholdValue::Utf8String(row_value), ThresholdValue::Utf8String(th)) => {
                compare(row_value, th, self)
            }
            _ => false,
        }
    }
}

fn compare<T: PartialOrd>(row_val: T, th_val: T, comparison: &Comparison) -> bool {
    match comparison {
        Comparison::LessThan => row_val < th_val,
        Comparison::LessThanOrEqual => row_val <= th_val,
        Comparison::Equal => row_val == th_val,
        Comparison::GreaterThanOrEqual => row_val >= th_val,
        Comparison::GreaterThan => row_val > th_val,
    }
}

pub fn parallel_predicate_function(
    chunk: &Chunk<Box<dyn Array>>,
    expression: &Expression,
    name_to_index: &HashMap<String, usize>,
) -> Result<BooleanArray, ArrowError> {
    let num_rows = chunk.len();
    // TODO: Optimize: Downcast outside of loop

    let bools: Vec<bool> = (0..num_rows)
        .into_par_iter()
        .map(|row_index| {
            evaluate_expression_on_row(expression, chunk, row_index, name_to_index)
                .unwrap_or(false)
        })
        .collect();

    Ok(BooleanArray::from_slice(bools))
}

fn evaluate_expression_on_row(
    expr: &Expression,
    chunk: &Chunk<Box<dyn Array>>,
    row_index: usize,
    name_to_index: &HashMap<String, usize>,
) -> Result<bool, ArrowError> {
    match expr {
        Expression::Condition(cond) => evaluate_condition(cond, chunk, row_index, name_to_index),
        Expression::And(left, right) => {
            Ok(evaluate_expression_on_row(left, chunk, row_index, name_to_index)?
                && evaluate_expression_on_row(right, chunk, row_index, name_to_index)?)
        }
        Expression::Or(left, right) => {
            Ok(evaluate_expression_on_row(left, chunk, row_index, name_to_index)?
                || evaluate_expression_on_row(right, chunk, row_index, name_to_index)?)
        }
        Expression::Not(inner) => {
            Ok(!evaluate_expression_on_row(inner, chunk, row_index, name_to_index)?)
        }
    }
}

fn evaluate_condition(
    cond: &Condition,
    chunk: &Chunk<Box<dyn Array>>,
    row_index: usize,
    col_index_map: &HashMap<String, usize>,
) -> Result<bool, ArrowError> {
    let col_index = match col_index_map.get(&cond.column_name) {
        Some(idx) => *idx,
        None => return Ok(false),
    };

    let column = &chunk[col_index];

    match column.data_type() {
        DataType::Int8 => {
            let col = column.as_any().downcast_ref::<PrimitiveArray<i8>>().ok_or_else(|| {
                ArrowError::InvalidArgumentError("Failed to downcast Int8Array".to_string())
            })?;
            if col.is_null(row_index) {
                return Ok(false);
            }
            let value = col.value(row_index) as i64;
            println!("INT8");
            Ok(cond.comparison
                .keep_row(&ThresholdValue::Int64(value), &cond.threshold))
        }
        DataType::Int16 => {
            println!("INT16");
            let col = column
                .as_any()
                .downcast_ref::<PrimitiveArray<i16>>()
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError("Failed to downcast Int16Array".to_string())
                })?;
            if col.is_null(row_index) {
                return Ok(false);
            }
            let value = col.value(row_index) as i64;
            Ok(cond.comparison
                .keep_row(&ThresholdValue::Int64(value), &cond.threshold))
        }
        DataType::Int32 => {
            println!("INT32");
            let col = column
                .as_any()
                .downcast_ref::<PrimitiveArray<i32>>()
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError("Failed to downcast Int32Array".to_string())
                })?;
            if col.is_null(row_index) {
                return Ok(false);
            }
            let value = col.value(row_index) as i64;
            Ok(cond.comparison
                .keep_row(&ThresholdValue::Int64(value), &cond.threshold))
        }
        DataType::Int64 => {
            println!("INT&$");
            let col = column
                .as_any()
                .downcast_ref::<PrimitiveArray<i64>>()
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError("Failed to downcast Int64Array".to_string())
                })?;
            if col.is_null(row_index) {
                return Ok(false);
            }
            let value = col.value(row_index);
            Ok(cond.comparison
                .keep_row(&ThresholdValue::Int64(value), &cond.threshold))
        }
        DataType::UInt8 => {
            println!("UINT8");
            let col = column
                .as_any()
                .downcast_ref::<PrimitiveArray<u8>>()
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError("Failed to downcast UInt8Array".to_string())
                })?;
            if col.is_null(row_index) {
                return Ok(false);
            }
            let value = col.value(row_index) as i64;
            Ok(cond.comparison
                .keep_row(&ThresholdValue::Int64(value), &cond.threshold))
        }
        DataType::UInt16 => {
            println!("UINT16");
            let col = column
                .as_any()
                .downcast_ref::<PrimitiveArray<u16>>()
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError("Failed to downcast UInt16Array".to_string())
                })?;
            if col.is_null(row_index) {
                return Ok(false);
            }
            let value = col.value(row_index) as i64;
            Ok(cond.comparison
                .keep_row(&ThresholdValue::Int64(value), &cond.threshold))
        }
        DataType::UInt32 => {
            println!("UINT32");
            let col = column
                .as_any()
                .downcast_ref::<PrimitiveArray<u32>>()
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError("Failed to downcast UInt32Array".to_string())
                })?;
            if col.is_null(row_index) {
                return Ok(false);
            }
            let value = col.value(row_index) as i64;
            Ok(cond.comparison
                .keep_row(&ThresholdValue::Int64(value), &cond.threshold))
        }
        DataType::UInt64 => {
            println!("UINT64");
            let col = column
                .as_any()
                .downcast_ref::<PrimitiveArray<u64>>()
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError("Failed to downcast UInt64Array".to_string())
                })?;
            if col.is_null(row_index) {
                return Ok(false);
            }
            let value: i64 = col.value(row_index).try_into().map_err(|_| {
                ArrowError::InvalidArgumentError("Failed to downcast UInt64Array".to_string())
            })?;
            Ok(cond.comparison
                .keep_row(&ThresholdValue::Int64(value), &cond.threshold))
        }
        DataType::Float16 => {
            println!("FLOAT16");
            let col = column
                .as_any()
                .downcast_ref::<Float16Array>()
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError("Failed to downcast Float16Array".to_string())
                })?;
            if col.is_null(row_index) {
                return Ok(false);
            }
            let half_val = col.value(row_index);
            let value: f64 = half_val.to_f32() as f64;
            Ok(cond.comparison
                .keep_row(&ThresholdValue::Float64(value), &cond.threshold))
        }
        DataType::Float32 => {
            println!("FLOAT32");
            let col = column
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError("Failed to downcast Float32Array".to_string())
                })?;
            if col.is_null(row_index) {
                return Ok(false);
            }
            let value = col.value(row_index) as f64;
            Ok(cond.comparison
                .keep_row(&ThresholdValue::Float64(value), &cond.threshold))
        }
        DataType::Float64 => {
            println!("FLOAT64");
            let col = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError("Failed to downcast Float64Array".to_string())
                })?;
            if col.is_null(row_index) {
                return Ok(false);
            }
            let value = col.value(row_index);
            Ok(cond.comparison
                .keep_row(&ThresholdValue::Float64(value), &cond.threshold))
        }
        DataType::Date32 => {
            println!("Date32");
            let col = column
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError("Failed to downcast Date32Array".to_string())
                })?;
            let value = col.value(row_index) as i64;
            Ok(cond.comparison
                .keep_row(&ThresholdValue::Int64(value), &cond.threshold))
        }
        DataType::Date64 => {
            println!("Date64");
            let col = column
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError("Failed to downcast Date64Array".to_string())
                })?;
            let value = col.value(row_index) as i64;
            Ok(cond.comparison
                .keep_row(&ThresholdValue::Int64(value), &cond.threshold))
        }
        DataType::Boolean => {
            println!("Boolean");
            let col = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError("Failed to downcast Date64Array".to_string())
                })?;
            if col.is_null(row_index) {
                return Ok(false);
            }
            let value = col.value(row_index);
            Ok(cond.comparison
                .keep_row(&ThresholdValue::Boolean(value), &cond.threshold))
        }
        DataType::Utf8 => {
            println!("UTF8");
            let col = column
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError("Failed to downcast Date64Array".to_string())
                })?;
            let value = col.value(row_index).to_owned();
            Ok(cond.comparison
                .keep_row(&ThresholdValue::Utf8String(value), &cond.threshold))
        }
        DataType::LargeUtf8 => {
            println!("Largeutf8");
            let col = column
                .as_any()
                .downcast_ref::<Utf8Array<i32>>()
                .ok_or_else(|| {
                    ArrowError::InvalidArgumentError("Failed to downcast Date64Array".to_string())
                })?;
            if col.is_null(row_index) {
                return Ok(false);
            }
            let value = col.value(row_index).to_owned();
            Ok(cond.comparison
                .keep_row(&ThresholdValue::Utf8String(value), &cond.threshold))
        }
        _ => {
            println!("Could not cast");
            Ok(false)
        }
    }
}
