use arrow2::{
    array::{
        Array, BooleanArray, Float16Array, PrimitiveArray, Utf8Array
    }, chunk::Chunk, compute::{cast::CastOptions, comparison::{boolean, primitive, utf8}}, datatypes::DataType, error::Error as ArrowError
};
use std::collections::HashMap;
use crate::utils::{Comparison, Expression, ThresholdValue};

use arrow2::compute;

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
