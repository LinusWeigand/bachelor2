use arrow2::datatypes::DataType;
use arrow2::types::NativeType;
use arrow2::{array::Array, error::Error as ArrowError};
use core::f64;
use std::{
    cmp::{max, min},
    fmt::Display,
    marker::PhantomData,
};

use arrow2::{array::PrimitiveArray, chunk::Chunk};
use chrono::NaiveDateTime;

use crate::utils;

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

impl Display for AggregationOp {
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

pub fn return_date_else_int(value: i64) -> ScalarValue {
    let date_time = utils::get_naive_date_time_from_timestamp(value);
    if let Some(date_time) = date_time {
        ScalarValue::Date(date_time)
    } else {
        ScalarValue::Int64(value)
    }
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
