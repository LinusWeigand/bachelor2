use core::f64;
use std::{cmp::{max, min}, i128, marker::PhantomData};

use arrow::{array::{Array, ArrowPrimitiveType, PrimitiveArray, RecordBatch}, datatypes::{DataType, Date32Type, Date64Type, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type}, error::ArrowError};
use chrono::NaiveDateTime;

use crate::utils;

#[derive(Debug, Clone)]
pub enum ScalarValue {
    Null,
    Int128(i128),
    Float64(f64),
    Boolean(bool),
    String(String),
    USize(usize),
    Date(NaiveDateTime)
}

pub trait Aggregator: Send + Sync {
    fn aggregate_batch(&mut self, batch: &RecordBatch) -> Result<(), ArrowError>;
    fn get_result(&self) -> ScalarValue;
}

#[derive(PartialEq, Clone, Debug)]
pub enum AggregationOp {
    SUM,
    AVG,
    COUNT,
    MIN,
    MAX,
}

#[derive(Debug)]
pub struct Aggregation {
    pub column_name: String,
    pub aggregation_op: AggregationOp
}



pub fn build_aggregator(
    column_index: usize,
    aggregation_op: AggregationOp,
    data_type: DataType,
) -> Option<Box<dyn Aggregator>> {
    let column_index = column_index;
    let aggregation_op = aggregation_op.clone();
    match data_type {
        DataType::Int8 => Some(Box::new(IntegerAggregator::<Int8Type>::new(
            column_index, aggregation_op,
        ))),
        DataType::Int16 => Some(Box::new(IntegerAggregator::<Int16Type>::new(
            column_index, aggregation_op,
        ))),
        DataType::Int32 => Some(Box::new(IntegerAggregator::<Int32Type>::new(
            column_index, aggregation_op,
        ))),
        DataType::Int64 => Some(Box::new(IntegerAggregator::<Int64Type>::new(
            column_index, aggregation_op,
        ))),
        DataType::UInt8 => Some(Box::new(IntegerAggregator::<UInt8Type>::new(
            column_index, aggregation_op,
        ))),
        DataType::UInt16 => Some(Box::new(IntegerAggregator::<UInt16Type>::new(
            column_index, aggregation_op,
        ))),
        DataType::UInt32 => Some(Box::new(IntegerAggregator::<UInt32Type>::new(
            column_index, aggregation_op,
        ))),
        DataType::UInt64 => Some(Box::new(IntegerAggregator::<UInt64Type>::new(
            column_index, aggregation_op,
        ))),
        DataType::Date32 => Some(Box::new(IntegerAggregator::<Date32Type>::new(
            column_index, aggregation_op,
        ))),
        DataType::Date64 => Some(Box::new(IntegerAggregator::<Date64Type>::new(
            column_index, aggregation_op,
        ))),
        DataType::Float16 => Some(Box::new(FloatAggregator::<Float16Type>::new(
            column_index, aggregation_op,
        ))),
        DataType::Float32 => Some(Box::new(FloatAggregator::<Float32Type>::new(
            column_index, aggregation_op,
        ))),
        DataType::Float64 => Some(Box::new(FloatAggregator::<Float64Type>::new(
            column_index, aggregation_op,
        ))),
        _ => None,
    }
}


pub struct IntegerAggregator<T: ArrowPrimitiveType> {
    column_index: usize,
    aggregation_op: AggregationOp,
    sum: i128,
    count: usize,
    min: i128,
    max: i128,

    // marker for compiler that we need T but not at runtime
    phantom: PhantomData<fn() -> T>,
}

impl<T> IntegerAggregator<T>
where 
    T: ArrowPrimitiveType,
    T::Native: Into<i128>,
{

    pub fn new(column_index: usize, aggregation_op: AggregationOp) -> Self {
        Self {
            column_index,
            aggregation_op,
            sum: 0,
            count: 0,
            min: i128::MAX,
            max: i128::MIN,
            phantom: PhantomData
        }
    }

    fn process_value(&mut self, v: i128) {
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
    T: ArrowPrimitiveType,
    T::Native: Into<i128>,

{
    fn aggregate_batch(&mut self, batch: &RecordBatch) -> Result<(), ArrowError>{
        let column = batch.column(self.column_index);

        let array = column
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| {
                ArrowError::SchemaError(format!(
                        "Downcast to PrimitiveArray<{}> failed!", 
                        std::any::type_name::<T>()
                ))
            })?;

        for i in 0..array.len() {
            if array.is_valid(i) {
                let value: i128 = array.value(i).into();
                self.process_value(value);
            }
        }
        Ok(())
    }
    fn get_result(&self) -> ScalarValue {
        match self.aggregation_op {
            AggregationOp::SUM => match T::DATA_TYPE {
                DataType::Date32 | DataType::Date64 => return_date_else_int(self.sum),
                _ => ScalarValue::Int128(self.sum),
            },
            AggregationOp::AVG => {
                if self.count == 0 {
                    ScalarValue::Null
                } else {
                    let value = self.sum / self.count as i128;
                    match T::DATA_TYPE {
                        DataType::Date32 | DataType::Date64 => return_date_else_int(value),
                        _ => ScalarValue::Int128(self.sum / self.count as i128)
                    }
                }
            }
            AggregationOp::COUNT => ScalarValue::USize(self.count),
            AggregationOp::MIN => match T::DATA_TYPE {
                DataType::Date32 | DataType::Date64 => return_date_else_int(self.min),
                _ => ScalarValue::Int128(self.min)
            },
            AggregationOp::MAX => match T::DATA_TYPE {
                DataType::Date32 | DataType::Date64 => return_date_else_int(self.max),
                _ => ScalarValue::Int128(self.max)
            },
        }
    }
}

pub fn return_date_else_int(value: i128) -> ScalarValue {
    let date_time = utils::get_naive_date_time_from_timestamp(value);
    if let Some(date_time) = date_time {
        ScalarValue::Date(date_time)
    } else {
        ScalarValue::Int128(value)
    }
}

pub struct FloatAggregator<T: ArrowPrimitiveType> {
    column_index: usize,
    aggregation_op: AggregationOp,
    sum: f64,
    count: usize,
    min: f64,
    max: f64,

    phantom: PhantomData<fn() -> T>,
}

impl<T> FloatAggregator<T>
where 
    T: ArrowPrimitiveType,
    T::Native: Into<f64>
{
    pub fn new(column_index: usize, aggregation_op: AggregationOp) -> Self {
        Self {
            column_index,
            aggregation_op,
            sum: 0.,
            count: 0,
            min: f64::MAX,
            max: f64::MIN,
            phantom: PhantomData
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
    T: ArrowPrimitiveType,
    T::Native: Into<f64>,
{
    fn aggregate_batch(&mut self, batch: &RecordBatch) -> Result<(), ArrowError> {
        let column = batch.column(self.column_index);

        let array = column
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| {
                ArrowError::SchemaError(format!(
                        "Downcast to PrimitiveArray<{}> failed!", 
                        std::any::type_name::<T>()
                ))
            })?;

        for i in 0..array.len() {
            if array.is_valid(i) {
                let value: f64 = array.value(i).into();
                self.process_value(value);
            }
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
            AggregationOp::MIN => ScalarValue::Float64(self.min),
            AggregationOp::MAX => ScalarValue::Float64(self.max),
        }
    }
}

pub struct BooleanAggregator<T: ArrowPrimitiveType> {
    column_index: usize,
    aggregation_op: AggregationOp,
    sum: usize,
    count: usize,
    min: bool,
    max: bool,

    phantom: PhantomData<fn() -> T>,
}

impl<T> BooleanAggregator<T>
where 
    T: ArrowPrimitiveType,
    T::Native: Into<bool>,
{

    pub fn new(column_index: usize, aggregation_op: AggregationOp) -> Self {
        Self {
            column_index,
            aggregation_op,
            sum: 0,
            count: 0,
            min: true,
            max: false,
            phantom: PhantomData
        }
    }

    fn process_value(&mut self, v: bool) {
        match self.aggregation_op {
            AggregationOp::SUM | AggregationOp::AVG => {
                if v {
                    self.sum += 1;
                }
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

impl<T> Aggregator for BooleanAggregator<T>
where
    T: ArrowPrimitiveType,
    T::Native: Into<bool>,

{
    fn aggregate_batch(&mut self, batch: &RecordBatch) -> Result<(), ArrowError>{
        let column = batch.column(self.column_index);

        let array = column
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| {
                ArrowError::SchemaError(format!(
                        "Downcast to PrimitiveArray<{}> failed!", 
                        std::any::type_name::<T>()
                ))
            })?;

        for i in 0..array.len() {
            if array.is_valid(i) {
                let value: bool = array.value(i).into();
                self.process_value(value);
            }
        }
        Ok(())
    }
    fn get_result(&self) -> ScalarValue {
        match self.aggregation_op {
            AggregationOp::SUM => ScalarValue::USize(self.sum),
            AggregationOp::AVG => {
                if self.count == 0 {
                    ScalarValue::Null
                } else {
                    ScalarValue::Int128((self.sum / self.count) as i128)
                }
            }
            AggregationOp::COUNT => ScalarValue::USize(self.count),
            AggregationOp::MIN => ScalarValue::Boolean(self.min),
            AggregationOp::MAX => ScalarValue::Boolean(self.max),
        }
    }
}

pub struct StringAggregator<T: ArrowPrimitiveType> {
    column_index: usize,
    aggregation_op: AggregationOp,
    sum: String,
    count: usize,
    min: String,
    max: String,

    phantom: PhantomData<fn() -> T>,
}

impl<T> StringAggregator<T>
where 
    T: ArrowPrimitiveType,
    T::Native: Into<String>,
{

    pub fn new(column_index: usize, aggregation_op: AggregationOp) -> Self {
        Self {
            column_index,
            aggregation_op,
            sum: "".into(),
            count: 0,
            min: "".into(),
            max: "".into(),

            phantom: PhantomData
        }
    }

    fn process_value(&mut self, v: &str) {
        match self.aggregation_op {
            AggregationOp::SUM | AggregationOp::AVG => {
                self.sum.push_str(v);
                self.count += 1;
            }
            AggregationOp::COUNT => {
                self.count += 1;
            }
            AggregationOp::MIN => {
                self.min = min(self.min.clone(), v.into());
                self.count += 1;
            }
            AggregationOp::MAX => {
                self.max = max(self.max.clone(), v.into());
                self.count += 1;
            }
        }
    }
}

impl<T> Aggregator for StringAggregator<T>
where
    T: ArrowPrimitiveType,
    T::Native: Into<String>,

{
    fn aggregate_batch(&mut self, batch: &RecordBatch) -> Result<(), ArrowError>{
        let column = batch.column(self.column_index);

        let array = column
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .ok_or_else(|| {
                ArrowError::SchemaError(format!(
                        "Downcast to PrimitiveArray<{}> failed!", 
                        std::any::type_name::<T>()
                ))
            })?;

        for i in 0..array.len() {
            if array.is_valid(i) {
                let value: String = array.value(i).into();
                self.process_value(&value);
            }
        }
        Ok(())
    }
    fn get_result(&self) -> ScalarValue {
        match self.aggregation_op {
            AggregationOp::SUM | AggregationOp::AVG=> ScalarValue::String(self.sum.clone()),
            AggregationOp::COUNT => ScalarValue::USize(self.count),
            AggregationOp::MIN => ScalarValue::String(self.min.clone()),
            AggregationOp::MAX => ScalarValue::String(self.max.clone()),
        }
    }
}
