use std::error::Error;

use parquet2::metadata::RowGroupMetaData;

use crate::{
    bloom_filter::BloomFilter,
    utils::{self, get_min_max_threshold, Comparison, Expression, ThresholdValue},
    Mode,
};

pub fn keep_row_group(
    row_group_metadata: &RowGroupMetaData,
    bloom_filters: &Option<Vec<Option<BloomFilter>>>,
    expression: &Expression,
    not: bool,
    mode: &Mode,
) -> Result<bool, Box<dyn Error + Send + Sync>> {
    match expression {
        Expression::Condition(condition) => {
            if let Some((column_index, column, _)) = row_group_metadata
                .columns()
                .iter()
                .enumerate()
                .filter_map(|(i, c)| {
                    match c.descriptor().path_in_schema.first() {
                        Some(v) => Some((i, c, v)),
                        None => None,
                    }})
                
                .find(|(_, _, column_name)| *column_name == &condition.column_name)
            {
                let stats = match column.statistics() {
                    Some(Ok(v)) => v,
                    _ => return Ok(true),
                };
                let (min_value, max_value) = match get_min_max_threshold(&stats) {
                    Some((min, max)) => (min, max),
                    _ => return Ok(true),
                };


                let mut result = condition.comparison.keep_row_group(
                    &min_value,
                    &max_value,
                    &condition.threshold,
                    not,
                );

                if mode == &Mode::Base || mode == &Mode::Group {
                    return Ok(result);
                }

                if !result || condition.comparison != Comparison::Equal {
                    return Ok(result);
                }

                let bloom_filter = match bloom_filters {
                    Some(v) => match v.get(column_index) {
                        Some(Some(x)) => x,
                        _ => return Ok(result),
                    },
                    _ => return Ok(result),
                };

                result = match &condition.threshold {
                    ThresholdValue::Int64(v) => bloom_filter.contains(&v),
                    ThresholdValue::Boolean(v) => bloom_filter.contains(&v),
                    ThresholdValue::Utf8String(v) => bloom_filter.contains(&v),
                    ThresholdValue::Float64(v) => {
                        let value = *v as i32;
                        bloom_filter.contains(&value)
                    }
                };

                return Ok(result);
            }
            Ok(true)
        }
        Expression::And(left, right) => Ok(match not {
            true => {
                keep_row_group(row_group_metadata, bloom_filters, left, true, mode)?
                    || keep_row_group(row_group_metadata, bloom_filters, right, true, mode)?
            }
            false => {
                keep_row_group(row_group_metadata, bloom_filters, left, false, mode)?
                    && keep_row_group(row_group_metadata, bloom_filters, right, false, mode)?
            }
        }),
        Expression::Or(left, right) => Ok(match not {
            true => {
                keep_row_group(row_group_metadata, bloom_filters, left, true, mode)?
                    && keep_row_group(row_group_metadata, bloom_filters, right, true, mode)?
            }
            false => {
                keep_row_group(row_group_metadata, bloom_filters, left, false, mode)?
                    || keep_row_group(row_group_metadata, bloom_filters, right, false, mode)?
            }
        }),
        Expression::Not(inner) => Ok(keep_row_group(row_group_metadata, bloom_filters, inner, !not, mode)?),
    }
}
pub fn compare<T: Ord>(min: T, max: T, v: T, comparison: &Comparison, not: bool) -> bool {
    match comparison {
        Comparison::LessThan => match not {
            false => min < v,
            true => max >= v,
        },
        Comparison::LessThanOrEqual => match not {
            false => min <= v,
            true => max > v,
        },
        Comparison::Equal => match not {
            false => v >= min && v <= max,
            true => !(v == min && v == max),
        },
        Comparison::GreaterThanOrEqual => match not {
            false => max >= v,
            true => min < v,
        },
        Comparison::GreaterThan => match not {
            false => max > v,
            true => min <= v,
        },
    }
}

pub fn compare_floats<T: utils::Float>(
    min: T,
    max: T,
    v: T,
    comparison: &Comparison,
    not: bool,
) -> bool {
    match comparison {
        Comparison::LessThan => match not {
            false => min < v,
            true => max >= v,
        },
        Comparison::LessThanOrEqual => match not {
            false => min <= v,
            true => max > v,
        },
        Comparison::Equal => match not {
            false => v >= min && v <= max,
            true => !(v.equal(min) && v.equal(max)),
        },
        Comparison::GreaterThanOrEqual => match not {
            false => max >= v,
            true => min < v,
        },
        Comparison::GreaterThan => match not {
            false => max > v,
            true => min <= v,
        },
    }
}

impl Comparison {
    pub fn keep_row_group(
        &self,
        row_group_min: &ThresholdValue,
        row_group_max: &ThresholdValue,
        user_threshold: &ThresholdValue,
        not: bool,
    ) -> bool {
        match (row_group_min, row_group_max, user_threshold) {
            (ThresholdValue::Int64(min), ThresholdValue::Int64(max), ThresholdValue::Int64(v)) => {
                compare(min, max, v, self, not)
            }
            (
                ThresholdValue::Float64(min),
                ThresholdValue::Float64(max),
                ThresholdValue::Float64(v),
            ) => compare_floats(*min, *max, *v, self, not),
            (
                ThresholdValue::Boolean(min),
                ThresholdValue::Boolean(max),
                ThresholdValue::Boolean(v),
            ) => {
                match self {
                    Comparison::LessThan => true,
                    Comparison::LessThanOrEqual => true,
                    Comparison::Equal => match not {
                        false => v == min || v == max,
                        true => !(v == min && v == max),
                    },
                    Comparison::GreaterThanOrEqual => true,
                    Comparison::GreaterThan => true,
                }
            }
            (
                ThresholdValue::Utf8String(min),
                ThresholdValue::Utf8String(max),
                ThresholdValue::Utf8String(v),
            ) => compare(min, max, v, self, not),
            _ => true,
        }
    }
}
