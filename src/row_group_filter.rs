use std::error::Error;

use parquet::file::metadata::RowGroupMetaData;

use crate::{
    bloom_filter::BloomFilter,
    utils::{self, ColumnMaps, Comparison, Expression, ThresholdValue},
};

pub fn keep_row_group(
    row_group: &RowGroupMetaData,
    bloom_filters: &Vec<Option<BloomFilter>>,
    row_group_index: usize,
    expression: &Expression,
    not: bool,
    column_maps: &ColumnMaps,
) -> Result<bool, Box<dyn Error>> {
    match expression {
        Expression::Condition(condition) => {
            if let Some((column_index, column)) = row_group
                .columns()
                .iter()
                .enumerate()
                .find(|(_, c)| c.column_path().string() == condition.column_name)
            {
                let column_type = column.column_type().to_string();

                let bloom_filter = match bloom_filters.get(column_index) {
                    Some(v) => v,
                    None => &None,
                };

                if let Some(stats) = column.statistics() {
                    if let (Some(min_bytes), Some(max_bytes)) =
                        (stats.min_bytes_opt(), stats.max_bytes_opt())
                    {
                        let min_value = utils::bytes_to_value(min_bytes, &column_type)?;
                        let max_value = utils::bytes_to_value(max_bytes, &column_type)?;
                        let mut result = condition.comparison.keep_row_group(
                            &min_value,
                            &max_value,
                            &condition.threshold,
                            not,
                        );

                        if result && condition.comparison == Comparison::Equal {
                            if let Some(bloom_filter) = bloom_filter {
                                result = match &condition.threshold {
                                    ThresholdValue::Int64(v) => bloom_filter.contains(&v),
                                    ThresholdValue::Boolean(v) => bloom_filter.contains(&v),
                                    ThresholdValue::Utf8String(v) => bloom_filter.contains(&v),
                                    ThresholdValue::Float64(v) => {
                                        let value = *v as i32;
                                        bloom_filter.contains(&value)
                                    }
                                };
                            }
                        }

                        return Ok(result);
                    }
                }
            }
            Ok(false)
        }
        Expression::And(left, right) => Ok(match not {
            true => {
                keep_row_group(
                    row_group,
                    bloom_filters,
                    row_group_index,
                    left,
                    true,
                    column_maps,
                )? || keep_row_group(
                    row_group,
                    bloom_filters,
                    row_group_index,
                    right,
                    true,
                    column_maps,
                )?
            }
            false => {
                keep_row_group(
                    row_group,
                    bloom_filters,
                    row_group_index,
                    left,
                    false,
                    column_maps,
                )? && keep_row_group(
                    row_group,
                    bloom_filters,
                    row_group_index,
                    right,
                    false,
                    column_maps,
                )?
            }
        }),
        Expression::Or(left, right) => Ok(match not {
            true => {
                keep_row_group(
                    row_group,
                    bloom_filters,
                    row_group_index,
                    left,
                    true,
                    column_maps,
                )? && keep_row_group(
                    row_group,
                    bloom_filters,
                    row_group_index,
                    right,
                    true,
                    column_maps,
                )?
            }
            false => {
                keep_row_group(
                    row_group,
                    bloom_filters,
                    row_group_index,
                    left,
                    false,
                    column_maps,
                )? || keep_row_group(
                    row_group,
                    bloom_filters,
                    row_group_index,
                    right,
                    false,
                    column_maps,
                )?
            }
        }),
        Expression::Not(inner) => Ok(keep_row_group(
            row_group,
            bloom_filters,
            row_group_index,
            inner,
            !not,
            column_maps,
        )?),
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
            ) => match self {
                Comparison::LessThan => true,
                Comparison::LessThanOrEqual => true,
                Comparison::Equal => match not {
                    false => v == min || v == max,
                    true => v != min || v != max,
                },
                Comparison::GreaterThanOrEqual => true,
                Comparison::GreaterThan => true,
            },
            (
                ThresholdValue::Utf8String(min),
                ThresholdValue::Utf8String(max),
                ThresholdValue::Utf8String(v),
            ) => compare(min, max, v, self, not),
            _ => true,
        }
    }
}
