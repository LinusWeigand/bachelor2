use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};

use std::{error::Error, i64};

use crate::utils::{self, Comparison, Condition, Expression, ThresholdValue};

pub fn parse_expression(input: &str) -> Result<Expression, Box<dyn Error + Send + Sync>> {
    let tokens = utils::tokenize(input)?;
    let mut pos = 0;
    parse_or(&tokens, &mut pos)
}

pub fn parse_or(
    tokens: &[String],
    pos: &mut usize,
) -> Result<Expression, Box<dyn Error + Send + Sync>> {
    let mut expr = parse_and(tokens, pos)?;

    while *pos < tokens.len() && tokens[*pos] == "OR" {
        *pos += 1;
        let right = parse_and(tokens, pos)?;
        expr = Expression::Or(Box::new(expr), Box::new(right));
    }

    Ok(expr)
}

pub fn parse_and(
    tokens: &[String],
    pos: &mut usize,
) -> Result<Expression, Box<dyn Error + Send + Sync>> {
    let mut expr = parse_not(tokens, pos)?;

    while *pos < tokens.len() && tokens[*pos] == "AND" {
        *pos += 1;
        let right = parse_not(tokens, pos)?;
        expr = Expression::And(Box::new(expr), Box::new(right));
    }

    Ok(expr)
}

pub fn parse_not(
    tokens: &[String],
    pos: &mut usize,
) -> Result<Expression, Box<dyn Error + Send + Sync>> {
    if *pos < tokens.len() && tokens[*pos] == "NOT" {
        *pos += 1;
        let expr = parse_primary(tokens, pos)?;
        return Ok(Expression::Not(Box::new(expr)));
    }

    parse_primary(tokens, pos)
}

pub fn parse_primary(
    tokens: &[String],
    pos: &mut usize,
) -> Result<Expression, Box<dyn Error + Send + Sync>> {
    if *pos >= tokens.len() {
        return Err("Unexpected end of input".into());
    }

    if tokens[*pos] == "(" {
        *pos += 1;
        let expr = parse_or(tokens, pos)?;
        if *pos >= tokens.len() || tokens[*pos] != ")" {
            return Err("Expected closing parenthesis".into());
        }
        *pos += 1;
        return Ok(expr);
    }

    // Parse condition
    let column_name = tokens[*pos].clone();
    *pos += 1;

    if *pos >= tokens.len() {
        return Err("Expected comparison operator".into());
    }

    let comparison = Comparison::from_str(&tokens[*pos]).ok_or("Invalid comparison operator")?;
    *pos += 1;

    if *pos >= tokens.len() {
        return Err("Expected threshold value".into());
    }

    let threshold_token = &tokens[*pos];
    *pos += 1;

    let threshold = if let Ok(datetime) = parse_iso_datetime(threshold_token) {
        ThresholdValue::Int64(datetime)
    } else if let Ok(bool) = threshold_token.parse::<bool>() {
        ThresholdValue::Boolean(bool)
    } else if threshold_token.contains('.') {
        if let Ok(num) = threshold_token.parse::<f64>() {
            ThresholdValue::Float64(num)
        } else {
            ThresholdValue::Utf8String(threshold_token.to_owned())
        }
    } else if let Ok(num) = threshold_token.parse::<i64>() {
        ThresholdValue::Int64(num)
    } else if let Ok(datetime) = parse_iso_datetime(threshold_token) {
        ThresholdValue::Int64(datetime)
    } else {
        ThresholdValue::Utf8String(threshold_token.to_owned())
    };

    Ok(Expression::Condition(Condition {
        column_name,
        comparison,
        threshold,
    }))
}

pub fn parse_iso_datetime(s: &str) -> Result<i64, chrono::ParseError> {
    let naive_date_time = NaiveDateTime::parse_from_str(s, "%Y-%m-%d-%H:%M:%S")?;
    let utc_date_time: DateTime<Utc> = Utc.from_utc_datetime(&naive_date_time);
    let timestamp = utc_date_time.timestamp_millis();
    println!("Parsed timestamp: {}", timestamp);
    Ok(timestamp)
}
