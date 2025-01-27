use std::error::Error;
use crate::{aggregation::{Aggregation, AggregationOp}, utils};

pub fn parse_aggregation(input: &str) -> Result<Aggregation, Box<dyn Error>> {
    let tokens = utils::tokenize(input)?;
    let aggregation_op = match tokens[0].as_str() {
        "SUM" => AggregationOp::SUM,
        "AVG" => AggregationOp::AVG,
        "COUNT" => AggregationOp::COUNT,
        "MIN" => AggregationOp::MIN,
        "MAX" => AggregationOp::MAX,
        _ => {
            return Err(format!("Invalid Operation: {}", tokens[0]).into());
        }
    };

    if tokens[1] != "(" || tokens[3] != ")" {
        return Err("Expected format: SUM(column_name)".into());
    }

    let column_name = (&tokens[2]).to_owned();
    
    Ok(Aggregation {
        column_name,
        aggregation_op,
    })
}
