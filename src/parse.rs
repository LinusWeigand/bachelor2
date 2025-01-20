use std::error::Error;
use std::str::FromStr;

#[derive(Debug)]
enum ComparisonOp {
    LessThan,
    LessThanOrEqual,
    Equal,
    GreaterThanOrEqual,
    GreaterThan,
}

impl ComparisonOp {
    fn from_str(op: &str) -> Option<Self> {
        match op {
            "<" => Some(ComparisonOp::LessThan),
            "<=" => Some(ComparisonOp::LessThanOrEqual),
            "==" => Some(ComparisonOp::Equal),
            ">=" => Some(ComparisonOp::GreaterThanOrEqual),
            ">" => Some(ComparisonOp::GreaterThan),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct Condition {
    column_name: String,
    threshold: f64,
    comparison: ComparisonOp,
}

#[derive(Debug)]
enum BooleanExpr {
    Condition(Condition),
    And(Box<BooleanExpr>, Box<BooleanExpr>),
    Or(Box<BooleanExpr>, Box<BooleanExpr>),
    Not(Box<BooleanExpr>),
}

fn parse_expression(input: &str) -> Result<BooleanExpr, Box<dyn Error>> {
    let tokens = tokenize(input)?;
    let mut pos = 0;
    parse_or(&tokens, &mut pos)
}

fn tokenize(input: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let mut tokens = Vec::new();
    let mut current = String::new();

    for c in input.chars() {
        match c {
            '(' | ')' | ' ' => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
                if c != ' ' {
                    tokens.push(c.to_string());
                }
            }
            _ => current.push(c),
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    Ok(tokens)
}

fn parse_or(tokens: &[String], pos: &mut usize) -> Result<BooleanExpr, Box<dyn Error>> {
    let mut expr = parse_and(tokens, pos)?;

    while *pos < tokens.len() && tokens[*pos] == "OR" {
        *pos += 1;
        let right = parse_and(tokens, pos)?;
        expr = BooleanExpr::Or(Box::new(expr), Box::new(right));
    }

    Ok(expr)
}

fn parse_and(tokens: &[String], pos: &mut usize) -> Result<BooleanExpr, Box<dyn Error>> {
    let mut expr = parse_not(tokens, pos)?;

    while *pos < tokens.len() && tokens[*pos] == "AND" {
        *pos += 1;
        let right = parse_not(tokens, pos)?;
        expr = BooleanExpr::And(Box::new(expr), Box::new(right));
    }

    Ok(expr)
}

fn parse_not(tokens: &[String], pos: &mut usize) -> Result<BooleanExpr, Box<dyn Error>> {
    if *pos < tokens.len() && tokens[*pos] == "NOT" {
        *pos += 1;
        let expr = parse_primary(tokens, pos)?;
        return Ok(BooleanExpr::Not(Box::new(expr)));
    }

    parse_primary(tokens, pos)
}

fn parse_primary(tokens: &[String], pos: &mut usize) -> Result<BooleanExpr, Box<dyn Error>> {
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

    let comparison = ComparisonOp::from_str(&tokens[*pos]).ok_or("Invalid comparison operator")?;
    *pos += 1;

    if *pos >= tokens.len() {
        return Err("Expected threshold value".into());
    }

    let threshold: f64 = tokens[*pos].parse()?;
    *pos += 1;

    Ok(BooleanExpr::Condition(Condition {
        column_name,
        comparison,
        threshold,
    }))
}

fn main() -> Result<(), Box<dyn Error>> {
    let input = "(age >= 65) AND (age < 18 OR age <= 17) OR age == 20";
    let expr = parse_expression(input)?;
    println!("Parsed Expression: {:?}", expr);
    Ok(())
}
