//! Compile JSON conditions and actions, then evaluate them against one event.

use std::cmp::Ordering;

use serde_json::{Map, Value};

use crate::error::RuleError;
use crate::parse::{Expr, Op, Path};

#[derive(Debug, Clone)]
pub(crate) enum Condition {
    Always,
    Expr(Expr),
    All(Vec<Clause>),
    Any(Vec<Clause>),
}

#[derive(Debug, Clone)]
pub(crate) struct Clause {
    path: Path,
    op: Op,
    value: Value,
}

#[derive(Debug, Clone)]
pub(crate) enum Action {
    Log,
    Drop,
    Reject,
    Republish {
        topic: String,
        payload: Option<Value>,
        qos: u8,
        retain: bool,
    },
    Webhook {
        url: String,
        body: Option<Value>,
    },
    Transform {
        payload: Value,
    },
}

pub(crate) fn compile_condition(value: &Value) -> Result<Condition, RuleError> {
    match value {
        Value::Null => Ok(Condition::Always),
        Value::Object(map) if map.is_empty() => Ok(Condition::Always),
        Value::Object(map) => compile_object(map),
        _ => Err(RuleError::Invalid(
            "conditions must be a JSON object".to_owned(),
        )),
    }
}

fn compile_object(map: &Map<String, Value>) -> Result<Condition, RuleError> {
    if map.contains_key("sql") {
        if map.len() != 1 {
            return Err(RuleError::Invalid(
                "sql conditions cannot include other fields".to_owned(),
            ));
        }
        let Some(sql) = map.get("sql").and_then(Value::as_str) else {
            return Err(RuleError::Invalid(
                "sql condition must be a string".to_owned(),
            ));
        };
        if sql.trim().is_empty() {
            return Err(RuleError::Invalid("sql condition is empty".to_owned()));
        }
        return Ok(Condition::Expr(Expr::parse(sql)?));
    }
    if let Some(items) = map.get("all") {
        if map.len() != 1 {
            return Err(RuleError::Invalid(
                "conditions must be sql, all, or any".to_owned(),
            ));
        }
        return Ok(Condition::All(compile_clauses(items)?));
    }
    if let Some(items) = map.get("any") {
        if map.len() != 1 {
            return Err(RuleError::Invalid(
                "conditions must be sql, all, or any".to_owned(),
            ));
        }
        return Ok(Condition::Any(compile_clauses(items)?));
    }
    Err(RuleError::Invalid(
        "conditions must be sql, all, or any".to_owned(),
    ))
}

fn compile_clauses(value: &Value) -> Result<Vec<Clause>, RuleError> {
    let Some(items) = value.as_array() else {
        return Err(RuleError::Invalid(
            "condition group must be an array".to_owned(),
        ));
    };
    items.iter().map(compile_clause).collect()
}

fn compile_clause(value: &Value) -> Result<Clause, RuleError> {
    let Some(map) = value.as_object() else {
        return Err(RuleError::Invalid(
            "condition clause must be an object".to_owned(),
        ));
    };
    let Some(field) = map.get("field").and_then(Value::as_str) else {
        return Err(RuleError::Invalid("condition field is required".to_owned()));
    };
    let Some(op) = map.get("op").and_then(Value::as_str) else {
        return Err(RuleError::Invalid("condition op is required".to_owned()));
    };
    let Some(literal) = map.get("value") else {
        return Err(RuleError::Invalid("condition value is required".to_owned()));
    };
    Ok(Clause {
        path: Path::parse(field)?,
        op: parse_op_name(op)?,
        value: literal.clone(),
    })
}

fn parse_op_name(op: &str) -> Result<Op, RuleError> {
    match op {
        "eq" => Ok(Op::Eq),
        "ne" => Ok(Op::Ne),
        "gt" => Ok(Op::Gt),
        "gte" => Ok(Op::Gte),
        "lt" => Ok(Op::Lt),
        "lte" => Ok(Op::Lte),
        "contains" => Ok(Op::Contains),
        _ => Err(RuleError::Invalid("unknown condition operator".to_owned())),
    }
}

pub(crate) fn compile_actions(value: &Value) -> Result<Vec<Action>, RuleError> {
    let Some(items) = value.as_array() else {
        return Err(RuleError::Invalid(
            "actions must be a JSON array".to_owned(),
        ));
    };
    items.iter().map(compile_action).collect()
}

fn compile_action(value: &Value) -> Result<Action, RuleError> {
    let Some(map) = value.as_object() else {
        return Err(RuleError::Invalid("action must be an object".to_owned()));
    };
    let Some(kind) = map.get("type").and_then(Value::as_str) else {
        return Err(RuleError::Invalid("action type is required".to_owned()));
    };
    match kind {
        "log" => Ok(Action::Log),
        "drop" => Ok(Action::Drop),
        "reject" => Ok(Action::Reject),
        "republish" => {
            let Some(topic) = map.get("topic").and_then(Value::as_str) else {
                return Err(RuleError::Invalid("republish topic is required".to_owned()));
            };
            if topic.trim().is_empty() {
                return Err(RuleError::Invalid("republish topic is invalid".to_owned()));
            }
            if !topic.contains("${") && !heron_core::valid_topic_name(topic) {
                return Err(RuleError::Invalid("republish topic is invalid".to_owned()));
            }
            let qos = match map.get("qos") {
                None => 0,
                Some(Value::Number(number)) => match number.as_u64() {
                    Some(0) => 0,
                    Some(1) => 1,
                    Some(2) => 2,
                    _ => {
                        return Err(RuleError::Invalid(
                            "republish qos must be 0, 1, or 2".into(),
                        ));
                    }
                },
                Some(_) => {
                    return Err(RuleError::Invalid(
                        "republish qos must be 0, 1, or 2".into(),
                    ));
                }
            };
            let retain = match map.get("retain") {
                None => false,
                Some(Value::Bool(flag)) => *flag,
                Some(_) => {
                    return Err(RuleError::Invalid(
                        "republish retain must be a boolean".into(),
                    ));
                }
            };
            Ok(Action::Republish {
                topic: topic.to_owned(),
                payload: map.get("payload").cloned(),
                qos,
                retain,
            })
        }
        "webhook" => {
            let Some(url) = map.get("url").and_then(Value::as_str) else {
                return Err(RuleError::Invalid("webhook url is required".to_owned()));
            };
            if url.trim().is_empty() {
                return Err(RuleError::Invalid("webhook url is required".to_owned()));
            }
            if !url.contains("${") && !is_http_url(url) {
                return Err(RuleError::Invalid(
                    "webhook url must be http or https".to_owned(),
                ));
            }
            Ok(Action::Webhook {
                url: url.to_owned(),
                body: map.get("payload").cloned(),
            })
        }
        "transform" => {
            let Some(payload) = map.get("payload") else {
                return Err(RuleError::Invalid(
                    "transform payload is required".to_owned(),
                ));
            };
            Ok(Action::Transform {
                payload: payload.clone(),
            })
        }
        _ => Err(RuleError::Invalid("unknown action type".to_owned())),
    }
}

fn is_http_url(text: &str) -> bool {
    reqwest::Url::parse(text).is_ok_and(|url| matches!(url.scheme(), "http" | "https"))
}

pub(crate) fn condition_matches(condition: &Condition, context: &Value) -> bool {
    match condition {
        Condition::Always => true,
        Condition::Expr(expr) => expr_matches(expr, context),
        Condition::All(clauses) => clauses.iter().all(|clause| clause_matches(clause, context)),
        Condition::Any(clauses) => clauses.iter().any(|clause| clause_matches(clause, context)),
    }
}

fn expr_matches(expr: &Expr, context: &Value) -> bool {
    match expr {
        Expr::Compare { path, op, value } => compare(&path.lookup(context), *op, value),
        Expr::And(left, right) => expr_matches(left, context) && expr_matches(right, context),
        Expr::Or(left, right) => expr_matches(left, context) || expr_matches(right, context),
        Expr::Not(inner) => !expr_matches(inner, context),
    }
}

fn clause_matches(clause: &Clause, context: &Value) -> bool {
    compare(&clause.path.lookup(context), clause.op, &clause.value)
}

fn compare(left: &Value, op: Op, right: &Value) -> bool {
    match op {
        Op::Eq => values_equal(left, right),
        Op::Ne => !values_equal(left, right),
        Op::Gt => ordered(left, right) == Some(Ordering::Greater),
        Op::Gte => matches!(
            ordered(left, right),
            Some(Ordering::Greater | Ordering::Equal)
        ),
        Op::Lt => ordered(left, right) == Some(Ordering::Less),
        Op::Lte => matches!(ordered(left, right), Some(Ordering::Less | Ordering::Equal)),
        Op::Contains => contains(left, right),
    }
}

fn values_equal(left: &Value, right: &Value) -> bool {
    if let (Some(left), Some(right)) = (left.as_f64(), right.as_f64()) {
        return left == right;
    }
    left == right
}

fn ordered(left: &Value, right: &Value) -> Option<Ordering> {
    if let (Some(left), Some(right)) = (left.as_f64(), right.as_f64()) {
        return left.partial_cmp(&right);
    }
    if let (Some(left), Some(right)) = (left.as_str(), right.as_str()) {
        return Some(left.cmp(right));
    }
    None
}

fn contains(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::String(text), Value::String(needle)) => text.contains(needle.as_str()),
        (Value::Array(items), value) => items.iter().any(|item| values_equal(item, value)),
        _ => false,
    }
}

pub(crate) fn render_json(template: &Value, context: &Value) -> Value {
    match template {
        Value::String(text) => render_string_value(text, context),
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| render_json(item, context))
                .collect(),
        ),
        Value::Object(map) => {
            let mut rendered = Map::new();
            for (key, value) in map {
                rendered.insert(key.clone(), render_json(value, context));
            }
            Value::Object(rendered)
        }
        other => other.clone(),
    }
}

fn render_string_value(text: &str, context: &Value) -> Value {
    if let Some(path) = single_placeholder(text.trim()) {
        return match Path::parse(path) {
            Ok(path) => path.lookup(context),
            Err(_) => Value::Null,
        };
    }
    if text.contains("${") {
        Value::String(render_text(text, context))
    } else {
        Value::String(text.to_owned())
    }
}

pub(crate) fn render_text(input: &str, context: &Value) -> String {
    let mut output = String::new();
    let mut rest = input;
    while let Some(start) = rest.find("${") {
        output.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find('}') else {
            output.push_str(&rest[start..]);
            return output;
        };
        let rendered = match Path::parse(after[..end].trim()) {
            Ok(path) => display_value(&path.lookup(context)),
            Err(_) => String::new(),
        };
        output.push_str(&rendered);
        rest = &after[end + 1..];
    }
    output.push_str(rest);
    output
}

fn single_placeholder(text: &str) -> Option<&str> {
    let inner = text.strip_prefix("${")?.strip_suffix('}')?;
    if inner.contains("${") || inner.contains('}') {
        return None;
    }
    Some(inner.trim())
}

fn display_value(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(text) => text.clone(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => number.to_string(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}
