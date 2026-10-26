//! Boolean expressions and `SELECT ... FROM ... WHERE ...` statements.

use serde_json::{Map, Value, json};

use crate::error::RuleError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Path {
    segments: Vec<Seg>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Seg {
    Key(String),
    Index(usize),
}

impl Path {
    pub(crate) fn parse(text: &str) -> Result<Self, RuleError> {
        let mut parser = Parser::new(text);
        let path = parser.parse_path()?;
        parser.skip();
        if !parser.rest().is_empty() {
            return Err(parser.error("invalid field"));
        }
        Ok(path)
    }

    pub(crate) fn display(&self) -> String {
        let mut text = String::new();
        for (index, segment) in self.segments.iter().enumerate() {
            if index > 0 {
                text.push('.');
            }
            match segment {
                Seg::Key(key) => text.push_str(key),
                Seg::Index(position) => text.push_str(&position.to_string()),
            }
        }
        text
    }

    pub(crate) fn lookup(&self, root: &Value) -> Value {
        let mut current = root;
        for segment in &self.segments {
            let next = match (current, segment) {
                (Value::Object(map), Seg::Key(key)) => map.get(key),
                (Value::Array(items), Seg::Index(index)) => items.get(*index),
                _ => None,
            };
            match next {
                Some(value) => current = value,
                None => return Value::Null,
            }
        }
        current.clone()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Op {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Contains,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Expr {
    Compare { path: Path, op: Op, value: Value },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
}

impl Expr {
    pub(crate) fn parse(text: &str) -> Result<Self, RuleError> {
        let mut parser = Parser::new(text);
        let expr = parser.parse_or()?;
        parser.skip();
        if !parser.rest().is_empty() {
            return Err(parser.error("invalid condition"));
        }
        Ok(expr)
    }
}

/// Topic, condition, and actions produced by [`parse_rule_sql`].
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedRule {
    /// MQTT topic filter from the `FROM` clause.
    pub topic_filter: String,
    /// `null` when the statement has no `WHERE` clause, otherwise `{"sql": ...}`.
    pub conditions: Value,
    /// Empty for `SELECT *`, otherwise one `transform` action.
    pub actions: Value,
}

/// Parse `SELECT <fields> FROM "<filter>" [WHERE <expr>]`.
pub fn parse_rule_sql(sql: &str) -> Result<ParsedRule, RuleError> {
    let mut parser = Parser::new(sql);
    if !parser.eat_keyword("SELECT") {
        return Err(parser.error("expected SELECT"));
    }
    let projection = parser.parse_select_list()?;
    if !parser.eat_keyword("FROM") {
        return Err(parser.error("expected FROM"));
    }
    let topic_filter = parser.parse_quoted_topic()?;
    let conditions = if parser.eat_keyword("WHERE") {
        let start = parser.index;
        let _ = parser.parse_or()?;
        let clause = parser.input[start..parser.index].trim();
        json!({ "sql": clause })
    } else {
        Value::Null
    };
    parser.skip();
    if !parser.rest().is_empty() {
        return Err(parser.error("invalid rule sql"));
    }
    let actions = match projection {
        None => Value::Array(Vec::new()),
        Some(fields) => {
            let mut payload = Map::new();
            for (path, alias) in fields {
                payload.insert(alias, Value::String(format!("${{{}}}", path.display())));
            }
            json!([{ "type": "transform", "payload": Value::Object(payload) }])
        }
    };
    Ok(ParsedRule {
        topic_filter,
        conditions,
        actions,
    })
}

struct Parser<'a> {
    input: &'a str,
    index: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, index: 0 }
    }

    fn rest(&self) -> &'a str {
        &self.input[self.index..]
    }

    fn skip(&mut self) {
        let trimmed = self.rest().trim_start();
        self.index = self.input.len() - trimmed.len();
    }

    fn error(&self, message: &str) -> RuleError {
        let near: String = self.rest().chars().take(24).collect();
        RuleError::Invalid(format!("{message} near '{near}'"))
    }

    fn eat_keyword(&mut self, keyword: &str) -> bool {
        self.skip();
        let rest = self.rest();
        if rest.len() < keyword.len() || !rest[..keyword.len()].eq_ignore_ascii_case(keyword) {
            return false;
        }
        let boundary = rest[keyword.len()..].chars().next();
        if boundary.is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
            return false;
        }
        self.index += keyword.len();
        true
    }

    fn parse_or(&mut self) -> Result<Expr, RuleError> {
        let mut left = self.parse_and()?;
        while self.eat_keyword("OR") {
            let right = self.parse_and()?;
            left = Expr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, RuleError> {
        let mut left = self.parse_not()?;
        while self.eat_keyword("AND") {
            let right = self.parse_not()?;
            left = Expr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expr, RuleError> {
        if self.eat_keyword("NOT") {
            let inner = self.parse_not()?;
            return Ok(Expr::Not(Box::new(inner)));
        }
        self.skip();
        if self.rest().starts_with('(') {
            self.index += 1;
            let inner = self.parse_or()?;
            self.skip();
            if !self.rest().starts_with(')') {
                return Err(self.error("expected ')'"));
            }
            self.index += 1;
            return Ok(inner);
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<Expr, RuleError> {
        let path = self.parse_path()?;
        let op = self.parse_op()?;
        let value = self.parse_literal()?;
        Ok(Expr::Compare { path, op, value })
    }

    fn parse_op(&mut self) -> Result<Op, RuleError> {
        self.skip();
        let rest = self.rest();
        let (op, len) = if rest.starts_with(">=") {
            (Op::Gte, 2)
        } else if rest.starts_with("<=") {
            (Op::Lte, 2)
        } else if rest.starts_with("!=") || rest.starts_with("<>") {
            (Op::Ne, 2)
        } else if rest.starts_with('>') {
            (Op::Gt, 1)
        } else if rest.starts_with('<') {
            (Op::Lt, 1)
        } else if rest.starts_with('=') {
            (Op::Eq, 1)
        } else if self.eat_keyword("CONTAINS") {
            return Ok(Op::Contains);
        } else {
            return Err(self.error("expected operator"));
        };
        self.index += len;
        Ok(op)
    }

    fn parse_path(&mut self) -> Result<Path, RuleError> {
        let first = self.parse_ident()?;
        let mut segments = vec![Seg::Key(first)];
        loop {
            self.skip();
            if !self.rest().starts_with('.') {
                break;
            }
            self.index += 1;
            self.skip();
            if self
                .rest()
                .chars()
                .next()
                .is_some_and(|ch| ch.is_ascii_digit())
            {
                let digits = self.take_while(|ch| ch.is_ascii_digit());
                let index = digits
                    .parse::<usize>()
                    .map_err(|_| self.error("invalid index"))?;
                segments.push(Seg::Index(index));
            } else {
                segments.push(Seg::Key(self.parse_ident()?));
            }
        }
        Ok(Path { segments })
    }

    fn parse_ident(&mut self) -> Result<String, RuleError> {
        self.skip();
        let mut chars = self.rest().chars();
        let Some(first) = chars.next() else {
            return Err(self.error("expected field"));
        };
        if !(first.is_ascii_alphabetic() || first == '_') {
            return Err(self.error("expected field"));
        }
        let text = self.take_while(|ch| ch.is_ascii_alphanumeric() || ch == '_');
        Ok(text)
    }

    fn parse_literal(&mut self) -> Result<Value, RuleError> {
        self.skip();
        if self.eat_keyword("TRUE") {
            return Ok(Value::Bool(true));
        }
        if self.eat_keyword("FALSE") {
            return Ok(Value::Bool(false));
        }
        if self.eat_keyword("NULL") {
            return Ok(Value::Null);
        }
        if self.rest().starts_with('\'') {
            return self.parse_string();
        }
        self.parse_number()
    }

    fn parse_string(&mut self) -> Result<Value, RuleError> {
        self.index += 1;
        let mut text = String::new();
        loop {
            let Some(ch) = self.rest().chars().next() else {
                return Err(self.error("unterminated string"));
            };
            self.index += ch.len_utf8();
            if ch == '\'' {
                if self.rest().starts_with('\'') {
                    self.index += 1;
                    text.push('\'');
                    continue;
                }
                break;
            }
            text.push(ch);
        }
        Ok(Value::String(text))
    }

    fn parse_number(&mut self) -> Result<Value, RuleError> {
        self.skip();
        let start = self.index;
        if self.rest().starts_with('-') {
            self.index += 1;
        }
        let digits = self.take_while(|ch| ch.is_ascii_digit());
        if digits.is_empty() {
            self.index = start;
            return Err(self.error("expected value"));
        }
        if self.rest().starts_with('.') {
            self.index += 1;
            let fraction = self.take_while(|ch| ch.is_ascii_digit());
            if fraction.is_empty() {
                return Err(self.error("expected fraction"));
            }
        }
        let text = &self.input[start..self.index];
        if text.contains('.') {
            let number = text
                .parse::<f64>()
                .ok()
                .and_then(serde_json::Number::from_f64)
                .ok_or_else(|| self.error("invalid number"))?;
            return Ok(Value::Number(number));
        }
        let number = text
            .parse::<i64>()
            .map(serde_json::Number::from)
            .map_err(|_| self.error("invalid number"))?;
        Ok(Value::Number(number))
    }

    fn parse_select_list(&mut self) -> Result<Option<Vec<(Path, String)>>, RuleError> {
        self.skip();
        if self.rest().starts_with('*') {
            self.index += 1;
            return Ok(None);
        }
        let mut fields = Vec::new();
        loop {
            let path = self.parse_path()?;
            let alias = if self.eat_keyword("AS") {
                self.parse_ident()?
            } else {
                match path.segments.last() {
                    Some(Seg::Key(key)) => key.clone(),
                    Some(Seg::Index(index)) => index.to_string(),
                    None => return Err(self.error("expected field")),
                }
            };
            fields.push((path, alias));
            self.skip();
            if self.rest().starts_with(',') {
                self.index += 1;
                continue;
            }
            break;
        }
        if fields.is_empty() {
            return Err(self.error("expected select list"));
        }
        Ok(Some(fields))
    }

    fn parse_quoted_topic(&mut self) -> Result<String, RuleError> {
        self.skip();
        if !self.rest().starts_with('"') {
            return Err(self.error("expected topic filter"));
        }
        self.index += 1;
        let mut text = String::new();
        loop {
            let Some(ch) = self.rest().chars().next() else {
                return Err(self.error("unterminated topic filter"));
            };
            self.index += ch.len_utf8();
            if ch == '"' {
                if self.rest().starts_with('"') {
                    self.index += 1;
                    text.push('"');
                    continue;
                }
                break;
            }
            text.push(ch);
        }
        if text.is_empty() {
            return Err(self.error("empty topic filter"));
        }
        Ok(text)
    }

    fn take_while(&mut self, mut predicate: impl FnMut(char) -> bool) -> String {
        let mut text = String::new();
        for ch in self.rest().chars() {
            if !predicate(ch) {
                break;
            }
            text.push(ch);
            self.index += ch.len_utf8();
        }
        text
    }
}
