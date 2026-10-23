//! In-memory rule configuration.
//!
//! These records match the management API in the design docs. The rules engine
//! does not evaluate them; successful responses include `"engine": "inactive"`.
//! The store lives in the API process and is empty again after a restart.

use std::sync::Mutex;

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use chrono::{SecondsFormat, Utc};
use heron_core::{FilterClass, classify_filter};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{AppState, error_response};

pub(crate) struct RuleStore {
    next_id: u64,
    rules: Vec<Rule>,
}

impl RuleStore {
    pub(crate) fn new() -> Self {
        Self {
            next_id: 1,
            rules: Vec::new(),
        }
    }
}

#[derive(Clone, Serialize)]
struct Rule {
    id: u64,
    name: String,
    description: Option<String>,
    enabled: bool,
    priority: i32,
    topic_filter: String,
    conditions: Value,
    actions: Value,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct RuleBody {
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default = "default_enabled")]
    enabled: bool,
    #[serde(default)]
    priority: i32,
    topic_filter: String,
    #[serde(default)]
    conditions: Value,
    actions: Value,
}

fn default_enabled() -> bool {
    true
}

pub(crate) async fn list_rules(State(state): State<AppState>) -> Response {
    let store = lock_rules(&state.rules);
    let mut rules = store.rules.clone();
    rules.sort_by(|left, right| {
        right
            .priority
            .cmp(&left.priority)
            .then_with(|| left.name.cmp(&right.name))
            .then_with(|| left.id.cmp(&right.id))
    });
    rule_ok(json!(rules))
}

pub(crate) async fn create_rule(
    State(state): State<AppState>,
    body: Result<Json<RuleBody>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Json(body) = match body {
        Ok(body) => body,
        Err(_) => {
            return error_response(StatusCode::BAD_REQUEST, "bad_request", "invalid JSON body");
        }
    };
    let mut store = lock_rules(&state.rules);
    let draft = match validate(&body, &store.rules, None) {
        Ok(draft) => draft,
        Err(message) => {
            return error_response(StatusCode::BAD_REQUEST, "bad_request", message);
        }
    };
    let id = store.next_id;
    store.next_id = store.next_id.saturating_add(1);
    let now = timestamp();
    let rule = Rule {
        id,
        name: draft.name,
        description: draft.description,
        enabled: draft.enabled,
        priority: draft.priority,
        topic_filter: draft.topic_filter,
        conditions: draft.conditions,
        actions: draft.actions,
        created_at: now.clone(),
        updated_at: now,
    };
    store.rules.push(rule.clone());
    rule_ok(json!(rule))
}

pub(crate) async fn update_rule(
    State(state): State<AppState>,
    Path(rule_id): Path<String>,
    body: Result<Json<RuleBody>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Ok(rule_id) = rule_id.parse::<u64>() else {
        return error_response(StatusCode::BAD_REQUEST, "bad_request", "invalid rule id");
    };
    let Json(body) = match body {
        Ok(body) => body,
        Err(_) => {
            return error_response(StatusCode::BAD_REQUEST, "bad_request", "invalid JSON body");
        }
    };
    let mut store = lock_rules(&state.rules);
    let Some(index) = store.rules.iter().position(|rule| rule.id == rule_id) else {
        return error_response(StatusCode::NOT_FOUND, "not_found", "resource not found");
    };
    let draft = match validate(&body, &store.rules, Some(rule_id)) {
        Ok(draft) => draft,
        Err(message) => {
            return error_response(StatusCode::BAD_REQUEST, "bad_request", message);
        }
    };
    let created_at = store.rules[index].created_at.clone();
    let rule = Rule {
        id: rule_id,
        name: draft.name,
        description: draft.description,
        enabled: draft.enabled,
        priority: draft.priority,
        topic_filter: draft.topic_filter,
        conditions: draft.conditions,
        actions: draft.actions,
        created_at,
        updated_at: timestamp(),
    };
    store.rules[index] = rule.clone();
    rule_ok(json!(rule))
}

pub(crate) async fn delete_rule(
    State(state): State<AppState>,
    Path(rule_id): Path<String>,
) -> Response {
    let Ok(rule_id) = rule_id.parse::<u64>() else {
        return error_response(StatusCode::BAD_REQUEST, "bad_request", "invalid rule id");
    };
    let mut store = lock_rules(&state.rules);
    let Some(index) = store.rules.iter().position(|rule| rule.id == rule_id) else {
        return error_response(StatusCode::NOT_FOUND, "not_found", "resource not found");
    };
    store.rules.remove(index);
    rule_ok(json!({ "id": rule_id, "deleted": true }))
}

struct Draft {
    name: String,
    description: Option<String>,
    enabled: bool,
    priority: i32,
    topic_filter: String,
    conditions: Value,
    actions: Value,
}

fn validate(
    body: &RuleBody,
    rules: &[Rule],
    ignore_id: Option<u64>,
) -> Result<Draft, &'static str> {
    let name = body.name.trim();
    if name.is_empty() || name.len() > 255 {
        return Err("name must be 1 to 255 characters");
    }
    if rules
        .iter()
        .any(|rule| rule.name == name && Some(rule.id) != ignore_id)
    {
        return Err("rule name already exists");
    }
    let topic_filter = body.topic_filter.trim();
    match classify_filter(topic_filter) {
        Ok(()) => {}
        Err(FilterClass::Invalid) => return Err("invalid topic filter"),
        Err(FilterClass::Shared) => return Err("shared subscriptions are not supported"),
    }
    if !matches!(body.conditions, Value::Null | Value::Object(_)) {
        return Err("conditions must be a JSON object");
    }
    if !body.actions.is_array() {
        return Err("actions must be a JSON array");
    }
    let description = body
        .description
        .as_deref()
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_owned);
    if description.as_ref().is_some_and(|text| text.len() > 2_000) {
        return Err("description must be at most 2000 characters");
    }
    Ok(Draft {
        name: name.to_owned(),
        description,
        enabled: body.enabled,
        priority: body.priority,
        topic_filter: topic_filter.to_owned(),
        conditions: body.conditions.clone(),
        actions: body.actions.clone(),
    })
}

fn lock_rules(rules: &Mutex<RuleStore>) -> std::sync::MutexGuard<'_, RuleStore> {
    match rules.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn rule_ok(data: Value) -> Response {
    Json(json!({
        "data": data,
        "error": null,
        "engine": "inactive",
    }))
    .into_response()
}

fn timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}
