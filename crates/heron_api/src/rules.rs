//! Rule configuration backed by [`heron_rules::RuleEngine`].
//!
//! Records stay in memory and disappear on restart. Successful responses include
//! `"engine": "active"` because the same engine is installed on the broker.

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use heron_rules::{RuleEngine, RuleError, RuleInput};
use serde_json::{Value, json};

use crate::{AppState, error_response};

pub(crate) fn engine() -> RuleEngine {
    RuleEngine::new()
}

pub(crate) async fn list_rules(State(state): State<AppState>) -> Response {
    rule_ok(json!(state.rules.list()))
}

pub(crate) async fn create_rule(
    State(state): State<AppState>,
    body: Result<Json<RuleInput>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Json(body) = match body {
        Ok(body) => body,
        Err(_) => {
            return error_response(StatusCode::BAD_REQUEST, "bad_request", "invalid JSON body");
        }
    };
    match state.rules.insert(body) {
        Ok(rule) => rule_ok(json!(rule)),
        Err(err) => rule_err(err),
    }
}

pub(crate) async fn update_rule(
    State(state): State<AppState>,
    Path(rule_id): Path<String>,
    body: Result<Json<RuleInput>, axum::extract::rejection::JsonRejection>,
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
    match state.rules.update(rule_id, body) {
        Ok(rule) => rule_ok(json!(rule)),
        Err(err) => rule_err(err),
    }
}

pub(crate) async fn delete_rule(
    State(state): State<AppState>,
    Path(rule_id): Path<String>,
) -> Response {
    let Ok(rule_id) = rule_id.parse::<u64>() else {
        return error_response(StatusCode::BAD_REQUEST, "bad_request", "invalid rule id");
    };
    match state.rules.delete(rule_id) {
        Ok(()) => rule_ok(json!({ "id": rule_id, "deleted": true })),
        Err(err) => rule_err(err),
    }
}

fn rule_err(err: RuleError) -> Response {
    match err {
        RuleError::NotFound => {
            error_response(StatusCode::NOT_FOUND, "not_found", "resource not found")
        }
        RuleError::Invalid(message) => {
            error_response(StatusCode::BAD_REQUEST, "bad_request", &message)
        }
    }
}

fn rule_ok(data: Value) -> Response {
    Json(json!({
        "data": data,
        "error": null,
        "engine": "active",
    }))
    .into_response()
}
