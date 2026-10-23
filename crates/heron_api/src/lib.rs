//! Management HTTP API for a single Heron node.
//!
//! Authenticated routes live under `/api/v1` and use a bearer token when one is
//! configured. `GET /health` and `GET /api/v1/system/health` stay open for probes.
//! `GET /metrics` exposes Prometheus text and follows the same bearer rule as the
//! management routes. `GET /api/v1/events` is a WebSocket that also accepts
//! `?token=` because browsers cannot set `Authorization` on the upgrade.

mod events;
mod rules;

use std::future::Future;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use axum::Json;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use bytes::Bytes;
use chrono::{DateTime, SecondsFormat};
use heron_core::{Broker, ClientSnapshot, IncomingPublish, PublishError, QoS};
use serde::Deserialize;
use serde_json::{Value, json};

/// HTTP listener for the management API.
pub struct ApiServer {
    broker: Broker,
    token: Option<String>,
    listener: tokio::net::TcpListener,
}

impl ApiServer {
    /// Bind `addr`. `token` is the bearer secret; `None` leaves the API open.
    pub async fn bind(
        broker: Broker,
        addr: SocketAddr,
        token: Option<String>,
    ) -> std::io::Result<Self> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        Ok(Self {
            broker,
            token,
            listener,
        })
    }

    /// Address after the kernel assigns a port.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Serve until `shutdown` completes.
    pub async fn serve(
        self,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> std::io::Result<()> {
        let app = router(self.broker, self.token);
        axum::serve(self.listener, app)
            .with_graceful_shutdown(shutdown)
            .await
    }
}

/// Router used by [`ApiServer`] and tests.
pub fn router(broker: Broker, token: Option<String>) -> Router {
    let state = AppState {
        broker,
        token,
        rules: Arc::new(Mutex::new(rules::RuleStore::new())),
    };
    let protected = Router::new()
        .route("/metrics", get(metrics))
        .route("/api/v1/clients", get(list_clients))
        .route(
            "/api/v1/clients/{client_id}",
            get(get_client).delete(delete_client),
        )
        .route("/api/v1/topics", get(list_topics))
        .route("/api/v1/topics/{*path}", get(get_topic).post(publish_topic))
        .route("/api/v1/subscriptions", get(list_subscriptions))
        .route(
            "/api/v1/rules",
            get(rules::list_rules).post(rules::create_rule),
        )
        .route(
            "/api/v1/rules/{rule_id}",
            put(rules::update_rule).delete(rules::delete_rule),
        )
        .route("/api/v1/system/status", get(system_status))
        .route("/api/v1/events", get(events::events))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_bearer,
        ))
        .with_state(state.clone());
    let open = Router::new()
        .route("/health", get(health))
        .route("/api/v1/system/health", get(health))
        .with_state(state);
    Router::new()
        .merge(open)
        .merge(protected)
        .layer(middleware::from_fn(cors))
}

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) broker: Broker,
    pub(crate) token: Option<String>,
    pub(crate) rules: Arc<Mutex<rules::RuleStore>>,
}

#[derive(Debug, Deserialize)]
struct ClientQuery {
    page: Option<u32>,
    page_size: Option<u32>,
    connected: Option<bool>,
    client_id: Option<String>,
    protocol: Option<u8>,
}

#[derive(Debug, Deserialize)]
struct PublishBody {
    payload: String,
    #[serde(default)]
    qos: u8,
    #[serde(default)]
    retain: bool,
}

async fn require_bearer(
    State(state): State<AppState>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    let Some(expected) = state.token.as_deref() else {
        return next.run(request).await;
    };
    let presented = presented_token(&request);
    match presented.as_deref() {
        Some(token) if constant_time_eq(token.as_bytes(), expected.as_bytes()) => {
            next.run(request).await
        }
        _ => error_response(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "bearer token required",
        ),
    }
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn metrics(State(state): State<AppState>) -> Response {
    let status = state.broker.status();
    let body = format!(
        "# HELP heron_uptime_seconds Seconds since the broker process started.\n\
         # TYPE heron_uptime_seconds gauge\n\
         heron_uptime_seconds {uptime}\n\
         # HELP heron_connections Current connected MQTT sessions.\n\
         # TYPE heron_connections gauge\n\
         heron_connections {connections}\n\
         # HELP heron_messages_received_total MQTT PUBLISH packets accepted.\n\
         # TYPE heron_messages_received_total counter\n\
         heron_messages_received_total {received}\n\
         # HELP heron_messages_sent_total MQTT PUBLISH packets queued to subscribers.\n\
         # TYPE heron_messages_sent_total counter\n\
         heron_messages_sent_total {sent}\n",
        uptime = status.uptime_secs,
        connections = status.connections,
        received = status.messages_received,
        sent = status.messages_sent,
    );
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
}

async fn system_status(State(state): State<AppState>) -> Json<Value> {
    let status = state.broker.status();
    Json(json!({
        "uptime": status.uptime_secs,
        "connections": status.connections,
        "messages_received": status.messages_received,
        "messages_sent": status.messages_sent,
    }))
}

async fn list_clients(
    State(state): State<AppState>,
    Query(query): Query<ClientQuery>,
) -> Json<Value> {
    let page = usize::try_from(query.page.unwrap_or(1).max(1)).unwrap_or(1);
    let page_size = usize::try_from(query.page_size.unwrap_or(20).max(1)).unwrap_or(20);
    let (total, clients) = state.broker.list_clients(
        query.connected,
        query.client_id.as_deref(),
        query.protocol,
        page,
        page_size,
    );
    let data: Vec<Value> = clients.iter().map(client_json).collect();
    Json(json!({
        "total": total,
        "page": page,
        "page_size": page_size,
        "data": data,
    }))
}

async fn get_client(State(state): State<AppState>, Path(client_id): Path<String>) -> Response {
    match state.broker.client(&client_id) {
        Some(client) => Json(json!({
            "data": client_json(&client),
            "error": null,
        }))
        .into_response(),
        None => error_response(StatusCode::NOT_FOUND, "not_found", "resource not found"),
    }
}

async fn delete_client(State(state): State<AppState>, Path(client_id): Path<String>) -> Response {
    if !state.broker.disconnect_client(&client_id) {
        return error_response(StatusCode::NOT_FOUND, "not_found", "resource not found");
    }
    Json(json!({
        "data": { "client_id": client_id, "disconnected": true },
        "error": null,
    }))
    .into_response()
}

async fn get_topic(State(state): State<AppState>, Path(path): Path<String>) -> Response {
    if path.is_empty() || path.ends_with("/publish") {
        return error_response(StatusCode::NOT_FOUND, "not_found", "resource not found");
    }
    let Some(topic) = state
        .broker
        .list_topics()
        .into_iter()
        .find(|topic| topic.topic == path)
    else {
        return error_response(StatusCode::NOT_FOUND, "not_found", "resource not found");
    };
    let retained = state.broker.retained(&topic.topic);
    let (payload_text, payload_hex, qos) = match retained {
        Some(message) => {
            let text = std::str::from_utf8(&message.payload)
                .ok()
                .map(str::to_owned);
            (text, Some(hex_encode(&message.payload)), Some(message.qos))
        }
        None => (None, None, None),
    };
    Json(json!({
        "data": {
            "topic": topic.topic,
            "subscribers": topic.subscribers,
            "retained": topic.retained,
            "payload_text": payload_text,
            "payload_hex": payload_hex,
            "qos": qos,
        },
        "error": null,
    }))
    .into_response()
}

async fn list_topics(State(state): State<AppState>) -> Json<Value> {
    let data: Vec<Value> = state
        .broker
        .list_topics()
        .into_iter()
        .map(|topic| {
            json!({
                "topic": topic.topic,
                "subscribers": topic.subscribers,
                "retained": topic.retained,
            })
        })
        .collect();
    Json(json!({ "data": data, "error": null }))
}

async fn list_subscriptions(State(state): State<AppState>) -> Json<Value> {
    let data: Vec<Value> = state
        .broker
        .list_subscriptions()
        .into_iter()
        .map(|subscription| {
            json!({
                "client_id": subscription.client_id,
                "topic": subscription.topic,
                "qos": subscription.qos,
            })
        })
        .collect();
    Json(json!({ "data": data, "error": null }))
}

async fn publish_topic(
    State(state): State<AppState>,
    Path(path): Path<String>,
    body: Result<Json<PublishBody>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Json(body) = match body {
        Ok(body) => body,
        Err(_) => {
            return error_response(StatusCode::BAD_REQUEST, "bad_request", "invalid JSON body");
        }
    };
    let Some(topic) = path.strip_suffix("/publish") else {
        return error_response(StatusCode::NOT_FOUND, "not_found", "resource not found");
    };
    let topic = topic.trim_end_matches('/');
    if topic.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "bad_request", "topic is empty");
    }
    let Ok(qos) = QoS::try_from(body.qos) else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "qos must be 0, 1, or 2",
        );
    };
    let publish = IncomingPublish {
        topic: topic.to_owned(),
        payload: Bytes::from(body.payload.into_bytes()),
        qos,
        retain: body.retain,
        packet_id: None,
    };
    match state.broker.publish_admin(publish).await {
        Ok(()) => Json(json!({
            "data": { "topic": topic, "qos": body.qos, "retain": body.retain },
            "error": null,
        }))
        .into_response(),
        Err(PublishError::InvalidTopic) => {
            error_response(StatusCode::BAD_REQUEST, "bad_request", "invalid topic")
        }
        Err(_) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "publish failed",
        ),
    }
}

fn client_json(client: &ClientSnapshot) -> Value {
    json!({
        "client_id": client.client_id,
        "protocol_version": client.protocol_version,
        "ip_address": ip_address(client.peer.as_deref()),
        "connected": client.connected,
        "connected_at": client.connected_at_ms.and_then(rfc3339),
        "username": client.username,
        "keep_alive": client.keep_alive,
        "clean_start": client.clean_start,
        "session_expiry": client.session_expiry,
    })
}

fn ip_address(peer: Option<&str>) -> Option<String> {
    let peer = peer?;
    peer.parse::<SocketAddr>()
        .ok()
        .map(|addr| addr.ip().to_string())
        .or_else(|| Some(peer.to_owned()))
}

fn rfc3339(millis: i64) -> Option<String> {
    DateTime::from_timestamp_millis(millis)
        .map(|time| time.to_rfc3339_opts(SecondsFormat::Secs, true))
}

pub(crate) fn error_response(status: StatusCode, code: &'static str, message: &str) -> Response {
    (
        status,
        Json(json!({
            "data": null,
            "error": { "code": code, "message": message },
        })),
    )
        .into_response()
}

fn presented_token(request: &axum::extract::Request) -> Option<String> {
    if let Some(token) = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
    {
        return Some(token.to_owned());
    }
    if request.uri().path() == "/api/v1/events" {
        return request
            .uri()
            .query()
            .and_then(|query| query_value(query, "token"));
    }
    None
}

fn query_value(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        if name == key {
            Some(percent_decode(value))
        } else {
            None
        }
    })
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && let Some(high) = index.checked_add(1).and_then(|next| bytes.get(next))
            && let Some(low) = index.checked_add(2).and_then(|next| bytes.get(next))
            && let Some(high) = hex_value(*high)
            && let Some(low) = hex_value(*low)
        {
            out.push((high << 4) | low);
            index += 3;
            continue;
        }
        out.push(if bytes[index] == b'+' {
            b' '
        } else {
            bytes[index]
        });
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        out.push(HEX[usize::from(byte >> 4)] as char);
        out.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    out
}

async fn cors(request: axum::extract::Request, next: Next) -> Response {
    if request.method() == Method::OPTIONS {
        let mut headers = HeaderMap::new();
        apply_cors(&mut headers);
        return (StatusCode::NO_CONTENT, headers).into_response();
    }
    let mut response = next.run(request).await;
    apply_cors(response.headers_mut());
    response
}

fn apply_cors(headers: &mut HeaderMap) {
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, POST, PUT, DELETE, OPTIONS"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static("authorization, content-type"),
    );
    headers.insert(
        header::ACCESS_CONTROL_MAX_AGE,
        HeaderValue::from_static("600"),
    );
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0u8;
    for (left, right) in left.iter().zip(right) {
        diff |= left ^ right;
    }
    diff == 0
}
