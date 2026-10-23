//! Management HTTP API for a single Heron node.
//!
//! Authenticated routes live under `/api/v1` and use a bearer token when one is
//! configured. `GET /health` and `GET /api/v1/system/health` stay open for probes.
//! `GET /metrics` exposes Prometheus text and follows the same bearer rule as the
//! management routes.

use std::future::Future;
use std::net::SocketAddr;

use axum::Json;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
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
    let state = AppState { broker, token };
    let protected = Router::new()
        .route("/metrics", get(metrics))
        .route("/api/v1/clients", get(list_clients))
        .route(
            "/api/v1/clients/{client_id}",
            get(get_client).delete(delete_client),
        )
        .route("/api/v1/topics", get(list_topics))
        .route("/api/v1/topics/{*path}", post(publish_topic))
        .route("/api/v1/subscriptions", get(list_subscriptions))
        .route("/api/v1/system/status", get(system_status))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_bearer,
        ))
        .with_state(state.clone());
    let open = Router::new()
        .route("/health", get(health))
        .route("/api/v1/system/health", get(health))
        .with_state(state);
    Router::new().merge(open).merge(protected)
}

#[derive(Clone)]
struct AppState {
    broker: Broker,
    token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ClientQuery {
    page: Option<u32>,
    page_size: Option<u32>,
    connected: Option<bool>,
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
    let presented = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    match presented {
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
    let (total, clients) = state.broker.list_clients(query.connected, page, page_size);
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

fn error_response(status: StatusCode, code: &'static str, message: &str) -> Response {
    (
        status,
        Json(json!({
            "data": null,
            "error": { "code": code, "message": message },
        })),
    )
        .into_response()
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
