//! HTTP coverage for the management API.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use heron_api::router;
use heron_core::{Broker, BrokerConfig, ConnectRequest, MemoryStorage, ProtocolVersion};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

async fn broker() -> Broker {
    Broker::open(Arc::new(MemoryStorage::new()), BrokerConfig::default())
        .await
        .expect("broker")
}

fn connect(id: &str) -> ConnectRequest {
    ConnectRequest {
        client_id: id.to_owned(),
        protocol: ProtocolVersion::V4,
        clean_start: true,
        session_expiry: Some(0),
        keep_alive: 30,
        will: None,
        receive_maximum: 16,
        max_packet_size: 0,
        enhanced_auth: false,
        peer: Some("127.0.0.1:1883".to_owned()),
        username: None,
        password: None,
        certificate_identity: None,
    }
}

async fn body_json(response: axum::response::Response) -> Value {
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    serde_json::from_slice(&bytes).expect("json")
}

#[tokio::test]
async fn health_should_stay_open_and_management_routes_should_require_a_bearer_token() {
    let broker = broker().await;
    broker.connect(connect("client-1")).await.expect("connect");
    let app = router(broker, Some("secret".to_owned()));

    let health = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("health");
    assert_eq!(health.status(), StatusCode::OK);
    assert_eq!(body_json(health).await["status"], "ok");

    let denied = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/clients")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("denied");
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);

    let clients = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/clients?page=1&page_size=10&connected=true")
                .header("authorization", "Bearer secret")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("clients");
    assert_eq!(clients.status(), StatusCode::OK);
    let listed = body_json(clients).await;
    assert_eq!(listed["total"], 1);
    assert_eq!(listed["data"][0]["client_id"], "client-1");
    assert_eq!(listed["data"][0]["ip_address"], "127.0.0.1");
    assert_eq!(listed["data"][0]["connected"], true);

    let detail = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/clients/client-1")
                .header("authorization", "Bearer secret")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("detail");
    assert_eq!(detail.status(), StatusCode::OK);
    assert_eq!(body_json(detail).await["data"]["client_id"], "client-1");

    let probe = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/system/health")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("probe");
    assert_eq!(probe.status(), StatusCode::OK);

    let subscriptions = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/subscriptions")
                .header("authorization", "Bearer secret")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("subscriptions");
    assert_eq!(subscriptions.status(), StatusCode::OK);

    let published = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/topics/sensors/temp/publish")
                .header("authorization", "Bearer secret")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"payload":"21","qos":0,"retain":true}"#))
                .expect("request"),
        )
        .await
        .expect("publish");
    assert_eq!(published.status(), StatusCode::OK);

    let topics = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/topics")
                .header("authorization", "Bearer secret")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("topics");
    let topics = body_json(topics).await;
    assert_eq!(topics["data"][0]["topic"], "sensors/temp");
    assert_eq!(topics["data"][0]["retained"], true);

    let status = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/system/status")
                .header("authorization", "Bearer secret")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("status");
    let status = body_json(status).await;
    assert_eq!(status["connections"], 1);
    assert_eq!(status["messages_received"], 1);

    let metrics = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .header("authorization", "Bearer secret")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("metrics");
    assert_eq!(metrics.status(), StatusCode::OK);
    let bytes = metrics
        .into_body()
        .collect()
        .await
        .expect("metrics body")
        .to_bytes();
    let text = String::from_utf8(bytes.to_vec()).expect("utf8");
    assert!(text.contains("heron_messages_received_total 1"));
    assert!(text.contains("heron_connections 1"));
}

#[tokio::test]
async fn delete_client_should_return_not_found_when_the_session_is_offline() {
    let app = router(broker().await, None);
    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/api/v1/clients/missing")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("delete");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = body_json(response).await;
    assert_eq!(body["error"]["code"], "not_found");
}
