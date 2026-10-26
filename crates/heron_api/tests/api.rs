//! HTTP coverage for the management API.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use futures_util::StreamExt;
use heron_api::{ApiServer, router};
use heron_core::{
    Broker, BrokerConfig, ConnectRequest, IncomingPublish, MemoryStorage, Outbound, PollEvent,
    ProtocolVersion, QoS, SubOpts, SubscribeAck, SubscribeFilter,
};
use heron_rules::RuleEngine;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
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
    let app = router(broker, RuleEngine::new(), Some("secret".to_owned()));

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
    let app = router(broker().await, RuleEngine::new(), None);
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

#[tokio::test]
async fn client_list_should_filter_by_identifier_and_protocol_and_expose_session_fields() {
    let broker = broker().await;
    let mut v5 = connect("sensor-west");
    v5.protocol = ProtocolVersion::V5;
    v5.clean_start = false;
    v5.session_expiry = Some(120);
    broker.connect(connect("sensor-east")).await.expect("v4");
    broker.connect(v5).await.expect("v5");
    let app = router(broker, RuleEngine::new(), None);

    let listed = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/clients?client_id=west&protocol=5")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("clients");
    let listed = body_json(listed).await;
    assert_eq!(listed["total"], 1);
    assert_eq!(listed["data"][0]["client_id"], "sensor-west");
    assert_eq!(listed["data"][0]["clean_start"], false);
    assert_eq!(listed["data"][0]["session_expiry"], 120);
}

#[tokio::test]
async fn topic_detail_should_return_retained_text_and_clear_when_an_empty_retain_is_published() {
    let app = router(broker().await, RuleEngine::new(), None);
    let published = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/topics/sensors/temp/publish")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"payload":"21","qos":0,"retain":true}"#))
                .expect("request"),
        )
        .await
        .expect("publish");
    assert_eq!(published.status(), StatusCode::OK);

    let detail = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/topics/sensors/temp")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("detail");
    let detail = body_json(detail).await;
    assert_eq!(detail["data"]["payload_text"], "21");
    assert_eq!(detail["data"]["payload_hex"], "3231");
    assert_eq!(detail["data"]["retained"], true);

    let cleared = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/topics/sensors/temp/publish")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"payload":"","qos":0,"retain":true}"#))
                .expect("request"),
        )
        .await
        .expect("clear");
    assert_eq!(cleared.status(), StatusCode::OK);

    let after = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/topics/sensors/temp")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("after");
    assert_eq!(after.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn rules_should_round_trip_in_memory_and_report_that_the_engine_is_active() {
    let app = router(broker().await, RuleEngine::new(), Some("secret".to_owned()));
    let denied = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/rules")
                .header("content-type", "application/json")
                .body(Body::from(rule_body("forward")))
                .expect("request"),
        )
        .await
        .expect("denied");
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);

    let created = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/rules")
                .header("authorization", "Bearer secret")
                .header("content-type", "application/json")
                .body(Body::from(rule_body("forward")))
                .expect("request"),
        )
        .await
        .expect("created");
    assert_eq!(created.status(), StatusCode::OK);
    let created = body_json(created).await;
    assert_eq!(created["engine"], "active");
    assert_eq!(created["data"]["name"], "forward");
    let id = created["data"]["id"].as_u64().expect("id");

    let invalid = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/rules")
                .header("authorization", "Bearer secret")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"name":"bad-filter","topic_filter":"a/#/b","actions":[{"type":"log"}]}"#,
                ))
                .expect("request"),
        )
        .await
        .expect("invalid");
    assert_eq!(invalid.status(), StatusCode::BAD_REQUEST);

    let updated = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/api/v1/rules/{id}"))
                .header("authorization", "Bearer secret")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"name":"forward","enabled":false,"priority":5,"topic_filter":"sensors/#","conditions":{"sql":"qos >= 0"},"actions":[{"type":"log"}]}"#,
                ))
                .expect("request"),
        )
        .await
        .expect("updated");
    assert_eq!(updated.status(), StatusCode::OK);
    assert_eq!(body_json(updated).await["data"]["enabled"], false);

    let listed = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/rules")
                .header("authorization", "Bearer secret")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("listed");
    let listed = body_json(listed).await;
    assert_eq!(listed["data"][0]["priority"], 5);
    assert_eq!(listed["engine"], "active");

    let deleted = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/v1/rules/{id}"))
                .header("authorization", "Bearer secret")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("deleted");
    assert_eq!(deleted.status(), StatusCode::OK);
    assert_eq!(body_json(deleted).await["data"]["deleted"], true);
}

#[tokio::test]
async fn rules_should_republish_a_matching_publish_to_subscribers() {
    let broker = broker().await;
    let engine = RuleEngine::new();
    broker.install_hooks(engine.hooks());
    let app = router(broker.clone(), engine, None);
    let created = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/rules")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"name":"hot","topic_filter":"sensors/#","conditions":{"sql":"payload.temp > 30"},"actions":[{"type":"republish","topic":"alerts/${client_id}","qos":0}]}"#,
                ))
                .expect("request"),
        )
        .await
        .expect("created");
    assert_eq!(created.status(), StatusCode::OK);

    let publisher = broker.connect(connect("pub")).await.expect("publisher");
    let subscriber = broker.connect(connect("sub")).await.expect("subscriber");
    let acks = broker
        .subscribe(
            &subscriber.client_id,
            subscriber.epoch,
            &[SubscribeFilter {
                filter: "alerts/pub".to_owned(),
                opts: SubOpts::v4(QoS::AtMostOnce),
            }],
        )
        .await
        .expect("subscribe");
    assert_eq!(acks, vec![SubscribeAck::Granted(QoS::AtMostOnce)]);

    publish_text(&broker, &publisher, "sensors/lab", r#"{"temp":20}"#).await;
    let cold = broker
        .poll(&subscriber.client_id, subscriber.epoch, 8)
        .await;
    assert!(
        cold.is_empty(),
        "temperature below the threshold is not forwarded"
    );

    publish_text(&broker, &publisher, "sensors/lab", r#"{"temp":31}"#).await;
    let hot = broker
        .poll(&subscriber.client_id, subscriber.epoch, 8)
        .await;
    match hot.first() {
        Some(PollEvent::Outbound(Outbound::Publish { topic, payload, .. })) => {
            assert_eq!(topic, "alerts/pub");
            assert_eq!(payload.as_ref(), br#"{"temp":31}"#);
        }
        other => panic!("expected republished payload, got {other:?}"),
    }
}

async fn publish_text(
    broker: &Broker,
    session: &heron_core::ConnectedSession,
    topic: &str,
    payload: &str,
) {
    broker
        .publish(
            &session.client_id,
            session.epoch,
            IncomingPublish {
                topic: topic.to_owned(),
                payload: bytes::Bytes::from(payload.to_owned()),
                qos: QoS::AtMostOnce,
                retain: false,
                packet_id: None,
            },
        )
        .await
        .expect("publish");
}

#[tokio::test]
async fn events_socket_should_require_a_token_and_push_a_snapshot_then_a_connect_notice() {
    let broker = broker().await;
    let server = ApiServer::bind(
        broker.clone(),
        RuleEngine::new(),
        "127.0.0.1:0".parse().expect("addr"),
        Some("secret".to_owned()),
    )
    .await
    .expect("bind");
    let addr = server.local_addr().expect("local");
    tokio::spawn(async move {
        let _ = server.serve(std::future::pending()).await;
    });

    let denied = connect_async(format!("ws://{addr}/api/v1/events")).await;
    assert!(denied.is_err(), "missing token should be rejected");

    let (mut socket, _) = connect_async(format!("ws://{addr}/api/v1/events?token=secret"))
        .await
        .expect("socket");
    let first = next_text(&mut socket).await;
    let snapshot: Value = serde_json::from_str(&first).expect("snapshot json");
    assert_eq!(snapshot["type"], "snapshot");
    assert_eq!(snapshot["connections"], 0);

    broker.connect(connect("live-1")).await.expect("connect");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    let mut saw_connect = false;
    while tokio::time::Instant::now() < deadline {
        let message = tokio::time::timeout(Duration::from_secs(2), next_text(&mut socket))
            .await
            .expect("timed out waiting for a client event");
        let event: Value = serde_json::from_str(&message).expect("event json");
        if event["type"] == "client" && event["action"] == "connected" {
            assert_eq!(event["client_id"], "live-1");
            saw_connect = true;
            break;
        }
    }
    assert!(saw_connect, "expected a connected client event");
}

#[tokio::test]
async fn options_should_advertise_cors_for_the_management_gui() {
    let app = router(broker().await, RuleEngine::new(), None);
    let response = app
        .oneshot(
            Request::builder()
                .method("OPTIONS")
                .uri("/api/v1/clients")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("options");
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        response
            .headers()
            .get("access-control-allow-origin")
            .and_then(|value| value.to_str().ok()),
        Some("*")
    );
}

fn rule_body(topic_filter: &str) -> String {
    json!({
        "name": "forward",
        "topic_filter": topic_filter,
        "actions": [{"type": "log"}],
    })
    .to_string()
}

async fn next_text<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>) -> String
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let frame = socket.next().await.expect("frame").expect("ws message");
        if let Message::Text(text) = frame {
            return text.to_string();
        }
    }
}
