//! Rule parsing and broker execution.

use std::sync::Arc;
use std::time::Duration;

use heron_core::{
    Broker, BrokerConfig, ClientFlow, ConnectError, ConnectRequest, IncomingPublish, MemoryStorage,
    Outbound, PollEvent, ProtocolVersion, QoS, Storage, SubOpts, SubscribeAck, SubscribeFailure,
    SubscribeFilter,
};
use heron_rules::{Hook, HookContext, HookError, RuleEngine, RuleInput, parse_rule_sql};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn input(
    name: &str,
    filter: &str,
    conditions: serde_json::Value,
    actions: serde_json::Value,
) -> RuleInput {
    RuleInput {
        name: name.to_owned(),
        description: None,
        enabled: true,
        priority: 0,
        topic_filter: filter.to_owned(),
        conditions,
        actions,
    }
}

async fn broker() -> Broker {
    Broker::open(Arc::new(MemoryStorage::new()), BrokerConfig::default())
        .await
        .expect("broker")
}

fn connect_request(id: &str) -> ConnectRequest {
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
        peer: Some("127.0.0.1:1".to_owned()),
        username: None,
        password: None,
        certificate_identity: None,
    }
}

async fn subscribe(broker: &Broker, client_id: &str, epoch: u64, filter: &str) {
    let acks = broker
        .subscribe(
            client_id,
            epoch,
            &[SubscribeFilter {
                filter: filter.to_owned(),
                opts: SubOpts::v4(QoS::AtMostOnce),
            }],
        )
        .await
        .expect("subscribe");
    assert_eq!(acks, vec![SubscribeAck::Granted(QoS::AtMostOnce)]);
}

async fn publish(broker: &Broker, client_id: &str, epoch: u64, topic: &str, payload: &str) {
    let flow = broker
        .publish(
            client_id,
            epoch,
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
    assert_eq!(flow, ClientFlow::Accepted);
}

fn payloads(events: &[PollEvent]) -> Vec<Vec<u8>> {
    events
        .iter()
        .filter_map(|event| match event {
            PollEvent::Outbound(Outbound::Publish { payload, .. }) => Some(payload.to_vec()),
            _ => None,
        })
        .collect()
}

#[test]
fn parse_rule_sql_should_capture_the_filter_condition_and_projection() {
    let parsed = parse_rule_sql(
        r#"SELECT payload.temp AS temp, client_id FROM "sensors/+/temp" WHERE payload.temp > 30"#,
    )
    .expect("sql");
    assert_eq!(parsed.topic_filter, "sensors/+/temp");
    assert_eq!(parsed.conditions["sql"], "payload.temp > 30");
    assert_eq!(parsed.actions[0]["type"], "transform");
    assert_eq!(parsed.actions[0]["payload"]["temp"], "${payload.temp}");
    assert_eq!(parsed.actions[0]["payload"]["client_id"], "${client_id}");
}

#[test]
fn parse_rule_sql_should_reject_a_statement_without_from() {
    let error = parse_rule_sql("SELECT * WHERE qos > 0").expect_err("sql");
    let message = error.to_string();
    assert!(message.contains("expected FROM"), "{message}");
}

#[tokio::test]
async fn publish_should_drop_transform_and_avoid_republish_loops() {
    let broker = broker().await;
    let engine = RuleEngine::new();
    broker.install_hooks(engine.hooks());
    engine
        .insert(input(
            "shape",
            "sensors/#",
            serde_json::json!({"all":[{"field":"payload.temp","op":"gt","value":30}]}),
            serde_json::json!([
                {"type":"transform","payload":{"c":"${payload.temp}"}},
                {"type":"republish","topic":"sensors/copy","qos":0}
            ]),
        ))
        .expect("rule");
    engine
        .insert(input(
            "silence",
            "noise/#",
            serde_json::Value::Null,
            serde_json::json!([{"type":"drop"}]),
        ))
        .expect("drop");

    let publisher = broker
        .connect(connect_request("pub"))
        .await
        .expect("publisher");
    let subscriber = broker
        .connect(connect_request("sub"))
        .await
        .expect("subscriber");
    subscribe(
        &broker,
        &subscriber.client_id,
        subscriber.epoch,
        "sensors/#",
    )
    .await;
    subscribe(&broker, &subscriber.client_id, subscriber.epoch, "noise/#").await;

    publish(
        &broker,
        &publisher.client_id,
        publisher.epoch,
        "sensors/lab",
        r#"{"temp":31}"#,
    )
    .await;
    publish(
        &broker,
        &publisher.client_id,
        publisher.epoch,
        "noise/lab",
        "loud",
    )
    .await;

    let events = broker
        .poll(&subscriber.client_id, subscriber.epoch, 8)
        .await;
    let got = payloads(&events);
    assert_eq!(
        got.len(),
        2,
        "original transform plus one republish, not a loop"
    );
    assert!(got.iter().any(|payload| payload == br#"{"c":31}"#));
    assert!(got.iter().any(|payload| payload == br#"{"temp":31}"#));
}

#[tokio::test]
async fn qos2_should_run_rules_when_pubrel_forwards_the_message() {
    let broker = broker().await;
    let engine = RuleEngine::new();
    broker.install_hooks(engine.hooks());
    engine
        .insert(input(
            "shape",
            "sensors/#",
            serde_json::Value::Null,
            serde_json::json!([{"type":"transform","payload":{"ok":true}}]),
        ))
        .expect("rule");
    let publisher = broker
        .connect(connect_request("pub"))
        .await
        .expect("publisher");
    let subscriber = broker
        .connect(connect_request("sub"))
        .await
        .expect("subscriber");
    subscribe(
        &broker,
        &subscriber.client_id,
        subscriber.epoch,
        "sensors/#",
    )
    .await;
    let flow = broker
        .publish(
            &publisher.client_id,
            publisher.epoch,
            IncomingPublish {
                topic: "sensors/lab".to_owned(),
                payload: bytes::Bytes::from_static(b"{}"),
                qos: QoS::ExactlyOnce,
                retain: false,
                packet_id: Some(4),
            },
        )
        .await
        .expect("publish");
    assert_eq!(flow, ClientFlow::PubRec);
    assert!(
        broker
            .poll(&subscriber.client_id, subscriber.epoch, 4)
            .await
            .is_empty()
    );
    let completed = broker
        .pubrel(&publisher.client_id, publisher.epoch, 4)
        .await
        .expect("pubrel");
    assert_eq!(completed, ClientFlow::PubComp);
    let got = payloads(
        &broker
            .poll(&subscriber.client_id, subscriber.epoch, 4)
            .await,
    );
    assert_eq!(got, vec![br#"{"ok":true}"#.to_vec()]);
}

#[tokio::test]
async fn webhook_should_receive_the_publish_context() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let mut collected = Vec::new();
        let mut buf = [0u8; 4096];
        let mut saw_data = false;
        loop {
            let wait = if saw_data {
                Duration::from_millis(200)
            } else {
                Duration::from_secs(2)
            };
            match tokio::time::timeout(wait, stream.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 => {
                    saw_data = true;
                    collected.extend_from_slice(&buf[..n]);
                }
                _ => break,
            }
        }
        let _ = stream
            .write_all(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await;
        let _ = tx.send(String::from_utf8_lossy(&collected).into_owned());
    });

    let broker = broker().await;
    let engine = RuleEngine::new();
    broker.install_hooks(engine.hooks());
    engine
        .insert(input(
            "notify",
            "sensors/#",
            serde_json::json!({"sql":"payload.temp > 30 AND NOT retain = true"}),
            serde_json::json!([{
                "type": "webhook",
                "url": format!("http://{addr}/hook")
            }]),
        ))
        .expect("rule");
    let publisher = broker
        .connect(connect_request("sensor-1"))
        .await
        .expect("publisher");
    publish(
        &broker,
        &publisher.client_id,
        publisher.epoch,
        "sensors/lab",
        r#"{"temp":42}"#,
    )
    .await;
    let body = tokio::time::timeout(Duration::from_secs(2), rx)
        .await
        .expect("webhook timed out")
        .expect("webhook body");
    assert!(body.contains("\"temp\":42"), "{body}");
    assert!(body.contains("sensor-1"), "{body}");
}

#[tokio::test]
async fn reject_rules_should_refuse_connect_and_a_matching_subscription() {
    let broker = broker().await;
    let engine = RuleEngine::new();
    broker.install_hooks(engine.hooks());
    engine
        .insert(input(
            "ban",
            "$events/client_connected",
            serde_json::json!({"sql":"client_id = 'banned'"}),
            serde_json::json!([{"type":"reject"}]),
        ))
        .expect("connect rule");
    engine
        .insert(input(
            "secret",
            "$events/client_subscribed",
            serde_json::json!({"sql":"filter = 'secret/#'"}),
            serde_json::json!([{"type":"reject"}]),
        ))
        .expect("subscribe rule");

    let refused = broker
        .connect(connect_request("banned"))
        .await
        .expect_err("banned");
    assert_eq!(refused, ConnectError::NotAuthorized);

    let session = broker
        .connect(connect_request("ok"))
        .await
        .expect("allowed");
    let acks = broker
        .subscribe(
            &session.client_id,
            session.epoch,
            &[SubscribeFilter {
                filter: "secret/#".to_owned(),
                opts: SubOpts::v4(QoS::AtMostOnce),
            }],
        )
        .await
        .expect("subscribe");
    assert_eq!(
        acks,
        vec![SubscribeAck::Failed(SubscribeFailure::NotAuthorized)]
    );
}

struct DenySecret;

#[async_trait::async_trait]
impl Hook for DenySecret {
    async fn on_publish(&self, ctx: &HookContext) -> Result<(), HookError> {
        if ctx.topic == "hidden" {
            return Err(HookError::Reject);
        }
        Ok(())
    }
}

#[tokio::test]
async fn hook_plugin_should_reject_a_publish_before_rules_run() {
    let broker = broker().await;
    let engine = RuleEngine::new();
    engine.add_hook(Arc::new(DenySecret));
    broker.install_hooks(engine.hooks());
    engine
        .insert(input(
            "copy",
            "hidden",
            serde_json::Value::Null,
            serde_json::json!([{"type":"republish","topic":"copied","qos":0}]),
        ))
        .expect("rule");
    let publisher = broker
        .connect(connect_request("pub"))
        .await
        .expect("publisher");
    let subscriber = broker
        .connect(connect_request("sub"))
        .await
        .expect("subscriber");
    subscribe(&broker, &subscriber.client_id, subscriber.epoch, "copied").await;
    subscribe(&broker, &subscriber.client_id, subscriber.epoch, "hidden").await;
    publish(
        &broker,
        &publisher.client_id,
        publisher.epoch,
        "hidden",
        "secret",
    )
    .await;
    let events = broker
        .poll(&subscriber.client_id, subscriber.epoch, 8)
        .await;
    assert!(payloads(&events).is_empty());
}

async fn persisted_rules(storage: &Arc<MemoryStorage>) -> Vec<heron_core::StoredRule> {
    for _ in 0..100 {
        let rules = storage.load_rules().await.expect("load");
        if !rules.is_empty() {
            return rules;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Vec::new()
}

#[tokio::test]
async fn successful_crud_should_write_through_to_storage() {
    let storage = Arc::new(MemoryStorage::new());
    let engine = RuleEngine::new();
    engine.set_storage(storage.clone());

    let created = engine
        .insert(input(
            "hot",
            "sensors/#",
            serde_json::Value::Null,
            serde_json::json!([{"type":"drop"}]),
        ))
        .expect("insert");

    let rules = persisted_rules(&storage).await;
    assert_eq!(rules.len(), 1);
    assert_eq!((rules[0].id, rules[0].name.as_str()), (created.id, "hot"));
    assert!(rules[0].actions.contains("drop"));

    engine
        .update(
            created.id,
            input(
                "cold",
                "alerts/#",
                serde_json::Value::Null,
                serde_json::json!([{"type":"drop"}]),
            ),
        )
        .expect("update");
    tokio::time::sleep(Duration::from_millis(50)).await;
    let rules = storage.load_rules().await.expect("reload");
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].name, "cold");

    engine.delete(created.id).expect("delete");
    tokio::time::sleep(Duration::from_millis(50)).await;
    let rules = storage.load_rules().await.expect("redelete");
    assert!(rules.is_empty(), "delete should be persisted");
}

#[tokio::test]
async fn restore_should_rebuild_rules_and_continue_ids_from_storage() {
    let storage = Arc::new(MemoryStorage::new());
    let first = RuleEngine::new();
    first.set_storage(storage.clone());
    let created = first
        .insert(input(
            "hot",
            "sensors/#",
            serde_json::Value::Null,
            serde_json::json!([{"type":"drop"}]),
        ))
        .expect("insert");
    let stored = persisted_rules(&storage).await;
    assert!(!stored.is_empty());

    let revived = RuleEngine::new();
    revived.restore(stored).await;
    let rules = revived.list();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].id, created.id);
    assert_eq!(rules[0].name, "hot");
    assert_eq!(rules[0].actions, created.actions);

    let moved = revived
        .insert(input(
            "second",
            "alerts/#",
            serde_json::Value::Null,
            serde_json::json!([{"type":"drop"}]),
        ))
        .expect("insert after restore");
    assert!(moved.id > created.id, "ids must never be reused");
}
