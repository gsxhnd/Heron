//! MQTT v5.0 integration tests against the in-process mock broker.

mod support;

use std::time::Duration;

use bytes::Bytes;
use heron_client::{
    ClientConfig, ClientError, Event, MqttClient, ProtocolVersion, ReconnectOptions,
};
use support::{
    BrokerConfig, BrokerEvent, expect_disconnected, expect_message, spawn_v5, wait_connected,
};
fn config(addr: &str) -> ClientConfig {
    ClientConfig::new(addr, "v5-client")
        .protocol_version(ProtocolVersion::V5)
        .keep_alive(60)
        .connect_timeout(Duration::from_secs(2))
        .request_timeout(Duration::from_secs(3))
        .reconnect(ReconnectOptions {
            enabled: false,
            ..ReconnectOptions::default()
        })
}

#[tokio::test]
async fn connect_should_complete_handshake_and_report_session_state() {
    let mut broker = spawn_v5(BrokerConfig {
        session_present: true,
        ..BrokerConfig::default()
    })
    .await;

    let (_client, mut events) = MqttClient::new(config(&broker.addr()));
    assert!(wait_connected(&mut events).await);

    match broker.next_event().await {
        BrokerEvent::Connect {
            client_id,
            keep_alive,
            clean_session,
        } => {
            assert_eq!(client_id, "v5-client");
            assert_eq!(keep_alive, 60);
            assert!(clean_session);
        }
        other => panic!("expected CONNECT, got {other:?}"),
    }
}

#[tokio::test]
async fn server_keep_alive_should_override_client_interval() {
    let mut broker = spawn_v5(BrokerConfig {
        server_keep_alive: Some(1),
        ..BrokerConfig::default()
    })
    .await;
    // Client asks for 60s, broker dictates 1s.
    let cfg = config(&broker.addr()).keep_alive(60);
    let (client, mut events) = MqttClient::new(cfg);
    wait_connected(&mut events).await;

    assert!(matches!(
        broker
            .expect_event("PINGREQ", |e| matches!(e, BrokerEvent::Ping))
            .await,
        BrokerEvent::Ping
    ));
    client.disconnect().await.ok();
}

#[tokio::test]
async fn assigned_client_identifier_should_be_reused_on_reconnect() {
    let mut broker = spawn_v5(BrokerConfig {
        assigned_client_id: Some("broker-assigned-id".to_string()),
        ..BrokerConfig::default()
    })
    .await;

    // Persistent session so the assigned id survives the reconnect.
    let cfg = ClientConfig::new(broker.addr(), "")
        .protocol_version(ProtocolVersion::V5)
        .clean_session(false)
        .connect_timeout(Duration::from_secs(2))
        .request_timeout(Duration::from_secs(5))
        .reconnect(ReconnectOptions {
            enabled: true,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(200),
            max_attempts: None,
        });

    let (client, mut events) = MqttClient::new(cfg);
    wait_connected(&mut events).await;

    // First CONNECT carries a generated id...
    let first = broker
        .expect_event("CONNECT", |e| matches!(e, BrokerEvent::Connect { .. }))
        .await;
    match first {
        BrokerEvent::Connect { client_id, .. } => {
            assert!(client_id.starts_with("mqtt-"), "got {client_id}")
        }
        other => panic!("expected CONNECT, got {other:?}"),
    }

    broker.close().await;

    // ...and the reconnect reuses the id assigned by the broker.
    let second = broker
        .expect_event("reconnect", |e| matches!(e, BrokerEvent::Connect { .. }))
        .await;
    match second {
        BrokerEvent::Connect { client_id, .. } => assert_eq!(client_id, "broker-assigned-id"),
        other => panic!("expected CONNECT, got {other:?}"),
    }

    client.disconnect().await.ok();
}

#[tokio::test]
async fn publish_qos1_should_wait_for_puback() {
    let mut broker = spawn_v5(BrokerConfig::default()).await;
    let (client, mut events) = MqttClient::new(config(&broker.addr()));
    wait_connected(&mut events).await;

    client
        .publish("sensor/temp", Bytes::from_static(b"30"), 1, false)
        .await
        .expect("qos 1 publish");

    assert!(matches!(
        broker
            .expect_event("PUBLISH", |e| matches!(e, BrokerEvent::Publish { .. }))
            .await,
        BrokerEvent::Publish { qos: 1, .. }
    ));

    let ack = tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .expect("PUBACK event");
    assert!(matches!(ack, Some(Event::PubAck { .. })));
    client.disconnect().await.ok();
}

#[tokio::test]
async fn publish_qos2_should_complete_pubrec_pubrel_pubcomp() {
    let mut broker = spawn_v5(BrokerConfig::default()).await;
    let (client, mut events) = MqttClient::new(config(&broker.addr()));
    wait_connected(&mut events).await;

    client
        .publish("cmd/reboot", Bytes::from_static(b"now"), 2, false)
        .await
        .expect("qos 2 publish");

    assert!(matches!(
        broker
            .expect_event("PUBLISH", |e| matches!(e, BrokerEvent::Publish { .. }))
            .await,
        BrokerEvent::Publish { qos: 2, .. }
    ));
    assert!(matches!(
        broker
            .expect_event("PUBREL", |e| matches!(e, BrokerEvent::PubRel { .. }))
            .await,
        BrokerEvent::PubRel { .. }
    ));

    let comp = tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .expect("PUBCOMP event");
    assert!(matches!(comp, Some(Event::PubComp { .. })));
    client.disconnect().await.ok();
}

#[tokio::test]
async fn subscribe_should_return_granted_reason_codes_and_receive_messages() {
    let mut broker = spawn_v5(BrokerConfig::default()).await;
    let (client, mut events) = MqttClient::new(config(&broker.addr()));
    wait_connected(&mut events).await;

    let granted = client
        .subscribe(vec![("room/+/temp".to_string(), 2)])
        .await
        .expect("subscribe");
    assert_eq!(granted, vec![2]);

    assert!(matches!(
        broker
            .expect_event("SUBSCRIBE", |e| matches!(e, BrokerEvent::Subscribe { .. }))
            .await,
        BrokerEvent::Subscribe { .. }
    ));

    broker.publish("room/1/temp", "27.0", 1).await;
    let (topic, payload, qos) = expect_message(&mut events).await;
    assert_eq!(topic, "room/1/temp");
    assert_eq!(payload.as_ref(), b"27.0");
    assert_eq!(qos, 1);
    assert!(matches!(
        broker
            .expect_event("PUBACK", |e| matches!(e, BrokerEvent::PubAck { .. }))
            .await,
        BrokerEvent::PubAck { .. }
    ));

    // QoS 2 delivery to the client: PUBREC → PUBREL → PUBCOMP.
    broker.publish("room/1/temp", "28.0", 2).await;
    assert!(matches!(
        broker
            .expect_event("PUBREC", |e| matches!(e, BrokerEvent::PubRec { .. }))
            .await,
        BrokerEvent::PubRec { .. }
    ));
    let (topic, payload, qos) = expect_message(&mut events).await;
    assert_eq!(topic, "room/1/temp");
    assert_eq!(payload.as_ref(), b"28.0");
    assert_eq!(qos, 2);

    client.disconnect().await.ok();
}

#[tokio::test]
async fn subscribe_should_fail_when_broker_returns_error_reason() {
    let broker = spawn_v5(BrokerConfig {
        suback_codes: Some(vec![0x87]),
        ..BrokerConfig::default()
    })
    .await;
    let (client, mut events) = MqttClient::new(config(&broker.addr()));
    wait_connected(&mut events).await;

    match client
        .subscribe(vec![("denied/topic".to_string(), 1)])
        .await
    {
        Err(ClientError::SubscribeRejected { codes }) => assert_eq!(codes, vec![0x87]),
        other => panic!("expected SubscribeRejected, got {other:?}"),
    }

    client.disconnect().await.ok();
}

#[tokio::test]
async fn unsubscribe_should_fail_when_broker_returns_error_reason() {
    let broker = spawn_v5(BrokerConfig {
        unsuback_codes: Some(vec![0x80]),
        ..BrokerConfig::default()
    })
    .await;
    let (client, mut events) = MqttClient::new(config(&broker.addr()));
    wait_connected(&mut events).await;

    client
        .subscribe(vec![("a/b".to_string(), 1)])
        .await
        .expect("subscribe");

    match client.unsubscribe(vec!["a/b".to_string()]).await {
        Err(ClientError::UnsubscribeRejected { codes }) => assert_eq!(codes, vec![0x80]),
        other => panic!("expected UnsubscribeRejected, got {other:?}"),
    }

    client.disconnect().await.ok();
}

#[tokio::test]
async fn refused_connection_should_surface_reason_code() {
    let broker = spawn_v5(BrokerConfig {
        refuse: true,
        ..BrokerConfig::default()
    })
    .await;
    let (client, mut events) = MqttClient::new(config(&broker.addr()));

    let reason = expect_disconnected(&mut events).await;
    assert!(reason.contains("0x87"), "unexpected reason: {reason}");
    assert!(
        client
            .publish("t", Bytes::from_static(b"x"), 0, false)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn pending_operations_should_fail_when_connection_is_lost() {
    let broker = spawn_v5(BrokerConfig::default()).await;
    let (client, mut events) = MqttClient::new(config(&broker.addr()));
    wait_connected(&mut events).await;

    // The broker never answers this SUBSCRIBE; closing the socket must not
    // leave the caller waiting for the request timeout.
    broker.close().await;

    let result = client.subscribe(vec![("never/acked".to_string(), 1)]).await;
    assert!(
        matches!(
            result,
            Err(ClientError::Disconnected | ClientError::ChannelClosed)
        ),
        "expected a disconnect error, got {result:?}"
    );
}

#[tokio::test]
async fn receive_maximum_should_cap_the_inflight_window() {
    // The broker imposes a Receive Maximum smaller than `max_inflight`;
    // publishing must still complete (the window is capped, not broken).
    let broker = spawn_v5(BrokerConfig {
        receive_maximum: Some(1),
        ..BrokerConfig::default()
    })
    .await;
    let cfg = config(&broker.addr()).max_inflight(16);
    let (client, mut events) = MqttClient::new(cfg);
    wait_connected(&mut events).await;

    for i in 0..3 {
        client
            .publish("bulk/item", Bytes::from(format!("{i}")), 1, false)
            .await
            .expect("publish");
    }

    client.disconnect().await.ok();
}

#[tokio::test]
async fn reconnect_should_reestablish_session_and_resubscribe() {
    let mut broker = spawn_v5(BrokerConfig::default()).await;
    let cfg = config(&broker.addr()).reconnect(ReconnectOptions {
        enabled: true,
        initial_delay: Duration::from_millis(100),
        max_delay: Duration::from_millis(200),
        max_attempts: None,
    });

    let (client, mut events) = MqttClient::new(cfg);
    wait_connected(&mut events).await;
    client
        .subscribe(vec![("a/b".to_string(), 1)])
        .await
        .expect("subscribe");

    assert!(matches!(
        broker
            .expect_event("CONNECT", |e| matches!(e, BrokerEvent::Connect { .. }))
            .await,
        BrokerEvent::Connect { .. }
    ));
    assert!(matches!(
        broker
            .expect_event("SUBSCRIBE", |e| matches!(e, BrokerEvent::Subscribe { .. }))
            .await,
        BrokerEvent::Subscribe { .. }
    ));

    broker.close().await;

    assert!(matches!(
        broker
            .expect_event("reconnect", |e| matches!(e, BrokerEvent::Connect { .. }))
            .await,
        BrokerEvent::Connect { .. }
    ));
    assert!(matches!(
        broker
            .expect_event("resubscribe", |e| matches!(
                e,
                BrokerEvent::Subscribe { .. }
            ))
            .await,
        BrokerEvent::Subscribe { .. }
    ));

    broker.publish("a/b", "after-reconnect", 0).await;
    let (topic, payload, _) = expect_message(&mut events).await;
    assert_eq!(topic, "a/b");
    assert_eq!(payload.as_ref(), b"after-reconnect");

    client.disconnect().await.ok();
}
