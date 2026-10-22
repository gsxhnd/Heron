//! MQTT v3.1.1 integration tests against the in-process mock broker.

mod support;

use std::time::Duration;

use bytes::Bytes;
use heron_client::{
    ClientConfig, ClientError, Event, MqttClient, ProtocolVersion, ReconnectOptions,
};
use support::{
    BrokerConfig, BrokerEvent, expect_disconnected, expect_message, spawn_v4, wait_connected,
};

fn config(addr: &str) -> ClientConfig {
    ClientConfig::new(addr, "v4-client")
        .protocol_version(ProtocolVersion::V4)
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
    let mut broker = spawn_v4(BrokerConfig {
        session_present: true,
        ..BrokerConfig::default()
    })
    .await;

    let (_client, mut events) = MqttClient::new(config(&broker.addr()));
    let session_present = wait_connected(&mut events).await;
    assert!(session_present);

    match broker.next_event().await {
        BrokerEvent::Connect {
            client_id,
            keep_alive,
            clean_session,
        } => {
            assert_eq!(client_id, "v4-client");
            assert_eq!(keep_alive, 60);
            assert!(clean_session);
        }
        other => panic!("expected CONNECT, got {other:?}"),
    }
}

#[tokio::test]
async fn publish_qos0_should_be_written_and_ack_locally() {
    let mut broker = spawn_v4(BrokerConfig::default()).await;
    let (client, mut events) = MqttClient::new(config(&broker.addr()));
    wait_connected(&mut events).await;

    client
        .publish("sensor/temp", Bytes::from_static(b"21.5"), 0, false)
        .await
        .expect("qos 0 publish");

    match broker
        .expect_event("PUBLISH", |e| matches!(e, BrokerEvent::Publish { .. }))
        .await
    {
        BrokerEvent::Publish {
            topic,
            payload,
            qos,
            retain,
            duplicate,
        } => {
            assert_eq!(topic, "sensor/temp");
            assert_eq!(payload.as_ref(), b"21.5");
            assert_eq!(qos, 0);
            assert!(!retain);
            assert!(!duplicate);
        }
        other => panic!("expected PUBLISH, got {other:?}"),
    }

    client.disconnect().await.ok();
}

#[tokio::test]
async fn publish_qos1_should_wait_for_puback() {
    let mut broker = spawn_v4(BrokerConfig::default()).await;
    let (client, mut events) = MqttClient::new(config(&broker.addr()));
    wait_connected(&mut events).await;

    client
        .publish("sensor/temp", Bytes::from_static(b"22"), 1, true)
        .await
        .expect("qos 1 publish");

    match broker
        .expect_event("PUBLISH", |e| matches!(e, BrokerEvent::Publish { .. }))
        .await
    {
        BrokerEvent::Publish {
            topic, qos, retain, ..
        } => {
            assert_eq!(topic, "sensor/temp");
            assert_eq!(qos, 1);
            assert!(retain);
        }
        other => panic!("expected PUBLISH, got {other:?}"),
    }

    // PUBACK is reported through the event stream as well.
    let ack = tokio::time::timeout(Duration::from_secs(2), events.recv())
        .await
        .expect("PUBACK event");
    assert!(matches!(ack, Some(Event::PubAck { .. })));
    client.disconnect().await.ok();
}

#[tokio::test]
async fn publish_qos2_should_complete_pubrec_pubrel_pubcomp() {
    let mut broker = spawn_v4(BrokerConfig::default()).await;
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
    // Client answers PUBREC with PUBREL, broker answers with PUBCOMP.
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
async fn subscribe_should_return_granted_qos_and_receive_messages() {
    let mut broker = spawn_v4(BrokerConfig::default()).await;
    let (client, mut events) = MqttClient::new(config(&broker.addr()));
    wait_connected(&mut events).await;

    let granted = client
        .subscribe(vec![("room/+/temp".to_string(), 1)])
        .await
        .expect("subscribe");
    assert_eq!(granted, vec![1]);

    assert!(matches!(
        broker
            .expect_event("SUBSCRIBE", |e| matches!(e, BrokerEvent::Subscribe { .. }))
            .await,
        BrokerEvent::Subscribe { .. }
    ));

    broker.publish("room/1/temp", "25.0", 1).await;
    let (topic, payload, qos) = expect_message(&mut events).await;
    assert_eq!(topic, "room/1/temp");
    assert_eq!(payload.as_ref(), b"25.0");
    assert_eq!(qos, 1);

    // QoS 2 inbound: PUBREC → PUBREL → PUBCOMP before delivery.
    broker.publish("room/1/temp", "26.0", 2).await;
    let (topic, payload, qos) = expect_message(&mut events).await;
    assert_eq!(topic, "room/1/temp");
    assert_eq!(payload.as_ref(), b"26.0");
    assert_eq!(qos, 2);
    assert!(matches!(
        broker
            .expect_event("PUBCOMP", |e| matches!(e, BrokerEvent::PubComp { .. }))
            .await,
        BrokerEvent::PubComp { .. }
    ));

    client.disconnect().await.ok();
}

#[tokio::test]
async fn subscribe_should_fail_when_broker_rejects_filter() {
    let broker = spawn_v4(BrokerConfig {
        suback_codes: Some(vec![0x80]),
        ..BrokerConfig::default()
    })
    .await;
    let (client, mut events) = MqttClient::new(config(&broker.addr()));
    wait_connected(&mut events).await;

    let result = client
        .subscribe(vec![("denied/topic".to_string(), 1)])
        .await;
    match result {
        Err(ClientError::SubscribeRejected { codes }) => assert_eq!(codes, vec![0x80]),
        other => panic!("expected SubscribeRejected, got {other:?}"),
    }

    client.disconnect().await.ok();
}

#[tokio::test]
async fn unsubscribe_should_wait_for_unsuback() {
    let mut broker = spawn_v4(BrokerConfig::default()).await;
    let (client, mut events) = MqttClient::new(config(&broker.addr()));
    wait_connected(&mut events).await;

    client
        .subscribe(vec![("a/b".to_string(), 0)])
        .await
        .expect("subscribe");
    client
        .unsubscribe(vec!["a/b".to_string()])
        .await
        .expect("unsubscribe");

    match broker
        .expect_event("UNSUBSCRIBE", |e| {
            matches!(e, BrokerEvent::Unsubscribe { .. })
        })
        .await
    {
        BrokerEvent::Unsubscribe { filters, .. } => assert_eq!(filters, vec!["a/b".to_string()]),
        other => panic!("expected UNSUBSCRIBE, got {other:?}"),
    }

    client.disconnect().await.ok();
}

#[tokio::test]
async fn publish_should_reject_invalid_qos_before_touching_the_network() {
    let (client, _events) = MqttClient::new(config("127.0.0.1:1"));

    match client
        .publish("t", Bytes::from_static(b"x"), 3, false)
        .await
    {
        Err(ClientError::InvalidQoS(3)) => {}
        other => panic!("expected InvalidQoS(3), got {other:?}"),
    }
}

#[tokio::test]
async fn refused_connection_should_surface_reason_and_fail_operations() {
    let broker = spawn_v4(BrokerConfig {
        refuse: true,
        ..BrokerConfig::default()
    })
    .await;
    let (client, mut events) = MqttClient::new(config(&broker.addr()));

    let reason = expect_disconnected(&mut events).await;
    assert!(
        reason.to_ascii_lowercase().contains("not author"),
        "unexpected reason: {reason}"
    );

    let result = client
        .publish("t", Bytes::from_static(b"x"), 0, false)
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn missing_connack_should_abort_after_connect_timeout() {
    let broker = spawn_v4(BrokerConfig {
        silent: true,
        ..BrokerConfig::default()
    })
    .await;
    let cfg = config(&broker.addr()).connect_timeout(Duration::from_millis(300));

    let (client, mut events) = MqttClient::new(cfg);
    let reason = expect_disconnected(&mut events).await;
    assert!(reason.contains("CONNACK"), "unexpected reason: {reason}");

    assert!(
        client
            .publish("t", Bytes::from_static(b"x"), 0, false)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn disconnect_should_send_disconnect_packet() {
    let mut broker = spawn_v4(BrokerConfig::default()).await;
    let (client, mut events) = MqttClient::new(config(&broker.addr()));
    wait_connected(&mut events).await;

    client.disconnect().await.expect("disconnect");

    assert!(matches!(
        broker
            .expect_event("DISCONNECT", |e| matches!(e, BrokerEvent::Disconnect))
            .await,
        BrokerEvent::Disconnect
    ));
}

#[tokio::test]
async fn keep_alive_should_ping_when_connection_is_idle() {
    let mut broker = spawn_v4(BrokerConfig::default()).await;
    let cfg = ClientConfig::new(broker.addr(), "v4-client")
        .keep_alive(1)
        .connect_timeout(Duration::from_secs(2))
        .request_timeout(Duration::from_secs(3))
        .reconnect(ReconnectOptions {
            enabled: false,
            ..ReconnectOptions::default()
        });

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
async fn reconnect_should_reestablish_session_and_resubscribe() {
    let mut broker = spawn_v4(BrokerConfig::default()).await;
    let cfg = ClientConfig::new(broker.addr(), "v4-client")
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
    client
        .subscribe(vec![("a/b".to_string(), 1)])
        .await
        .expect("subscribe");

    // First CONNECT + SUBSCRIBE.
    assert!(matches!(
        broker.next_event().await,
        BrokerEvent::Connect { .. }
    ));
    assert!(matches!(
        broker.next_event().await,
        BrokerEvent::Subscribe { .. }
    ));

    broker.close().await;

    // The client reconnects and replays its subscriptions.
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

    // Session still usable after the reconnect.
    broker.publish("a/b", "after-reconnect", 0).await;
    let (topic, payload, _) = expect_message(&mut events).await;
    assert_eq!(topic, "a/b");
    assert_eq!(payload.as_ref(), b"after-reconnect");

    client.disconnect().await.ok();
}

#[tokio::test]
async fn unanswered_pingreq_should_terminate_the_connection() {
    let broker = spawn_v4(BrokerConfig {
        ignore_pings: true,
        ..BrokerConfig::default()
    })
    .await;
    let cfg = ClientConfig::new(broker.addr(), "v4-client")
        .keep_alive(1)
        .connect_timeout(Duration::from_secs(2))
        .request_timeout(Duration::from_secs(5))
        .reconnect(ReconnectOptions {
            enabled: false,
            ..ReconnectOptions::default()
        });

    let (_client, mut events) = MqttClient::new(cfg);
    wait_connected(&mut events).await;

    let reason = expect_disconnected(&mut events).await;
    assert!(reason.contains("keep-alive"), "unexpected reason: {reason}");
}
