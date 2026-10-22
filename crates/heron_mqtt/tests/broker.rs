//! TCP 与 WebSocket 上的协议集成测试。

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use heron_core::{Broker, BrokerConfig, MemoryStorage};
use heron_mqtt::Server;
use futures_util::{SinkExt, StreamExt};
use heron_client::{ClientConfig, Event, MqttClient, ProtocolVersion};
use heron_codec::v4::{self, ConnectPacket, ConnectReturnCode, MqttCodec, Packet};
use heron_codec::v5::{self, ReasonCode};
use heron_codec::{Decoder, Encoder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

async fn start() -> (
    std::net::SocketAddr,
    std::net::SocketAddr,
    tokio::sync::oneshot::Sender<()>,
) {
    let broker = Broker::open(Arc::new(MemoryStorage::new()), BrokerConfig::default())
        .await
        .expect("broker");
    let server = Server::bind(
        broker,
        Some("127.0.0.1:0".parse().expect("tcp")),
        Some("127.0.0.1:0".parse().expect("ws")),
    )
    .await
    .expect("bind");
    let tcp = server.tcp_addr().expect("tcp addr");
    let websocket = server.websocket_addr().expect("ws addr");
    let (shutdown, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let _ = server
            .serve(async move {
                let _ = rx.await;
            })
            .await;
    });
    (tcp, websocket, shutdown)
}

fn client(
    addr: std::net::SocketAddr,
    id: &str,
    protocol: ProtocolVersion,
    clean: bool,
) -> ClientConfig {
    let mut config = ClientConfig::new(addr.to_string(), id);
    config.protocol_version = protocol;
    config.clean_session = clean;
    config.reconnect.enabled = false;
    config.request_timeout = Duration::from_secs(3);
    config.connect_timeout = Duration::from_secs(3);
    config
}

async fn connected(events: &mut tokio::sync::mpsc::Receiver<Event>) -> bool {
    timeout(Duration::from_secs(3), async {
        while let Some(event) = events.recv().await {
            if let Event::Connected { session_present } = event {
                return session_present;
            }
        }
        false
    })
    .await
    .expect("connected")
}

async fn next_message(events: &mut tokio::sync::mpsc::Receiver<Event>) -> Event {
    timeout(Duration::from_secs(3), async {
        while let Some(event) = events.recv().await {
            if matches!(event, Event::Message { .. }) {
                return event;
            }
        }
        panic!("event stream closed");
    })
    .await
    .expect("message")
}

#[tokio::test]
async fn tcp_v4_should_route_qos0_and_qos1() {
    let (addr, _, _shutdown) = start().await;
    let (subscriber, mut events) = MqttClient::new(client(addr, "sub", ProtocolVersion::V4, true));
    assert!(!connected(&mut events).await);
    subscriber
        .subscribe(vec![("sensors/#".into(), 1)])
        .await
        .expect("subscribe");

    let (publisher, mut pub_events) =
        MqttClient::new(client(addr, "pub", ProtocolVersion::V4, true));
    let _ = connected(&mut pub_events).await;
    publisher
        .publish("sensors/temp", "21", 0, false)
        .await
        .expect("qos0");
    match next_message(&mut events).await {
        Event::Message {
            topic,
            payload,
            qos,
            ..
        } => {
            assert_eq!(topic, "sensors/temp");
            assert_eq!(payload.as_ref(), b"21");
            assert_eq!(qos, 0);
        }
        other => panic!("unexpected {other:?}"),
    }

    publisher
        .publish("sensors/temp", "22", 1, false)
        .await
        .expect("qos1");
    match next_message(&mut events).await {
        Event::Message { payload, qos, .. } => {
            assert_eq!(payload.as_ref(), b"22");
            assert_eq!(qos, 1);
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn tcp_v4_qos2_should_complete_the_handshake() {
    let (addr, _, _shutdown) = start().await;
    let (subscriber, mut events) = MqttClient::new(client(addr, "sub2", ProtocolVersion::V4, true));
    let _ = connected(&mut events).await;
    subscriber
        .subscribe(vec![("q2".into(), 2)])
        .await
        .expect("subscribe");
    let (publisher, mut pub_events) =
        MqttClient::new(client(addr, "pub2", ProtocolVersion::V4, true));
    let _ = connected(&mut pub_events).await;
    publisher
        .publish("q2", "once", 2, false)
        .await
        .expect("qos2");
    match next_message(&mut events).await {
        Event::Message { payload, qos, .. } => {
            assert_eq!(payload.as_ref(), b"once");
            assert_eq!(qos, 2);
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn tcp_v5_should_publish_and_restore_a_persistent_session() {
    let (addr, _, _shutdown) = start().await;
    let mut config = client(addr, "persist", ProtocolVersion::V4, false);
    config.clean_session = false;
    let (subscriber, mut events) = MqttClient::new(config);
    assert!(!connected(&mut events).await);
    subscriber
        .subscribe(vec![("keep".into(), 1)])
        .await
        .expect("subscribe");
    subscriber.disconnect().await.expect("disconnect");

    let (publisher, mut pub_events) =
        MqttClient::new(client(addr, "other", ProtocolVersion::V5, true));
    let _ = connected(&mut pub_events).await;
    publisher
        .publish("keep", "later", 1, false)
        .await
        .expect("publish");

    let (subscriber, mut events) =
        MqttClient::new(client(addr, "persist", ProtocolVersion::V4, false));
    assert!(connected(&mut events).await);
    match next_message(&mut events).await {
        Event::Message { payload, .. } => assert_eq!(payload.as_ref(), b"later"),
        other => panic!("unexpected {other:?}"),
    }
    let _ = subscriber;
}

#[tokio::test]
async fn websocket_should_accept_mqtt_subprotocol_and_connack() {
    let (_, addr, _shutdown) = start().await;
    let mut request = format!("ws://{addr}/mqtt")
        .into_client_request()
        .expect("request");
    request
        .headers_mut()
        .insert("Sec-WebSocket-Protocol", "mqtt".parse().expect("protocol"));
    let (mut socket, response) = tokio_tungstenite::connect_async(request)
        .await
        .expect("upgrade");
    assert_eq!(
        response
            .headers()
            .get("sec-websocket-protocol")
            .and_then(|v| v.to_str().ok()),
        Some("mqtt")
    );

    let connect = Packet::Connect(ConnectPacket {
        protocol_name: "MQTT".into(),
        protocol_level: 4,
        clean_session: true,
        keep_alive: 30,
        client_id: "ws-client".into(),
        ..ConnectPacket::default()
    });
    let mut encoded = BytesMut::new();
    MqttCodec::new()
        .encode(connect, &mut encoded)
        .expect("encode");
    socket
        .send(Message::Binary(encoded.to_vec().into()))
        .await
        .expect("send");

    let frame = timeout(Duration::from_secs(3), socket.next())
        .await
        .expect("frame timeout")
        .expect("closed")
        .expect("frame");
    let Message::Binary(bytes) = frame else {
        panic!("expected binary connack");
    };
    let mut buf = BytesMut::from(bytes.as_ref());
    let packet = MqttCodec::new()
        .decode(&mut buf)
        .expect("decode")
        .expect("packet");
    let Packet::ConnAck(ack) = packet else {
        panic!("expected connack");
    };
    assert!(ack.return_code.is_success());
}

#[tokio::test]
async fn tcp_should_refuse_invalid_client_id_with_protocol_connack_code() {
    let (addr, _, _shutdown) = start().await;

    let long_id = "a".repeat(256);
    let v5_ack = send_v5(
        addr,
        v5::Packet::Connect(v5::ConnectPacket::builder(long_id).build()),
    )
    .await;
    assert_eq!(v5_ack.reason_code, ReasonCode::ClientIdentifierNotValid);

    let mut nul_id = String::from("bad");
    nul_id.push('\0');
    let v5_nul = send_v5(
        addr,
        v5::Packet::Connect(v5::ConnectPacket::builder(nul_id).build()),
    )
    .await;
    assert_eq!(v5_nul.reason_code, ReasonCode::ClientIdentifierNotValid);

    let v4_ack = send_v4(
        addr,
        Packet::Connect(ConnectPacket {
            client_id: "b".repeat(256),
            ..ConnectPacket::default()
        }),
    )
    .await;
    assert_eq!(v4_ack.return_code, ConnectReturnCode::IdentifierRejected);
}

async fn send_v5(addr: std::net::SocketAddr, packet: v5::Packet) -> v5::ConnAckPacket {
    let mut encoded = BytesMut::new();
    v5::MqttCodec::new()
        .encode(packet, &mut encoded)
        .expect("encode");
    let mut buf = exchange(addr, &encoded).await;
    let packet = v5::MqttCodec::new()
        .decode(&mut buf)
        .expect("decode")
        .expect("packet");
    let v5::Packet::ConnAck(ack) = packet else {
        panic!("expected connack");
    };
    ack
}

async fn send_v4(addr: std::net::SocketAddr, packet: Packet) -> v4::ConnAckPacket {
    let mut encoded = BytesMut::new();
    MqttCodec::new().encode(packet, &mut encoded).expect("encode");
    let mut buf = exchange(addr, &encoded).await;
    let packet = MqttCodec::new()
        .decode(&mut buf)
        .expect("decode")
        .expect("packet");
    let Packet::ConnAck(ack) = packet else {
        panic!("expected connack");
    };
    ack
}

async fn exchange(addr: std::net::SocketAddr, encoded: &[u8]) -> BytesMut {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream.write_all(encoded).await.expect("write");
    let mut buf = BytesMut::new();
    let n = timeout(Duration::from_secs(3), stream.read_buf(&mut buf))
        .await
        .expect("timeout")
        .expect("read");
    assert!(n > 0, "broker closed without a connack");
    buf
}
