#![allow(dead_code)]
//! In-process MQTT broker used by the integration tests.
//!
//! The broker speaks real MQTT over a loopback TCP socket using `mqtt_codec`,
//! so the client is exercised end-to-end: connect handshake, QoS flows,
//! keep-alive, reconnects and error paths.

use std::time::Duration;

use bytes::{Bytes, BytesMut};
use mqtt_client::Event;
use mqtt_codec::{Decoder, Encoder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// How long tests wait for the broker or the client before failing.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Behaviour knobs for the broker.
#[derive(Debug, Clone, Default)]
pub struct BrokerConfig {
    /// Report a previous session in CONNACK.
    pub session_present: bool,
    /// Refuse the connection.
    pub refuse: bool,
    /// Never answer CONNECT at all.
    pub silent: bool,
    /// Do not answer PINGREQ (simulates a dead broker).
    pub ignore_pings: bool,
    /// Delay before CONNACK.
    pub connack_delay: Option<Duration>,
    /// SUBACK codes; defaults to the requested QoS.
    pub suback_codes: Option<Vec<u8>>,
    /// UNSUBACK codes (v5 only); defaults to success.
    pub unsuback_codes: Option<Vec<u8>>,
    /// v5: Server Keep Alive property.
    pub server_keep_alive: Option<u16>,
    /// v5: Receive Maximum property.
    pub receive_maximum: Option<u16>,
    /// v5: Assigned Client Identifier property.
    pub assigned_client_id: Option<String>,
}

/// Something the broker observed on the wire.
#[derive(Debug)]
pub enum BrokerEvent {
    Connect {
        client_id: String,
        keep_alive: u16,
        clean_session: bool,
    },
    Subscribe {
        packet_id: u16,
        filters: Vec<(String, u8)>,
    },
    Unsubscribe {
        packet_id: u16,
        filters: Vec<String>,
    },
    Publish {
        topic: String,
        payload: Bytes,
        qos: u8,
        retain: bool,
        duplicate: bool,
    },
    /// Client acknowledged a broker → client QoS 1 delivery.
    PubAck {
        packet_id: u16,
    },
    /// Client received a broker → client QoS 2 delivery.
    PubRec {
        packet_id: u16,
    },
    /// PUBREL for a broker → client QoS 2 delivery.
    PubRel {
        packet_id: u16,
    },
    /// PUBCOMP for a broker → client QoS 2 delivery.
    PubComp {
        packet_id: u16,
    },
    Ping,
    Disconnect,
}

/// An instruction the test drives into the broker.
pub enum BrokerCommand {
    Publish {
        topic: String,
        payload: Bytes,
        qos: u8,
    },
    Close,
}

/// Handle to a running mock broker.
pub struct BrokerHandle {
    addr: std::net::SocketAddr,
    events: mpsc::Receiver<BrokerEvent>,
    commands: mpsc::Sender<BrokerCommand>,
    _task: JoinHandle<()>,
}

impl BrokerHandle {
    /// Broker address in `host:port` form.
    pub fn addr(&self) -> String {
        self.addr.to_string()
    }

    /// Deliver a message from the broker to the connected client.
    pub async fn publish(&self, topic: &str, payload: &str, qos: u8) {
        let _ = self
            .commands
            .send(BrokerCommand::Publish {
                topic: topic.to_string(),
                payload: Bytes::from(payload.to_string()),
                qos,
            })
            .await;
    }

    /// Drop the current connection (the broker accepts the next one).
    pub async fn close(&self) {
        let _ = self.commands.send(BrokerCommand::Close).await;
    }

    /// Next observed event, panicking after [`DEFAULT_TIMEOUT`].
    pub async fn next_event(&mut self) -> BrokerEvent {
        tokio::time::timeout(DEFAULT_TIMEOUT, self.events.recv())
            .await
            .expect("timed out waiting for a broker event")
            .expect("broker event channel closed")
    }

    /// Skip events until one matches `pred`.
    pub async fn expect_event<F>(&mut self, label: &str, pred: F) -> BrokerEvent
    where
        F: Fn(&BrokerEvent) -> bool,
    {
        let deadline = tokio::time::Instant::now() + DEFAULT_TIMEOUT;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                panic!("timed out waiting for {label}");
            }
            match tokio::time::timeout(remaining, self.events.recv()).await {
                Ok(Some(event)) if pred(&event) => return event,
                Ok(Some(_)) => continue,
                Ok(None) => panic!("broker channel closed while waiting for {label}"),
                Err(_) => panic!("timed out waiting for {label}"),
            }
        }
    }
}

/// Wait for the `Connected` event, returning `session_present`.
pub async fn wait_connected(events: &mut mpsc::Receiver<Event>) -> bool {
    loop {
        match tokio::time::timeout(DEFAULT_TIMEOUT, events.recv()).await {
            Ok(Some(Event::Connected { session_present })) => return session_present,
            Ok(Some(_)) => continue,
            Ok(None) => panic!("event channel closed before CONNACK"),
            Err(_) => panic!("timed out waiting for Connected"),
        }
    }
}

/// Wait for the `Disconnected` event, returning its reason.
pub async fn expect_disconnected(events: &mut mpsc::Receiver<Event>) -> String {
    loop {
        match tokio::time::timeout(DEFAULT_TIMEOUT, events.recv()).await {
            Ok(Some(Event::Disconnected { reason })) => return reason.unwrap_or_default(),
            Ok(Some(_)) => continue,
            Ok(None) => panic!("event channel closed before Disconnected"),
            Err(_) => panic!("timed out waiting for Disconnected"),
        }
    }
}

/// Wait for the next inbound `Message` event.
pub async fn expect_message(events: &mut mpsc::Receiver<Event>) -> (String, Bytes, u8) {
    loop {
        match tokio::time::timeout(DEFAULT_TIMEOUT, events.recv()).await {
            Ok(Some(Event::Message {
                topic,
                payload,
                qos,
                ..
            })) => return (topic, payload, qos),
            Ok(Some(_)) => continue,
            Ok(None) => panic!("event channel closed before Message"),
            Err(_) => panic!("timed out waiting for Message"),
        }
    }
}

/// Spawn a v3.1.1 broker on an ephemeral loopback port.
pub async fn spawn_v4(config: BrokerConfig) -> BrokerHandle {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock broker");
    let addr = listener.local_addr().expect("broker local addr");
    let (event_tx, event_rx) = mpsc::channel(64);
    let (cmd_tx, cmd_rx) = mpsc::channel(16);

    let task = tokio::spawn(async move {
        let mut cmd_rx = cmd_rx;
        while let Ok((socket, _)) = listener.accept().await {
            serve_v4(socket, &config, &event_tx, &mut cmd_rx).await;
        }
    });

    BrokerHandle {
        addr,
        events: event_rx,
        commands: cmd_tx,
        _task: task,
    }
}

/// Spawn a v5.0 broker on an ephemeral loopback port.
pub async fn spawn_v5(config: BrokerConfig) -> BrokerHandle {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock broker");
    let addr = listener.local_addr().expect("broker local addr");
    let (event_tx, event_rx) = mpsc::channel(64);
    let (cmd_tx, cmd_rx) = mpsc::channel(16);

    let task = tokio::spawn(async move {
        let mut cmd_rx = cmd_rx;
        while let Ok((socket, _)) = listener.accept().await {
            serve_v5(socket, &config, &event_tx, &mut cmd_rx).await;
        }
    });

    BrokerHandle {
        addr,
        events: event_rx,
        commands: cmd_tx,
        _task: task,
    }
}

// ===========================================================================
// v3.1.1
// ===========================================================================

type CodecV4 = mqtt_codec::v4::MqttCodec;
type PacketV4 = mqtt_codec::v4::Packet;

async fn serve_v4(
    mut socket: TcpStream,
    config: &BrokerConfig,
    event_tx: &mpsc::Sender<BrokerEvent>,
    cmd_rx: &mut mpsc::Receiver<BrokerCommand>,
) {
    let mut codec = CodecV4::new();
    let mut read_buf = BytesMut::with_capacity(4096);
    let mut write_buf = BytesMut::with_capacity(4096);
    let mut next_packet_id: u16 = 1;

    loop {
        tokio::select! {
            result = socket.read_buf(&mut read_buf) => {
                match result {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {}
                }
                while let Some(packet) = codec.decode(&mut read_buf).expect("broker decode v4") {
                    let stop = handle_v4(
                        &mut socket,
                        &mut codec,
                        &mut write_buf,
                        config,
                        event_tx,
                        packet,
                    )
                    .await;
                    if stop {
                        return;
                    }
                }
            }
            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    BrokerCommand::Publish { topic, payload, qos } => {
                        let packet_id = alloc_id(&mut next_packet_id, qos);
                        let packet = mqtt_codec::v4::PublishPacket {
                            topic_name: topic,
                            packet_id,
                            payload,
                            qos: mqtt_codec::v4::QoS::try_from(qos).expect("valid qos"),
                            duplicate: false,
                            retain: false,
                        };
                        write_v4(&mut socket, &mut codec, &mut write_buf, PacketV4::Publish(packet)).await;
                    }
                    BrokerCommand::Close => {
                        let _ = socket.shutdown().await;
                        return;
                    }
                }
            }
        }
    }
}

async fn handle_v4(
    socket: &mut TcpStream,
    codec: &mut CodecV4,
    write_buf: &mut BytesMut,
    config: &BrokerConfig,
    event_tx: &mpsc::Sender<BrokerEvent>,
    packet: PacketV4,
) -> bool {
    match packet {
        PacketV4::Connect(connect) => {
            let _ = event_tx
                .send(BrokerEvent::Connect {
                    client_id: connect.client_id.clone(),
                    keep_alive: connect.keep_alive,
                    clean_session: connect.clean_session,
                })
                .await;
            if config.silent {
                return false;
            }
            if let Some(delay) = config.connack_delay {
                tokio::time::sleep(delay).await;
            }
            let code = if config.refuse {
                mqtt_codec::v4::ConnectReturnCode::NotAuthorized
            } else {
                mqtt_codec::v4::ConnectReturnCode::Accepted
            };
            write_v4(
                socket,
                codec,
                write_buf,
                PacketV4::ConnAck(mqtt_codec::v4::ConnAckPacket::new(
                    code,
                    config.session_present,
                )),
            )
            .await;
            false
        }
        PacketV4::Subscribe(subscribe) => {
            let filters: Vec<(String, u8)> = subscribe
                .topics
                .iter()
                .map(|(filter, qos)| (filter.clone(), u8::from(*qos)))
                .collect();
            let codes = config
                .suback_codes
                .clone()
                .unwrap_or_else(|| filters.iter().map(|(_, qos)| granted_code(*qos)).collect());
            let return_codes = codes
                .iter()
                .map(|code| {
                    mqtt_codec::v4::SubAckReturnCode::try_from(*code).expect("valid suback code")
                })
                .collect();
            let _ = event_tx
                .send(BrokerEvent::Subscribe {
                    packet_id: subscribe.packet_id,
                    filters,
                })
                .await;
            write_v4(
                socket,
                codec,
                write_buf,
                PacketV4::SubAck(mqtt_codec::v4::SubAckPacket::new(
                    subscribe.packet_id,
                    return_codes,
                )),
            )
            .await;
            false
        }
        PacketV4::Unsubscribe(unsubscribe) => {
            let _ = event_tx
                .send(BrokerEvent::Unsubscribe {
                    packet_id: unsubscribe.packet_id,
                    filters: unsubscribe.topics.clone(),
                })
                .await;
            write_v4(
                socket,
                codec,
                write_buf,
                PacketV4::UnsubAck(mqtt_codec::v4::UnsubAckPacket {
                    packet_id: unsubscribe.packet_id,
                }),
            )
            .await;
            false
        }
        PacketV4::Publish(publish) => {
            let _ = event_tx
                .send(BrokerEvent::Publish {
                    topic: publish.topic_name.clone(),
                    payload: publish.payload.clone(),
                    qos: u8::from(publish.qos),
                    retain: publish.retain,
                    duplicate: publish.duplicate,
                })
                .await;
            let packet_id = publish.packet_id.unwrap_or_default();
            match publish.qos {
                mqtt_codec::v4::QoS::AtLeastOnce => {
                    write_v4(
                        socket,
                        codec,
                        write_buf,
                        PacketV4::PubAck(mqtt_codec::v4::PubAckPacket { packet_id }),
                    )
                    .await;
                }
                mqtt_codec::v4::QoS::ExactlyOnce => {
                    write_v4(
                        socket,
                        codec,
                        write_buf,
                        PacketV4::PubRec(mqtt_codec::v4::PubRecPacket { packet_id }),
                    )
                    .await;
                }
                mqtt_codec::v4::QoS::AtMostOnce => {}
            }
            false
        }
        PacketV4::PubRel(rel) => {
            let _ = event_tx
                .send(BrokerEvent::PubRel {
                    packet_id: rel.packet_id,
                })
                .await;
            write_v4(
                socket,
                codec,
                write_buf,
                PacketV4::PubComp(mqtt_codec::v4::PubCompPacket {
                    packet_id: rel.packet_id,
                }),
            )
            .await;
            false
        }
        PacketV4::PubComp(comp) => {
            let _ = event_tx
                .send(BrokerEvent::PubComp {
                    packet_id: comp.packet_id,
                })
                .await;
            false
        }
        PacketV4::PingReq(_) => {
            let _ = event_tx.send(BrokerEvent::Ping).await;
            if !config.ignore_pings {
                write_v4(
                    socket,
                    codec,
                    write_buf,
                    PacketV4::PingResp(Default::default()),
                )
                .await;
            }
            false
        }
        PacketV4::Disconnect(_) => {
            let _ = event_tx.send(BrokerEvent::Disconnect).await;
            true
        }
        PacketV4::PubAck(ack) => {
            let _ = event_tx
                .send(BrokerEvent::PubAck {
                    packet_id: ack.packet_id,
                })
                .await;
            false
        }
        // Client received our QoS 2 delivery: answer with PUBREL.
        PacketV4::PubRec(rec) => {
            let _ = event_tx
                .send(BrokerEvent::PubRec {
                    packet_id: rec.packet_id,
                })
                .await;
            write_v4(
                socket,
                codec,
                write_buf,
                PacketV4::PubRel(mqtt_codec::v4::PubRelPacket {
                    packet_id: rec.packet_id,
                }),
            )
            .await;
            false
        }
        other => panic!(
            "broker received unexpected packet: {:?}",
            other.packet_type()
        ),
    }
}

async fn write_v4(
    socket: &mut TcpStream,
    codec: &mut CodecV4,
    write_buf: &mut BytesMut,
    packet: PacketV4,
) {
    write_buf.clear();
    codec.encode(packet, write_buf).expect("broker encode v4");
    socket.write_all(write_buf).await.expect("broker write");
    socket.flush().await.expect("broker flush");
}

// ===========================================================================
// v5.0
// ===========================================================================

type CodecV5 = mqtt_codec::v5::MqttCodec;
type PacketV5 = mqtt_codec::v5::Packet;

async fn serve_v5(
    mut socket: TcpStream,
    config: &BrokerConfig,
    event_tx: &mpsc::Sender<BrokerEvent>,
    cmd_rx: &mut mpsc::Receiver<BrokerCommand>,
) {
    let mut codec = CodecV5::new();
    let mut read_buf = BytesMut::with_capacity(4096);
    let mut write_buf = BytesMut::with_capacity(4096);
    let mut next_packet_id: u16 = 1;

    loop {
        tokio::select! {
            result = socket.read_buf(&mut read_buf) => {
                match result {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {}
                }
                while let Some(packet) = codec.decode(&mut read_buf).expect("broker decode v5") {
                    let stop = handle_v5(
                        &mut socket,
                        &mut codec,
                        &mut write_buf,
                        config,
                        event_tx,
                        packet,
                    )
                    .await;
                    if stop {
                        return;
                    }
                }
            }
            Some(cmd) = cmd_rx.recv() => {
                match cmd {
                    BrokerCommand::Publish { topic, payload, qos } => {
                        let packet_id = alloc_id(&mut next_packet_id, qos);
                        let packet = mqtt_codec::v5::PublishPacket {
                            topic_name: topic,
                            packet_id,
                            payload,
                            qos: mqtt_codec::v5::QoS::try_from(qos).expect("valid qos"),
                            duplicate: false,
                            retain: false,
                            properties: mqtt_codec::v5::Properties::new(),
                        };
                        write_v5(&mut socket, &mut codec, &mut write_buf, PacketV5::Publish(packet)).await;
                    }
                    BrokerCommand::Close => {
                        let _ = socket.shutdown().await;
                        return;
                    }
                }
            }
        }
    }
}

async fn handle_v5(
    socket: &mut TcpStream,
    codec: &mut CodecV5,
    write_buf: &mut BytesMut,
    config: &BrokerConfig,
    event_tx: &mpsc::Sender<BrokerEvent>,
    packet: PacketV5,
) -> bool {
    use mqtt_codec::v5::{Properties, ReasonCode};

    match packet {
        PacketV5::Connect(connect) => {
            let _ = event_tx
                .send(BrokerEvent::Connect {
                    client_id: connect.client_id.clone(),
                    keep_alive: connect.keep_alive,
                    clean_session: connect.clean_start,
                })
                .await;
            if config.silent {
                return false;
            }
            if let Some(delay) = config.connack_delay {
                tokio::time::sleep(delay).await;
            }

            let mut properties = Properties::new();
            properties.server_keep_alive = config.server_keep_alive;
            properties.receive_maximum = config.receive_maximum;
            properties.assigned_client_identifier = config.assigned_client_id.clone();

            let ack = mqtt_codec::v5::ConnAckPacket {
                session_present: config.session_present,
                reason_code: if config.refuse {
                    ReasonCode::NotAuthorized
                } else {
                    ReasonCode::Success
                },
                properties,
            };
            write_v5(socket, codec, write_buf, PacketV5::ConnAck(ack)).await;
            false
        }
        PacketV5::Subscribe(subscribe) => {
            let filters: Vec<(String, u8)> = subscribe
                .topics
                .iter()
                .map(|option| (option.topic_filter.clone(), u8::from(option.qos)))
                .collect();
            let codes = config
                .suback_codes
                .clone()
                .unwrap_or_else(|| filters.iter().map(|(_, qos)| *qos).collect());
            let reason_codes = codes
                .iter()
                .map(|code| ReasonCode::try_from(*code).expect("valid reason code"))
                .collect();
            let _ = event_tx
                .send(BrokerEvent::Subscribe {
                    packet_id: subscribe.packet_id,
                    filters,
                })
                .await;
            write_v5(
                socket,
                codec,
                write_buf,
                PacketV5::SubAck(mqtt_codec::v5::SubAckPacket {
                    packet_id: subscribe.packet_id,
                    properties: Properties::new(),
                    reason_codes,
                }),
            )
            .await;
            false
        }
        PacketV5::Unsubscribe(unsubscribe) => {
            let _ = event_tx
                .send(BrokerEvent::Unsubscribe {
                    packet_id: unsubscribe.packet_id,
                    filters: unsubscribe.topics.clone(),
                })
                .await;
            let codes = config
                .unsuback_codes
                .clone()
                .unwrap_or_else(|| vec![0; unsubscribe.topics.len()]);
            let reason_codes = codes
                .iter()
                .map(|code| ReasonCode::try_from(*code).expect("valid reason code"))
                .collect();
            write_v5(
                socket,
                codec,
                write_buf,
                PacketV5::UnsubAck(mqtt_codec::v5::UnsubAckPacket {
                    packet_id: unsubscribe.packet_id,
                    properties: Properties::new(),
                    reason_codes,
                }),
            )
            .await;
            false
        }
        PacketV5::Publish(publish) => {
            let _ = event_tx
                .send(BrokerEvent::Publish {
                    topic: publish.topic_name.clone(),
                    payload: publish.payload.clone(),
                    qos: u8::from(publish.qos),
                    retain: publish.retain,
                    duplicate: publish.duplicate,
                })
                .await;
            let packet_id = publish.packet_id.unwrap_or_default();
            match publish.qos {
                mqtt_codec::v5::QoS::AtLeastOnce => {
                    write_v5(
                        socket,
                        codec,
                        write_buf,
                        PacketV5::PubAck(mqtt_codec::v5::PubAckPacket {
                            packet_id,
                            reason_code: ReasonCode::Success,
                            properties: Properties::new(),
                        }),
                    )
                    .await;
                }
                mqtt_codec::v5::QoS::ExactlyOnce => {
                    write_v5(
                        socket,
                        codec,
                        write_buf,
                        PacketV5::PubRec(mqtt_codec::v5::PubRecPacket {
                            packet_id,
                            reason_code: ReasonCode::Success,
                            properties: Properties::new(),
                        }),
                    )
                    .await;
                }
                mqtt_codec::v5::QoS::AtMostOnce => {}
            }
            false
        }
        PacketV5::PubRel(rel) => {
            let _ = event_tx
                .send(BrokerEvent::PubRel {
                    packet_id: rel.packet_id,
                })
                .await;
            write_v5(
                socket,
                codec,
                write_buf,
                PacketV5::PubComp(mqtt_codec::v5::PubCompPacket {
                    packet_id: rel.packet_id,
                    reason_code: ReasonCode::Success,
                    properties: Properties::new(),
                }),
            )
            .await;
            false
        }
        PacketV5::PubComp(comp) => {
            let _ = event_tx
                .send(BrokerEvent::PubComp {
                    packet_id: comp.packet_id,
                })
                .await;
            false
        }
        PacketV5::PingReq(_) => {
            let _ = event_tx.send(BrokerEvent::Ping).await;
            if !config.ignore_pings {
                write_v5(
                    socket,
                    codec,
                    write_buf,
                    PacketV5::PingResp(Default::default()),
                )
                .await;
            }
            false
        }
        PacketV5::Disconnect(_) => {
            let _ = event_tx.send(BrokerEvent::Disconnect).await;
            true
        }
        PacketV5::PubAck(ack) => {
            let _ = event_tx
                .send(BrokerEvent::PubAck {
                    packet_id: ack.packet_id,
                })
                .await;
            false
        }
        // Client received our QoS 2 delivery: answer with PUBREL.
        PacketV5::PubRec(rec) => {
            let _ = event_tx
                .send(BrokerEvent::PubRec {
                    packet_id: rec.packet_id,
                })
                .await;
            write_v5(
                socket,
                codec,
                write_buf,
                PacketV5::PubRel(mqtt_codec::v5::PubRelPacket {
                    packet_id: rec.packet_id,
                    reason_code: mqtt_codec::v5::ReasonCode::Success,
                    properties: mqtt_codec::v5::Properties::new(),
                }),
            )
            .await;
            false
        }
        PacketV5::Auth(_) => false,
        other => panic!(
            "broker received unexpected packet: {:?}",
            other.packet_type()
        ),
    }
}

async fn write_v5(
    socket: &mut TcpStream,
    codec: &mut CodecV5,
    write_buf: &mut BytesMut,
    packet: PacketV5,
) {
    write_buf.clear();
    codec.encode(packet, write_buf).expect("broker encode v5");
    socket.write_all(write_buf).await.expect("broker write");
    socket.flush().await.expect("broker flush");
}

/// v3.1.1 grants at most the requested QoS.
fn granted_code(requested: u8) -> u8 {
    requested.min(2)
}

fn alloc_id(next: &mut u16, qos: u8) -> Option<u16> {
    if qos == 0 {
        return None;
    }
    let id = *next;
    *next = if id == u16::MAX { 1 } else { id + 1 };
    Some(id)
}
