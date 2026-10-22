//! MQTT connection state machine shared by TCP and WebSocket.

use std::time::Duration;

use bytes::BytesMut;
use heron_core::{
    Broker, BrokerConfig, ClientAck, ClientFlow, ConnectError, ConnectRequest, ConnectedSession,
    DisconnectRequest, IncomingPublish, Outbound, PollEvent, ProtocolVersion, PublishError, QoS,
    SubOpts, SubscribeAck, SubscribeFailure, SubscribeFilter, Will,
};
use heron_codec::v4::{self, ConnectReturnCode, SubAckReturnCode};
use heron_codec::v5::{self, ReasonCode};
use heron_codec::{Decoder, Encoder, MqttError};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::{self, Instant};

const RETRY: Duration = Duration::from_secs(5);

/// Read and write an MQTT byte stream. TCP writes raw bytes; WebSocket writes one binary frame.
pub trait MqttIo: Send {
    /// Append newly received bytes to `buf`. `Ok(0)` means the peer closed the socket.
    fn read(
        &mut self,
        buf: &mut BytesMut,
    ) -> impl std::future::Future<Output = std::io::Result<usize>> + Send;

    /// Write one encoded burst.
    fn write_all(
        &mut self,
        bytes: &[u8],
    ) -> impl std::future::Future<Output = std::io::Result<()>> + Send;
}

/// Drive one accepted connection until it closes.
pub async fn serve_connection<S>(
    io: &mut S,
    broker: Broker,
    peer: Option<String>,
) -> std::io::Result<()>
where
    S: MqttIo,
{
    let config = broker.config();
    let mut buf = BytesMut::new();
    let wire = match read_protocol(io, &mut buf, config.max_packet_size).await? {
        Some(wire) => wire,
        None => return Ok(()),
    };
    let mut link = Link::new(wire);
    let Some(connect) = decode_one(&mut link, &mut buf).map_err(std::io::Error::other)? else {
        return Ok(());
    };
    let request = match connect_request(connect, peer) {
        Ok(request) => request,
        Err(refusal) => {
            write_packets(io, &mut link, &[BrokerPacket::ConnAckRefused(refusal)]).await?;
            return Ok(());
        }
    };
    let session = match broker.connect(request).await {
        Ok(session) => session,
        Err(err) => {
            write_packets(
                io,
                &mut link,
                &[BrokerPacket::ConnAckRefused(refusal_code(&err))],
            )
            .await?;
            return Ok(());
        }
    };
    tracing::info!(client_id = %session.client_id, "客户端已连接");
    let result = session_loop(io, &broker, &mut link, &mut buf, &session, &config).await;
    tracing::info!(client_id = %session.client_id, "客户端已断开");
    result
}

async fn session_loop<S: MqttIo>(
    io: &mut S,
    broker: &Broker,
    link: &mut Link,
    buf: &mut BytesMut,
    session: &ConnectedSession,
    config: &BrokerConfig,
) -> std::io::Result<()> {
    write_packets(
        io,
        link,
        &[BrokerPacket::ConnAck {
            session_present: session.session_present,
            server_assigned_id: session
                .server_assigned_id
                .then(|| session.client_id.clone()),
            receive_maximum: config.receive_maximum,
            max_packet_size: config.max_packet_size,
        }],
    )
    .await?;

    let mut deadline = keepalive_deadline(session.keep_alive);
    let mut retry_at = Instant::now() + RETRY;

    loop {
        let events = broker.poll(&session.client_id, session.epoch, 32).await;
        if write_events(io, link, &events).await? {
            break;
        }
        if events.len() == 32 {
            continue;
        }

        let notified = session.notify.notified();
        tokio::pin!(notified);
        let keepalive = async {
            match deadline {
                Some(at) => time::sleep_until(at).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            _ = &mut notified => {}
            read = io.read(buf) => {
                let n = read?;
                if n == 0 {
                    break;
                }
                if buf.len() > usize::try_from(config.max_packet_size).unwrap_or(usize::MAX).saturating_add(8) {
                    close_protocol(io, link, 0x95).await?;
                    break;
                }
                deadline = keepalive_deadline(session.keep_alive);
                if let Err(code) = handle_buffered(io, broker, link, buf, session).await? {
                    if code != 0 {
                        close_protocol(io, link, code).await?;
                    }
                    break;
                }
            }
            _ = keepalive => {
                close_protocol(io, link, 0x8D).await?;
                break;
            }
            _ = time::sleep_until(retry_at) => {
                broker.requeue_unacked(&session.client_id, session.epoch);
                retry_at = Instant::now() + RETRY;
            }
        }
    }

    broker
        .disconnect(DisconnectRequest {
            client_id: session.client_id.clone(),
            epoch: session.epoch,
            graceful: false,
            session_expiry: None,
        })
        .await
        .map_err(std::io::Error::other)?;
    Ok(())
}

/// `Ok(Err(code))` asks the caller to disconnect. `code == 0` closes without a DISCONNECT packet.
async fn handle_buffered<S: MqttIo>(
    io: &mut S,
    broker: &Broker,
    link: &mut Link,
    buf: &mut BytesMut,
    session: &ConnectedSession,
) -> std::io::Result<Result<(), u8>> {
    loop {
        let Some(packet) = decode_one(link, buf).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "malformed mqtt packet")
        })?
        else {
            return Ok(Ok(()));
        };
        let mut replies = Vec::new();
        match dispatch(broker, session, packet, &mut replies).await {
            Ok(Step::Continue) => {}
            Ok(Step::Graceful { session_expiry }) => {
                write_packets(io, link, &replies).await?;
                broker
                    .disconnect(DisconnectRequest {
                        client_id: session.client_id.clone(),
                        epoch: session.epoch,
                        graceful: true,
                        session_expiry,
                    })
                    .await
                    .map_err(std::io::Error::other)?;
                return Ok(Err(0));
            }
            Ok(Step::Protocol(code)) => return Ok(Err(code)),
            Err(err) => return Err(err),
        }
        write_packets(io, link, &replies).await?;
        let events = broker.poll(&session.client_id, session.epoch, 32).await;
        if write_events(io, link, &events).await? {
            broker
                .disconnect(DisconnectRequest {
                    client_id: session.client_id.clone(),
                    epoch: session.epoch,
                    graceful: false,
                    session_expiry: None,
                })
                .await
                .map_err(std::io::Error::other)?;
            return Ok(Err(0));
        }
    }
}

enum Step {
    Continue,
    Graceful { session_expiry: Option<u32> },
    Protocol(u8),
}

async fn dispatch(
    broker: &Broker,
    session: &ConnectedSession,
    packet: Decoded,
    replies: &mut Vec<BrokerPacket>,
) -> std::io::Result<Step> {
    match packet {
        Decoded::V4(packet) => dispatch_v4(broker, session, packet, replies).await,
        Decoded::V5(packet) => dispatch_v5(broker, session, *packet, replies).await,
    }
}

async fn dispatch_v4(
    broker: &Broker,
    session: &ConnectedSession,
    packet: v4::Packet,
    replies: &mut Vec<BrokerPacket>,
) -> std::io::Result<Step> {
    match packet {
        v4::Packet::Connect(_) => Ok(Step::Protocol(0x82)),
        v4::Packet::Publish(publish) => {
            publish_client(
                broker,
                session,
                IncomingPublish {
                    topic: publish.topic_name,
                    payload: publish.payload,
                    qos: core_qos(publish.qos as u8),
                    retain: publish.retain,
                    packet_id: publish.packet_id,
                },
                replies,
            )
            .await
        }
        v4::Packet::PubAck(ack) => {
            ack_client(broker, session, ack.packet_id, ClientAck::PubAck).await?;
            Ok(Step::Continue)
        }
        v4::Packet::PubRec(ack) => {
            ack_client(broker, session, ack.packet_id, ClientAck::PubRec).await?;
            Ok(Step::Continue)
        }
        v4::Packet::PubRel(ack) => finish_qos2(broker, session, ack.packet_id, replies).await,
        v4::Packet::PubComp(ack) => {
            ack_client(broker, session, ack.packet_id, ClientAck::PubComp).await?;
            Ok(Step::Continue)
        }
        v4::Packet::Subscribe(subscribe) => {
            let filters = subscribe
                .topics
                .into_iter()
                .map(|(filter, qos)| SubscribeFilter {
                    filter,
                    opts: SubOpts::v4(core_qos(qos as u8)),
                })
                .collect::<Vec<_>>();
            let acks = broker
                .subscribe(&session.client_id, session.epoch, &filters)
                .await
                .map_err(std::io::Error::other)?;
            replies.push(BrokerPacket::SubAck {
                packet_id: subscribe.packet_id,
                acks,
            });
            Ok(Step::Continue)
        }
        v4::Packet::Unsubscribe(unsubscribe) => {
            broker
                .unsubscribe(&session.client_id, session.epoch, &unsubscribe.topics)
                .await
                .map_err(std::io::Error::other)?;
            replies.push(BrokerPacket::UnsubAck {
                packet_id: unsubscribe.packet_id,
                count: 0,
            });
            Ok(Step::Continue)
        }
        v4::Packet::PingReq(_) => {
            replies.push(BrokerPacket::PingResp);
            Ok(Step::Continue)
        }
        v4::Packet::Disconnect(_) => Ok(Step::Graceful {
            session_expiry: None,
        }),
        v4::Packet::ConnAck(_)
        | v4::Packet::SubAck(_)
        | v4::Packet::UnsubAck(_)
        | v4::Packet::PingResp(_) => Ok(Step::Protocol(0x82)),
    }
}

async fn dispatch_v5(
    broker: &Broker,
    session: &ConnectedSession,
    packet: v5::Packet,
    replies: &mut Vec<BrokerPacket>,
) -> std::io::Result<Step> {
    match packet {
        v5::Packet::Connect(_) | v5::Packet::Auth(_) => Ok(Step::Protocol(0x82)),
        v5::Packet::Publish(publish) => {
            if publish.properties.topic_alias.is_some() && publish.topic_name.is_empty() {
                return Ok(Step::Protocol(0x94));
            }
            publish_client(
                broker,
                session,
                IncomingPublish {
                    topic: publish.topic_name,
                    payload: publish.payload,
                    qos: core_qos(publish.qos as u8),
                    retain: publish.retain,
                    packet_id: publish.packet_id,
                },
                replies,
            )
            .await
        }
        v5::Packet::PubAck(ack) => {
            ack_client(broker, session, ack.packet_id, ClientAck::PubAck).await?;
            Ok(Step::Continue)
        }
        v5::Packet::PubRec(ack) => {
            ack_client(broker, session, ack.packet_id, ClientAck::PubRec).await?;
            Ok(Step::Continue)
        }
        v5::Packet::PubRel(ack) => finish_qos2(broker, session, ack.packet_id, replies).await,
        v5::Packet::PubComp(ack) => {
            ack_client(broker, session, ack.packet_id, ClientAck::PubComp).await?;
            Ok(Step::Continue)
        }
        v5::Packet::Subscribe(subscribe) => {
            let filters = subscribe
                .topics
                .into_iter()
                .map(|topic| SubscribeFilter {
                    filter: topic.topic_filter,
                    opts: SubOpts {
                        qos: core_qos(topic.qos as u8),
                        no_local: topic.no_local,
                        retain_as_published: topic.retain_as_published,
                        retain_handling: topic.retain_handling,
                    },
                })
                .collect::<Vec<_>>();
            let acks = broker
                .subscribe(&session.client_id, session.epoch, &filters)
                .await
                .map_err(std::io::Error::other)?;
            replies.push(BrokerPacket::SubAck {
                packet_id: subscribe.packet_id,
                acks,
            });
            Ok(Step::Continue)
        }
        v5::Packet::Unsubscribe(unsubscribe) => {
            let count = unsubscribe.topics.len();
            broker
                .unsubscribe(&session.client_id, session.epoch, &unsubscribe.topics)
                .await
                .map_err(std::io::Error::other)?;
            replies.push(BrokerPacket::UnsubAck {
                packet_id: unsubscribe.packet_id,
                count,
            });
            Ok(Step::Continue)
        }
        v5::Packet::PingReq(_) => {
            replies.push(BrokerPacket::PingResp);
            Ok(Step::Continue)
        }
        v5::Packet::Disconnect(disconnect) => {
            let graceful = disconnect.reason_code == ReasonCode::Success;
            if graceful {
                Ok(Step::Graceful {
                    session_expiry: disconnect.properties.session_expiry_interval,
                })
            } else {
                broker
                    .disconnect(DisconnectRequest {
                        client_id: session.client_id.clone(),
                        epoch: session.epoch,
                        graceful: false,
                        session_expiry: disconnect.properties.session_expiry_interval,
                    })
                    .await
                    .map_err(std::io::Error::other)?;
                Ok(Step::Protocol(0))
            }
        }
        v5::Packet::ConnAck(_)
        | v5::Packet::SubAck(_)
        | v5::Packet::UnsubAck(_)
        | v5::Packet::PingResp(_) => Ok(Step::Protocol(0x82)),
    }
}

async fn publish_client(
    broker: &Broker,
    session: &ConnectedSession,
    publish: IncomingPublish,
    replies: &mut Vec<BrokerPacket>,
) -> std::io::Result<Step> {
    let packet_id = publish.packet_id;
    match broker
        .publish(&session.client_id, session.epoch, publish)
        .await
    {
        Ok(ClientFlow::Accepted) => Ok(Step::Continue),
        Ok(ClientFlow::PubAck) => {
            replies.push(BrokerPacket::PubAck {
                packet_id: packet_id.unwrap_or(0),
            });
            Ok(Step::Continue)
        }
        Ok(ClientFlow::PubRec) => {
            replies.push(BrokerPacket::PubRec {
                packet_id: packet_id.unwrap_or(0),
            });
            Ok(Step::Continue)
        }
        Ok(ClientFlow::PubComp) => Ok(Step::Continue),
        Err(PublishError::StaleSession) => Ok(Step::Protocol(0)),
        Err(PublishError::InvalidTopic) => Ok(Step::Protocol(0x90)),
        Err(PublishError::MissingPacketId) => Ok(Step::Protocol(0x82)),
        Err(PublishError::ReceiveMaximumExceeded) => Ok(Step::Protocol(0x93)),
    }
}

async fn finish_qos2(
    broker: &Broker,
    session: &ConnectedSession,
    packet_id: u16,
    replies: &mut Vec<BrokerPacket>,
) -> std::io::Result<Step> {
    match broker
        .pubrel(&session.client_id, session.epoch, packet_id)
        .await
    {
        Ok(_) => {
            replies.push(BrokerPacket::PubComp { packet_id });
            Ok(Step::Continue)
        }
        Err(PublishError::StaleSession) => Ok(Step::Protocol(0)),
        Err(_) => Ok(Step::Protocol(0x82)),
    }
}

async fn ack_client(
    broker: &Broker,
    session: &ConnectedSession,
    packet_id: u16,
    ack: ClientAck,
) -> std::io::Result<()> {
    broker
        .acknowledge(&session.client_id, session.epoch, packet_id, ack)
        .await
        .map_err(std::io::Error::other)
}

fn connect_request(packet: Decoded, peer: Option<String>) -> Result<ConnectRequest, u8> {
    match packet {
        Decoded::V4(v4::Packet::Connect(connect)) => {
            if connect.protocol_level != 4 {
                return Err(0x01);
            }
            Ok(ConnectRequest {
                client_id: connect.client_id,
                protocol: ProtocolVersion::V4,
                clean_start: connect.clean_session,
                session_expiry: connect.clean_session.then_some(0),
                keep_alive: connect.keep_alive,
                will: connect.will_flag.then(|| Will {
                    topic: connect.will_topic.unwrap_or_default(),
                    payload: connect.will_message.unwrap_or_default(),
                    qos: core_qos(connect.will_qos as u8),
                    retain: connect.will_retain,
                    delay_secs: 0,
                }),
                receive_maximum: 65_535,
                max_packet_size: 0,
                enhanced_auth: false,
                peer,
            })
        }
        Decoded::V5(packet) => {
            let v5::Packet::Connect(connect) = *packet else {
                return Err(0x82);
            };
            if connect.protocol_level != 5 {
                return Err(0x84);
            }
            if connect.properties.authentication_method.is_some() {
                return Err(0x8C);
            }
            let session_expiry = connect.properties.session_expiry_interval.unwrap_or(0);
            Ok(ConnectRequest {
                client_id: connect.client_id,
                protocol: ProtocolVersion::V5,
                clean_start: connect.clean_start,
                session_expiry: Some(session_expiry),
                keep_alive: connect.keep_alive,
                will: connect.will_flag.then(|| Will {
                    topic: connect.will_topic.unwrap_or_default(),
                    payload: connect.will_message.unwrap_or_default(),
                    qos: core_qos(connect.will_qos as u8),
                    retain: connect.will_retain,
                    delay_secs: connect
                        .will_properties
                        .as_ref()
                        .and_then(|props| props.will_delay_interval)
                        .unwrap_or(0),
                }),
                receive_maximum: connect.properties.receive_maximum.unwrap_or(65_535),
                max_packet_size: connect.properties.maximum_packet_size.unwrap_or(0),
                enhanced_auth: false,
                peer,
            })
        }
        _ => Err(0x82),
    }
}

fn refusal_code(err: &ConnectError) -> u8 {
    match err {
        ConnectError::IdentifierRejected => 0x02,
        ConnectError::BadAuthenticationMethod => 0x8C,
        ConnectError::ServerUnavailable(_) => 0x03,
    }
}

enum BrokerPacket {
    ConnAck {
        session_present: bool,
        server_assigned_id: Option<String>,
        receive_maximum: u16,
        max_packet_size: u32,
    },
    ConnAckRefused(u8),
    SubAck {
        packet_id: u16,
        acks: Vec<SubscribeAck>,
    },
    UnsubAck {
        packet_id: u16,
        count: usize,
    },
    PubAck {
        packet_id: u16,
    },
    PubRec {
        packet_id: u16,
    },
    PubComp {
        packet_id: u16,
    },
    PingResp,
    Outbound(Outbound),
    Disconnect(u8),
}

enum Link {
    V4(v4::MqttCodec),
    V5(v5::MqttCodec),
}

impl Link {
    fn new(level: u8) -> Self {
        if level == 5 {
            Self::V5(v5::MqttCodec::new())
        } else {
            Self::V4(v4::MqttCodec::new())
        }
    }

    fn is_v5(&self) -> bool {
        matches!(self, Self::V5(_))
    }
}

enum Decoded {
    V4(v4::Packet),
    V5(Box<v5::Packet>),
}

fn decode_one(link: &mut Link, buf: &mut BytesMut) -> Result<Option<Decoded>, MqttError> {
    match link {
        Link::V4(codec) => Ok(codec.decode(buf)?.map(Decoded::V4)),
        Link::V5(codec) => Ok(codec
            .decode(buf)?
            .map(|packet| Decoded::V5(Box::new(packet)))),
    }
}

fn encode_packet(
    link: &mut Link,
    packet: &BrokerPacket,
    dst: &mut BytesMut,
) -> Result<(), MqttError> {
    match link {
        Link::V4(codec) => {
            if let Some(packet) = v4_packet(packet) {
                codec.encode(packet, dst)?;
            }
        }
        Link::V5(codec) => {
            if let Some(packet) = v5_packet(packet) {
                codec.encode(packet, dst)?;
            }
        }
    }
    Ok(())
}

fn v4_packet(packet: &BrokerPacket) -> Option<v4::Packet> {
    Some(match packet {
        BrokerPacket::ConnAck {
            session_present, ..
        } => v4::Packet::ConnAck(v4::ConnAckPacket::accepted(*session_present)),
        BrokerPacket::ConnAckRefused(code) => {
            v4::Packet::ConnAck(v4::ConnAckPacket::new(v4_connect_code(*code), false))
        }
        BrokerPacket::SubAck { packet_id, acks } => v4::Packet::SubAck(v4::SubAckPacket::new(
            *packet_id,
            acks.iter().copied().map(v4_suback).collect(),
        )),
        BrokerPacket::UnsubAck { packet_id, .. } => v4::Packet::UnsubAck(v4::UnsubAckPacket {
            packet_id: *packet_id,
        }),
        BrokerPacket::PubAck { packet_id } => v4::Packet::PubAck(v4::PubAckPacket {
            packet_id: *packet_id,
        }),
        BrokerPacket::PubRec { packet_id } => v4::Packet::PubRec(v4::PubRecPacket {
            packet_id: *packet_id,
        }),
        BrokerPacket::PubComp { packet_id } => v4::Packet::PubComp(v4::PubCompPacket {
            packet_id: *packet_id,
        }),
        BrokerPacket::PingResp => v4::Packet::PingResp(v4::PingRespPacket),
        BrokerPacket::Outbound(outbound) => return v4_outbound(outbound),
        BrokerPacket::Disconnect(_) => return None,
    })
}

fn v4_outbound(outbound: &Outbound) -> Option<v4::Packet> {
    Some(match outbound {
        Outbound::Publish {
            topic,
            payload,
            qos,
            packet_id,
            dup,
            retain,
        } => v4::Packet::Publish(v4::PublishPacket {
            topic_name: topic.clone(),
            packet_id: *packet_id,
            payload: payload.clone(),
            qos: v4_qos(*qos),
            duplicate: *dup,
            retain: *retain,
        }),
        Outbound::PubRel { packet_id } => v4::Packet::PubRel(v4::PubRelPacket {
            packet_id: *packet_id,
        }),
    })
}

fn v5_packet(packet: &BrokerPacket) -> Option<v5::Packet> {
    Some(match packet {
        BrokerPacket::ConnAck {
            session_present,
            server_assigned_id,
            receive_maximum,
            max_packet_size,
        } => {
            let mut properties = v5::Properties::new();
            if *receive_maximum > 0 {
                properties.receive_maximum = Some(*receive_maximum);
            }
            if *max_packet_size > 0 {
                properties.maximum_packet_size = Some(*max_packet_size);
            }
            properties.topic_alias_maximum = Some(0);
            properties.shared_subscription_available = Some(false);
            properties.assigned_client_identifier = server_assigned_id.clone();
            v5::Packet::ConnAck(v5::ConnAckPacket {
                session_present: *session_present,
                reason_code: ReasonCode::Success,
                properties,
            })
        }
        BrokerPacket::ConnAckRefused(code) => v5::Packet::ConnAck(v5::ConnAckPacket {
            session_present: false,
            reason_code: v5_connack_reason(*code),
            properties: v5::Properties::new(),
        }),
        BrokerPacket::SubAck { packet_id, acks } => v5::Packet::SubAck(v5::SubAckPacket {
            packet_id: *packet_id,
            properties: v5::Properties::new(),
            reason_codes: acks.iter().copied().map(v5_suback).collect(),
        }),
        BrokerPacket::UnsubAck { packet_id, count } => v5::Packet::UnsubAck(v5::UnsubAckPacket {
            packet_id: *packet_id,
            properties: v5::Properties::new(),
            reason_codes: vec![ReasonCode::Success; *count],
        }),
        BrokerPacket::PubAck { packet_id } => v5::Packet::PubAck(v5::PubAckPacket {
            packet_id: *packet_id,
            reason_code: ReasonCode::Success,
            properties: v5::Properties::new(),
        }),
        BrokerPacket::PubRec { packet_id } => v5::Packet::PubRec(v5::PubRecPacket {
            packet_id: *packet_id,
            reason_code: ReasonCode::Success,
            properties: v5::Properties::new(),
        }),
        BrokerPacket::PubComp { packet_id } => v5::Packet::PubComp(v5::PubCompPacket {
            packet_id: *packet_id,
            reason_code: ReasonCode::Success,
            properties: v5::Properties::new(),
        }),
        BrokerPacket::PingResp => v5::Packet::PingResp(v5::PingRespPacket),
        BrokerPacket::Outbound(outbound) => return v5_outbound(outbound),
        BrokerPacket::Disconnect(code) => v5::Packet::Disconnect(v5::DisconnectPacket {
            reason_code: v5_reason(*code),
            properties: v5::Properties::new(),
        }),
    })
}

fn v5_outbound(outbound: &Outbound) -> Option<v5::Packet> {
    Some(match outbound {
        Outbound::Publish {
            topic,
            payload,
            qos,
            packet_id,
            dup,
            retain,
        } => v5::Packet::Publish(v5::PublishPacket {
            topic_name: topic.clone(),
            packet_id: *packet_id,
            payload: payload.clone(),
            qos: v5_qos(*qos),
            duplicate: *dup,
            retain: *retain,
            properties: v5::Properties::new(),
        }),
        Outbound::PubRel { packet_id } => v5::Packet::PubRel(v5::PubRelPacket {
            packet_id: *packet_id,
            reason_code: ReasonCode::Success,
            properties: v5::Properties::new(),
        }),
    })
}

async fn write_events<S: MqttIo>(
    io: &mut S,
    link: &mut Link,
    events: &[PollEvent],
) -> std::io::Result<bool> {
    let mut packets = Vec::new();
    for event in events {
        match event {
            PollEvent::Outbound(outbound) => packets.push(BrokerPacket::Outbound(outbound.clone())),
            PollEvent::Disconnect { reason } => {
                if link.is_v5()
                    && let Some(code) = reason.filter(|code| *code != 0)
                {
                    packets.push(BrokerPacket::Disconnect(code));
                }
                write_packets(io, link, &packets).await?;
                return Ok(true);
            }
        }
    }
    write_packets(io, link, &packets).await?;
    Ok(false)
}

async fn write_packets<S: MqttIo>(
    io: &mut S,
    link: &mut Link,
    packets: &[BrokerPacket],
) -> std::io::Result<()> {
    if packets.is_empty() {
        return Ok(());
    }
    let mut dst = BytesMut::new();
    for packet in packets {
        encode_packet(link, packet, &mut dst).map_err(std::io::Error::other)?;
    }
    if !dst.is_empty() {
        io.write_all(&dst).await?;
    }
    Ok(())
}

async fn close_protocol<S: MqttIo>(io: &mut S, link: &mut Link, code: u8) -> std::io::Result<()> {
    if code != 0 {
        write_packets(io, link, &[BrokerPacket::Disconnect(code)]).await?;
    }
    Ok(())
}

async fn read_protocol<S: MqttIo>(
    io: &mut S,
    buf: &mut BytesMut,
    max_packet_size: u32,
) -> std::io::Result<Option<u8>> {
    loop {
        match peek_connect(buf, max_packet_size) {
            Peek::NeedMore => {}
            Peek::Level(level) => return Ok(Some(level)),
            Peek::Unacceptable => {
                io.write_all(&[0x20, 0x02, 0x00, 0x01]).await?;
                return Ok(None);
            }
            Peek::Fatal => return Ok(None),
        }
        let n = io.read(buf).await?;
        if n == 0 {
            return Ok(None);
        }
    }
}

enum Peek {
    NeedMore,
    Level(u8),
    Unacceptable,
    Fatal,
}

fn peek_connect(buf: &[u8], max_packet_size: u32) -> Peek {
    if buf.is_empty() {
        return Peek::NeedMore;
    }
    if buf[0] >> 4 != 1 {
        return Peek::Fatal;
    }
    let Some((remaining, length_bytes)) = remaining_length(&buf[1..]) else {
        return Peek::NeedMore;
    };
    if remaining > usize::try_from(max_packet_size).unwrap_or(usize::MAX) {
        return Peek::Fatal;
    }
    let header = 1 + length_bytes;
    if buf.len() < header + remaining {
        return Peek::NeedMore;
    }
    let variable = &buf[header..header + remaining];
    if variable.len() < 2 {
        return Peek::Fatal;
    }
    let name_len = usize::from(u16::from_be_bytes([variable[0], variable[1]]));
    if variable.len() < 2 + name_len + 1 {
        return Peek::Fatal;
    }
    let name = &variable[2..2 + name_len];
    let level = variable[2 + name_len];
    if (name == b"MQTT" && (level == 4 || level == 5)) || (name == b"MQIsdp" && level == 4) {
        return Peek::Level(level);
    }
    if name == b"MQTT" || name == b"MQIsdp" {
        return Peek::Unacceptable;
    }
    Peek::Fatal
}

fn remaining_length(input: &[u8]) -> Option<(usize, usize)> {
    let mut value = 0usize;
    let mut multiplier = 1usize;
    for (index, byte) in input.iter().copied().enumerate().take(4) {
        value = value.saturating_add(usize::from(byte & 0x7F).saturating_mul(multiplier));
        if byte & 0x80 == 0 {
            return Some((value, index + 1));
        }
        multiplier = multiplier.saturating_mul(128);
    }
    if input.len() < 4 { None } else { Some((0, 4)) }
}

fn keepalive_deadline(keep_alive: u16) -> Option<Instant> {
    if keep_alive == 0 {
        None
    } else {
        Some(Instant::now() + Duration::from_millis(u64::from(keep_alive).saturating_mul(1500)))
    }
}

fn core_qos(value: u8) -> QoS {
    QoS::try_from(value).unwrap_or(QoS::AtMostOnce)
}

fn v4_qos(qos: QoS) -> v4::QoS {
    match qos {
        QoS::AtMostOnce => v4::QoS::AtMostOnce,
        QoS::AtLeastOnce => v4::QoS::AtLeastOnce,
        QoS::ExactlyOnce => v4::QoS::ExactlyOnce,
    }
}

fn v5_qos(qos: QoS) -> v5::QoS {
    match qos {
        QoS::AtMostOnce => v5::QoS::AtMostOnce,
        QoS::AtLeastOnce => v5::QoS::AtLeastOnce,
        QoS::ExactlyOnce => v5::QoS::ExactlyOnce,
    }
}

fn v4_connect_code(code: u8) -> ConnectReturnCode {
    match code {
        0x02 => ConnectReturnCode::IdentifierRejected,
        0x04 => ConnectReturnCode::BadUserNameOrPassword,
        0x05 | 0x8C => ConnectReturnCode::NotAuthorized,
        0x03 => ConnectReturnCode::ServerUnavailable,
        _ => ConnectReturnCode::UnacceptableProtocolVersion,
    }
}

fn v4_suback(ack: SubscribeAck) -> SubAckReturnCode {
    match ack {
        SubscribeAck::Granted(QoS::AtMostOnce) => SubAckReturnCode::SuccessQoS0,
        SubscribeAck::Granted(QoS::AtLeastOnce) => SubAckReturnCode::SuccessQoS1,
        SubscribeAck::Granted(QoS::ExactlyOnce) => SubAckReturnCode::SuccessQoS2,
        SubscribeAck::Failed(_) => SubAckReturnCode::Failure,
    }
}

fn v5_suback(ack: SubscribeAck) -> ReasonCode {
    match ack {
        SubscribeAck::Granted(QoS::AtMostOnce) => ReasonCode::Success,
        SubscribeAck::Granted(QoS::AtLeastOnce) => ReasonCode::GrantedQoS1,
        SubscribeAck::Granted(QoS::ExactlyOnce) => ReasonCode::GrantedQoS2,
        SubscribeAck::Failed(SubscribeFailure::Unspecified) => ReasonCode::UnspecifiedError,
        SubscribeAck::Failed(SubscribeFailure::SharedSubscriptionsNotSupported) => {
            ReasonCode::SharedSubscriptionNotSupported
        }
    }
}

fn v5_reason(code: u8) -> ReasonCode {
    ReasonCode::try_from(code).unwrap_or(ReasonCode::UnspecifiedError)
}

/// Map a connect refusal onto a v5 CONNACK reason code.
///
/// `refusal_code` uses MQTT 3.1.1 connect return codes. `0x02` is Identifier
/// Rejected there, and Granted QoS 2 in v5.
fn v5_connack_reason(code: u8) -> ReasonCode {
    match code {
        0x02 => ReasonCode::ClientIdentifierNotValid,
        other => v5_reason(other),
    }
}

/// TCP byte stream.
pub struct TcpStreamIo<S> {
    stream: S,
}

impl<S> TcpStreamIo<S> {
    pub fn new(stream: S) -> Self {
        Self { stream }
    }
}

impl<S> MqttIo for TcpStreamIo<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    async fn read(&mut self, buf: &mut BytesMut) -> std::io::Result<usize> {
        self.stream.read_buf(buf).await
    }

    async fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.stream.write_all(bytes).await
    }
}

/// WebSocket byte stream. Each write is one binary frame.
pub struct WebSocketIo<S> {
    socket: S,
}

impl<S> WebSocketIo<S> {
    pub fn new(socket: S) -> Self {
        Self { socket }
    }
}

impl<S> MqttIo for WebSocketIo<S>
where
    S: futures_util::Stream<
            Item = Result<
                tokio_tungstenite::tungstenite::Message,
                tokio_tungstenite::tungstenite::Error,
            >,
        > + futures_util::Sink<
            tokio_tungstenite::tungstenite::Message,
            Error = tokio_tungstenite::tungstenite::Error,
        > + Unpin
        + Send,
{
    async fn read(&mut self, buf: &mut BytesMut) -> std::io::Result<usize> {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message;
        loop {
            let Some(message) = self.socket.next().await else {
                return Ok(0);
            };
            let message = message.map_err(std::io::Error::other)?;
            match message {
                Message::Binary(data) => {
                    let len = data.len();
                    buf.extend_from_slice(&data);
                    return Ok(len);
                }
                Message::Ping(data) => {
                    self.socket
                        .send(Message::Pong(data))
                        .await
                        .map_err(std::io::Error::other)?;
                }
                Message::Close(_) => return Ok(0),
                Message::Text(_) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "MQTT over WebSocket requires binary frames",
                    ));
                }
                Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    }

    async fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        use futures_util::SinkExt;
        use tokio_tungstenite::tungstenite::Message;
        self.socket
            .send(Message::Binary(bytes.to_vec().into()))
            .await
            .map_err(std::io::Error::other)
    }
}
