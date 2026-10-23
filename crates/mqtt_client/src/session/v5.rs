//! MQTT v5.0 session.

use std::time::Instant;

use bytes::{Bytes, BytesMut};
use mqtt_codec::v5::{
    ConnectPacketBuilder, MqttCodec, Packet, PingReqPacket, Properties, PubAckPacket,
    PubCompPacket, PubRecPacket, PubRelPacket, PublishPacketBuilder, QoS, ReasonCode,
    SubscribePacketBuilder, UnsubscribePacketBuilder,
};
use mqtt_codec::{Decoder, Encoder};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time;

use super::state::{PendingOp, SessionState};
use super::{
    ConnAckInfo, Flow, SessionOutcome, arm_pingresp_deadline, client_id, keep_alive_grace,
    keep_alive_secs, reset_deadline,
};
use crate::client::Command;
use crate::config::ClientConfig;
use crate::error::ClientError;
use crate::event::Event;
use crate::inflight::{InflightPublish, InflightStage, PendingPublish};
use crate::transport::BrokerStream;

pub(super) async fn run(
    stream: BrokerStream,
    config: &ClientConfig,
    command_rx: &mut mpsc::Receiver<Command>,
    event_tx: &mpsc::Sender<Event>,
    state: &mut SessionState,
) -> Result<SessionOutcome, ClientError> {
    let mut session = Session::new(stream, state);

    let info = match time::timeout(config.connect_timeout, session.handshake(config)).await {
        Ok(Ok(info)) => info,
        Ok(Err(e)) => return Ok(SessionOutcome::ConnectionLost(Some(e.to_string()))),
        Err(_) => {
            return Ok(SessionOutcome::ConnectionLost(Some(format!(
                "CONNACK not received within {:?}",
                config.connect_timeout
            ))));
        }
    };

    let _ = event_tx
        .send(Event::Connected {
            session_present: info.session_present,
        })
        .await;

    if config.clean_session || !info.session_present {
        session.resubscribe_all().await?;
    }
    session.retransmit_inflight().await?;

    while let Some(cmd) = session.state.pending_commands.pop_front() {
        if handle_command(&mut session, cmd).await? {
            return Ok(SessionOutcome::GracefulDisconnect);
        }
    }

    let ping_every = keep_alive_secs(info.keep_alive);
    let grace = keep_alive_grace(info.keep_alive);
    let mut ping_deadline = None;
    // Armed only after a PINGREQ goes out unanswered.
    let mut pingresp_deadline = None;
    reset_deadline(&mut ping_deadline, ping_every);
    let mut retry_deadline = time::Instant::now() + config.ack_timeout;

    loop {
        let ping_sleep = ping_deadline.map(time::sleep_until);
        let pingresp_sleep = pingresp_deadline.map(time::sleep_until);
        let retry_sleep = time::sleep_until(retry_deadline);
        let has_inflight = !session.state.inflight.is_empty();

        tokio::select! {
            cmd = command_rx.recv() => {
                match cmd {
                    Some(cmd) => {
                        if handle_command(&mut session, cmd).await? {
                            return Ok(SessionOutcome::GracefulDisconnect);
                        }
                        reset_deadline(&mut ping_deadline, ping_every);
                    }
                    None => return Ok(SessionOutcome::GracefulDisconnect),
                }
            }
            read_result = session.read_packet() => {
                match read_result {
                    Ok(None) => {
                        return Ok(SessionOutcome::ConnectionLost(Some(
                            "connection closed".into(),
                        )));
                    }
                    Ok(Some(packet)) => {
                        match handle_packet(&mut session, event_tx, packet).await {
                            Ok(Flow::Continue) => {}
                            Ok(Flow::Close(reason)) => return Ok(SessionOutcome::ConnectionLost(reason)),
                            Err(e) => return Ok(SessionOutcome::ConnectionLost(Some(e.to_string()))),
                        }
                        reset_deadline(&mut ping_deadline, ping_every);
                        pingresp_deadline = None;
                    }
                    Err(e) => {
                        return Ok(SessionOutcome::ConnectionLost(Some(e.to_string())));
                    }
                }
            }
            _ = async {
                match ping_sleep {
                    Some(sleep) => sleep.await,
                    None => std::future::pending::<()>().await,
                }
            },
            if ping_deadline.is_some() => {
                if let Err(e) = session.ping().await {
                    return Ok(SessionOutcome::ConnectionLost(Some(e.to_string())));
                }
                arm_pingresp_deadline(&mut pingresp_deadline, grace);
                reset_deadline(&mut ping_deadline, ping_every);
            }
            _ = async {
                match pingresp_sleep {
                    Some(sleep) => sleep.await,
                    None => std::future::pending::<()>().await,
                }
            },
            if pingresp_deadline.is_some() => {
                return Ok(SessionOutcome::ConnectionLost(Some(
                    "keep-alive timeout: no PINGRESP from broker".into(),
                )));
            }
            () = retry_sleep, if has_inflight => {
                if let Err(e) = session.retry_due(config.ack_timeout).await {
                    return Ok(SessionOutcome::ConnectionLost(Some(e.to_string())));
                }
                retry_deadline = time::Instant::now() + config.ack_timeout;
            }
        }
    }
}

async fn handle_command(session: &mut Session<'_>, cmd: Command) -> Result<bool, ClientError> {
    match cmd {
        Command::Publish {
            topic,
            payload,
            qos,
            retain,
            reply,
        } => {
            session.publish(topic, payload, qos, retain, reply).await?;
            Ok(false)
        }
        Command::Subscribe { topics, reply } => {
            session.subscribe(topics, reply).await?;
            Ok(false)
        }
        Command::Unsubscribe { topics, reply } => {
            session.unsubscribe(topics, reply).await?;
            Ok(false)
        }
        Command::Disconnect => {
            let _ = session.send_disconnect().await;
            Ok(true)
        }
    }
}

struct Session<'a> {
    stream: BrokerStream,
    codec: MqttCodec,
    read_buf: BytesMut,
    write_buf: BytesMut,
    state: &'a mut SessionState,
}

impl<'a> Session<'a> {
    fn new(stream: BrokerStream, state: &'a mut SessionState) -> Self {
        Self {
            stream,
            codec: MqttCodec::new(),
            read_buf: BytesMut::with_capacity(4096),
            write_buf: BytesMut::with_capacity(4096),
            state,
        }
    }

    async fn handshake(&mut self, config: &ClientConfig) -> Result<ConnAckInfo, ClientError> {
        let connect = build_connect(config, self.state)?;
        self.write_packet(Packet::Connect(connect)).await?;

        let packet = self
            .read_packet()
            .await?
            .ok_or(ClientError::UnexpectedPacket(
                "connection closed before CONNACK".into(),
            ))?;

        match packet {
            Packet::ConnAck(ack) if ack.reason_code == ReasonCode::Success => {
                if let Some(max) = ack.properties.maximum_packet_size {
                    self.codec.set_max_packet_size(max);
                }
                // The broker may cap our inflight window (Receive Maximum).
                if let Some(receive_maximum) = ack.properties.receive_maximum {
                    let limit = receive_maximum.min(config.max_inflight);
                    self.state.inflight.set_max(limit);
                }
                // Remember a broker-assigned id for subsequent connections.
                self.state.assigned_client_id = ack.properties.assigned_client_identifier.clone();

                Ok(ConnAckInfo {
                    session_present: ack.session_present,
                    keep_alive: ack
                        .properties
                        .server_keep_alive
                        .unwrap_or(config.keep_alive),
                })
            }
            Packet::ConnAck(ack) => Err(ClientError::ConnectionRefused {
                reason: format!("CONNACK reason 0x{:02X}", ack.reason_code.as_u8()),
            }),
            other => Err(ClientError::UnexpectedPacket(format!(
                "expected CONNACK, got {:?}",
                other.packet_type()
            ))),
        }
    }

    async fn publish(
        &mut self,
        topic: String,
        payload: Bytes,
        qos: u8,
        retain: bool,
        reply: crate::client::AckReply,
    ) -> Result<(), ClientError> {
        let qos_enum = parse_qos(qos)?;
        if qos_enum == QoS::AtMostOnce {
            let packet = PublishPacketBuilder::new(&topic, payload)
                .qos(qos_enum)
                .retain(retain)
                .build();
            self.write_packet(Packet::Publish(packet)).await?;
            let _ = reply.send(Ok(()));
            return Ok(());
        }

        if !self.state.inflight.has_capacity() {
            self.state.inflight.push_pending(PendingPublish {
                topic,
                payload,
                qos,
                retain,
                reply,
            });
            return Ok(());
        }

        self.send_qos_publish(topic, payload, qos, retain, false, reply)
            .await
    }

    async fn send_qos_publish(
        &mut self,
        topic: String,
        payload: Bytes,
        qos: u8,
        retain: bool,
        duplicate: bool,
        reply: crate::client::AckReply,
    ) -> Result<(), ClientError> {
        let qos_enum = parse_qos(qos)?;
        let packet_id = self.state.alloc_packet_id();
        let stage = match qos_enum {
            QoS::AtLeastOnce => InflightStage::Ack,
            QoS::ExactlyOnce => InflightStage::Rec,
            QoS::AtMostOnce => InflightStage::Ack,
        };

        let packet = PublishPacketBuilder::new(&topic, payload.clone())
            .qos(qos_enum)
            .retain(retain)
            .packet_id(packet_id)
            .duplicate(duplicate)
            .build();
        self.write_packet(Packet::Publish(packet)).await?;

        self.state.inflight.insert(
            packet_id,
            InflightPublish {
                topic,
                payload,
                qos,
                retain,
                stage,
                sent_at: Instant::now(),
            },
        );
        self.state.publish_replies.insert(packet_id, reply);
        Ok(())
    }

    async fn flush_pending(&mut self) -> Result<(), ClientError> {
        while let Some(pending) = self.state.inflight.pop_pending() {
            self.send_qos_publish(
                pending.topic,
                pending.payload,
                pending.qos,
                pending.retain,
                false,
                pending.reply,
            )
            .await?;
        }
        Ok(())
    }

    async fn subscribe(
        &mut self,
        topics: Vec<(String, u8)>,
        reply: crate::client::CodesReply,
    ) -> Result<(), ClientError> {
        if topics.is_empty() {
            let _ = reply.send(Ok(Vec::new()));
            return Ok(());
        }
        for (filter, qos) in &topics {
            self.state.remember_subscription(filter.clone(), *qos);
        }
        let filters: Vec<String> = topics.iter().map(|(f, _)| f.clone()).collect();
        let packet_id = self.send_subscribe(topics).await?;
        self.state
            .pending_ops
            .insert(packet_id, PendingOp::Subscribe { filters, reply });
        Ok(())
    }

    /// Send SUBSCRIBE, returning the packet id used.
    async fn send_subscribe(&mut self, topics: Vec<(String, u8)>) -> Result<u16, ClientError> {
        let packet_id = self.state.alloc_packet_id();
        let mut builder = SubscribePacketBuilder::new(packet_id);
        for (filter, qos) in topics {
            builder = builder.topic(filter, parse_qos(qos)?);
        }
        let subscribe = builder
            .build()
            .ok_or(ClientError::UnexpectedPacket("empty SUBSCRIBE".into()))?;
        self.write_packet(Packet::Subscribe(subscribe)).await?;
        Ok(packet_id)
    }

    async fn resubscribe_all(&mut self) -> Result<(), ClientError> {
        if self.state.subscriptions.is_empty() {
            return Ok(());
        }
        let topics: Vec<(String, u8)> = self
            .state
            .subscriptions
            .iter()
            .map(|(f, q)| (f.clone(), *q))
            .collect();
        self.send_subscribe(topics).await.map(|_| ())
    }

    async fn unsubscribe(
        &mut self,
        topics: Vec<String>,
        reply: crate::client::AckReply,
    ) -> Result<(), ClientError> {
        if topics.is_empty() {
            let _ = reply.send(Ok(()));
            return Ok(());
        }

        let removed: Vec<(String, u8)> = topics
            .iter()
            .filter_map(|filter| {
                self.state
                    .subscriptions
                    .get(filter)
                    .map(|qos| (filter.clone(), *qos))
            })
            .collect();
        self.state.forget_subscriptions(&topics);

        let packet_id = self.state.alloc_packet_id();
        let mut builder = UnsubscribePacketBuilder::new(packet_id);
        for topic in topics {
            builder = builder.topic(topic);
        }
        let unsubscribe = builder
            .build()
            .ok_or(ClientError::UnexpectedPacket("empty UNSUBSCRIBE".into()))?;
        self.write_packet(Packet::Unsubscribe(unsubscribe)).await?;

        self.state
            .pending_ops
            .insert(packet_id, PendingOp::Unsubscribe { removed, reply });
        Ok(())
    }

    async fn retransmit_inflight(&mut self) -> Result<(), ClientError> {
        let snapshot = self.state.inflight.snapshot();
        for (packet_id, entry) in snapshot {
            match entry.stage {
                InflightStage::Ack | InflightStage::Rec => {
                    let qos_enum = parse_qos(entry.qos)?;
                    let packet = PublishPacketBuilder::new(&entry.topic, entry.payload.clone())
                        .qos(qos_enum)
                        .retain(entry.retain)
                        .packet_id(packet_id)
                        .duplicate(true)
                        .build();
                    self.write_packet(Packet::Publish(packet)).await?;
                    if let Some(slot) = self.state.inflight.get_mut(packet_id) {
                        slot.sent_at = Instant::now();
                    }
                }
                InflightStage::Comp => {
                    self.write_packet(Packet::PubRel(PubRelPacket {
                        packet_id,
                        reason_code: ReasonCode::Success,
                        properties: Properties::new(),
                    }))
                    .await?;
                    if let Some(slot) = self.state.inflight.get_mut(packet_id) {
                        slot.sent_at = Instant::now();
                    }
                }
            }
        }
        Ok(())
    }

    async fn retry_due(&mut self, timeout: std::time::Duration) -> Result<(), ClientError> {
        let due = self.state.inflight.due_for_retry(timeout);
        for packet_id in due {
            let Some(entry) = self.state.inflight.get_mut(packet_id) else {
                continue;
            };
            let stage = entry.stage;
            let topic = entry.topic.clone();
            let payload = entry.payload.clone();
            let qos = entry.qos;
            let retain = entry.retain;
            entry.sent_at = Instant::now();

            match stage {
                InflightStage::Ack | InflightStage::Rec => {
                    let qos_enum = parse_qos(qos)?;
                    let packet = PublishPacketBuilder::new(&topic, payload)
                        .qos(qos_enum)
                        .retain(retain)
                        .packet_id(packet_id)
                        .duplicate(true)
                        .build();
                    self.write_packet(Packet::Publish(packet)).await?;
                }
                InflightStage::Comp => {
                    self.write_packet(Packet::PubRel(PubRelPacket {
                        packet_id,
                        reason_code: ReasonCode::Success,
                        properties: Properties::new(),
                    }))
                    .await?;
                }
            }
        }
        Ok(())
    }

    async fn ping(&mut self) -> Result<(), ClientError> {
        self.write_packet(Packet::PingReq(PingReqPacket)).await
    }

    async fn send_disconnect(&mut self) -> Result<(), ClientError> {
        self.write_packet(Packet::Disconnect(Default::default()))
            .await
    }

    async fn write_packet(&mut self, packet: Packet) -> Result<(), ClientError> {
        self.write_buf.clear();
        self.codec.encode(packet, &mut self.write_buf)?;
        self.stream
            .write_all(&self.write_buf)
            .await
            .map_err(ClientError::Io)?;
        self.stream.flush().await.map_err(ClientError::Io)
    }

    async fn read_packet(&mut self) -> Result<Option<Packet>, ClientError> {
        loop {
            if let Some(packet) = self.codec.decode(&mut self.read_buf)? {
                return Ok(Some(packet));
            }
            let n = self
                .stream
                .read_buf(&mut self.read_buf)
                .await
                .map_err(ClientError::Io)?;
            if n == 0 {
                return Ok(None);
            }
        }
    }
}

async fn handle_packet(
    session: &mut Session<'_>,
    event_tx: &mpsc::Sender<Event>,
    packet: Packet,
) -> Result<Flow, ClientError> {
    match packet {
        Packet::Publish(publish) => {
            let qos = u8::from(publish.qos);
            let retain = publish.retain;
            let topic = publish.topic_name;
            let payload = publish.payload;

            match publish.qos {
                QoS::AtMostOnce => {
                    let _ = event_tx
                        .send(Event::Message {
                            topic,
                            payload,
                            qos,
                            retain,
                        })
                        .await;
                }
                QoS::AtLeastOnce => {
                    if let Some(packet_id) = publish.packet_id {
                        session
                            .write_packet(Packet::PubAck(PubAckPacket {
                                packet_id,
                                reason_code: ReasonCode::Success,
                                properties: Properties::new(),
                            }))
                            .await?;
                    }
                    let _ = event_tx
                        .send(Event::Message {
                            topic,
                            payload,
                            qos,
                            retain,
                        })
                        .await;
                }
                QoS::ExactlyOnce => {
                    let Some(packet_id) = publish.packet_id else {
                        return Err(ClientError::UnexpectedPacket(
                            "QoS 2 PUBLISH missing packet id".into(),
                        ));
                    };
                    session
                        .state
                        .qos2_incoming
                        .insert(packet_id, (topic, payload, retain));
                    session
                        .write_packet(Packet::PubRec(PubRecPacket {
                            packet_id,
                            reason_code: ReasonCode::Success,
                            properties: Properties::new(),
                        }))
                        .await?;
                }
            }
        }
        Packet::PubAck(ack) => {
            if session.state.inflight.remove(ack.packet_id).is_some() {
                session.state.complete_publish(ack.packet_id, Ok(()));
                let _ = event_tx
                    .send(Event::PubAck {
                        packet_id: ack.packet_id,
                    })
                    .await;
                session.flush_pending().await?;
            }
        }
        Packet::PubRec(rec) => {
            if let Some(entry) = session.state.inflight.get_mut(rec.packet_id) {
                entry.stage = InflightStage::Comp;
                entry.sent_at = Instant::now();
            }
            session
                .write_packet(Packet::PubRel(PubRelPacket {
                    packet_id: rec.packet_id,
                    reason_code: ReasonCode::Success,
                    properties: Properties::new(),
                }))
                .await?;
        }
        Packet::PubRel(rel) => {
            session
                .write_packet(Packet::PubComp(PubCompPacket {
                    packet_id: rel.packet_id,
                    reason_code: ReasonCode::Success,
                    properties: Properties::new(),
                }))
                .await?;
            if let Some((topic, payload, retain)) =
                session.state.qos2_incoming.remove(&rel.packet_id)
            {
                let _ = event_tx
                    .send(Event::Message {
                        topic,
                        payload,
                        qos: 2,
                        retain,
                    })
                    .await;
            }
        }
        Packet::PubComp(comp) => {
            if session.state.inflight.remove(comp.packet_id).is_some() {
                session.state.complete_publish(comp.packet_id, Ok(()));
                let _ = event_tx
                    .send(Event::PubComp {
                        packet_id: comp.packet_id,
                    })
                    .await;
                session.flush_pending().await?;
            }
        }
        Packet::SubAck(ack) => {
            let codes: Vec<u8> = ack.reason_codes.iter().map(|c| c.as_u8()).collect();
            if let Some(PendingOp::Subscribe { filters, reply }) =
                session.state.pending_ops.remove(&ack.packet_id)
            {
                let rejected: Vec<String> = filters
                    .iter()
                    .zip(codes.iter())
                    .filter(|(_, code)| **code >= REASON_FAILURE)
                    .map(|(filter, _)| filter.clone())
                    .collect();
                session.state.forget_subscriptions(&rejected);

                let result = if rejected.is_empty() {
                    Ok(codes.clone())
                } else {
                    Err(ClientError::SubscribeRejected {
                        codes: codes.clone(),
                    })
                };
                let _ = reply.send(result);
            }
            let _ = event_tx
                .send(Event::SubAck {
                    packet_id: ack.packet_id,
                    return_codes: codes,
                })
                .await;
        }
        Packet::UnsubAck(ack) => {
            let codes: Vec<u8> = ack.reason_codes.iter().map(|c| c.as_u8()).collect();
            if let Some(PendingOp::Unsubscribe { removed, reply }) =
                session.state.pending_ops.remove(&ack.packet_id)
            {
                let failed = codes.iter().any(|code| *code >= REASON_FAILURE);
                if failed {
                    // The broker refused the request: keep the subscriptions.
                    session.state.restore_subscriptions(removed);
                    let _ = reply.send(Err(ClientError::UnsubscribeRejected {
                        codes: codes.clone(),
                    }));
                } else {
                    let _ = reply.send(Ok(()));
                }
            }
            let _ = event_tx
                .send(Event::UnsubAck {
                    packet_id: ack.packet_id,
                })
                .await;
        }
        Packet::PingResp(_) | Packet::Auth(_) => {}
        Packet::Disconnect(disconnect) => {
            let reason = if disconnect.reason_code == ReasonCode::Success {
                "broker sent DISCONNECT".to_string()
            } else {
                format!(
                    "broker sent DISCONNECT (reason 0x{:02X})",
                    disconnect.reason_code.as_u8()
                )
            };
            return Ok(Flow::Close(Some(reason)));
        }
        other => {
            return Err(ClientError::UnexpectedPacket(format!(
                "unexpected packet from broker: {:?}",
                other.packet_type()
            )));
        }
    }
    Ok(Flow::Continue)
}

/// Reason codes at or above this value signal failure (MQTT v5.0 §2.4).
const REASON_FAILURE: u8 = 0x80;

fn build_connect(
    config: &ClientConfig,
    state: &SessionState,
) -> Result<mqtt_codec::v5::ConnectPacket, ClientError> {
    let mut builder = ConnectPacketBuilder::new(client_id(config, state))
        .keep_alive(config.keep_alive)
        .clean_start(config.clean_session)
        .receive_maximum(config.max_inflight);

    if let Some(creds) = &config.credentials {
        builder = builder.username(&creds.username);
        if let Some(password) = &creds.password {
            builder = builder.password(password.clone());
        }
    }

    if let Some(will) = &config.last_will {
        builder = builder.will(
            &will.topic,
            will.message.clone(),
            parse_qos(will.qos)?,
            will.retain,
        );
    }

    Ok(builder.build())
}

fn parse_qos(qos: u8) -> Result<QoS, ClientError> {
    QoS::try_from(qos).map_err(|_| ClientError::InvalidQoS(qos))
}
