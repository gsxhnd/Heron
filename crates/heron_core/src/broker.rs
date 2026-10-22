//! Session state, topic routing, and QoS 0/1/2 delivery.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::Notify;

use crate::error::{
    ClientFlow, ConnectError, CoreError, PublishError, SubscribeAck, SubscribeFailure,
};
use crate::message::{
    AppMessage, ClientAck, ConnectRequest, ConnectedSession, DisconnectRequest, IncomingPublish,
    Outbound, PollEvent, ProtocolVersion, SubOpts, SubscribeFilter, Will,
};
use crate::qos::QoS;
use crate::storage::{
    Direction, MessageState, Storage, StoredClient, StoredMessage, StoredRetained,
    StoredSubscription, WriteOp, apply_all,
};
use crate::topic::{FilterClass, TopicTree, classify_filter, valid_topic_name};

/// Runtime limits for the in-memory broker.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    /// Maximum QoS 0 plus unacked QoS 1/2 messages queued for one session.
    pub max_queued_messages: usize,
    /// Advertised server receive maximum. Well-behaved clients cap their own window with it.
    pub receive_maximum: u16,
    /// Default maximum packet size advertised to clients, in bytes.
    pub max_packet_size: u32,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            max_queued_messages: 1_000,
            receive_maximum: 32,
            max_packet_size: 1024 * 1024,
        }
    }
}

/// Cloneable handle for one broker process.
#[derive(Clone)]
pub struct Broker {
    inner: Arc<Inner>,
}

struct Inner {
    state: Mutex<State>,
    storage: Arc<dyn Storage>,
    config: BrokerConfig,
}

struct State {
    sessions: HashMap<String, Session>,
    topics: TopicTree,
}

struct Session {
    client_id: String,
    epoch: u64,
    connected: bool,
    protocol: ProtocolVersion,
    clean_start: bool,
    session_expiry: Option<u32>,
    keep_alive: u16,
    receive_maximum: u16,
    max_packet_size: u32,
    will: Option<Will>,
    pending_will: Option<Will>,
    will_epoch: u64,
    subscriptions: HashMap<String, SubOpts>,
    outbound: HashMap<u16, OutRecord>,
    ready: VecDeque<ReadyItem>,
    inflight_sent: u16,
    inbound: HashMap<u16, AppMessage>,
    next_packet_id: u16,
    notify: Arc<Notify>,
    /// `(old_epoch, v5 reason)` so the displaced connection can send DISCONNECT.
    superseded: Option<(u64, u8)>,
    peer: Option<String>,
    disconnected_at_ms: Option<i64>,
}

struct OutRecord {
    message: AppMessage,
    state: OutState,
    /// True after this connection has written the current handshake step.
    on_wire: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum OutState {
    Ready,
    WaitPubAck,
    WaitPubRec,
    WaitPubComp,
}

#[derive(Clone)]
enum ReadyItem {
    QoS0(AppMessage),
    Reliable(u16),
}

impl Broker {
    /// Load durable state and start expiry timers.
    pub async fn open(storage: Arc<dyn Storage>, config: BrokerConfig) -> Result<Self, CoreError> {
        let clients = storage.load_clients().await?;
        let subscriptions = storage.load_subscriptions().await?;
        let retained = storage.load_retained().await?;
        let messages = storage.load_messages().await?;

        let mut state = State {
            sessions: HashMap::new(),
            topics: TopicTree::new(),
        };
        let mut ops = Vec::new();
        let mut expired_clients = std::collections::HashSet::new();

        for message in retained {
            state
                .topics
                .set_retained(&message.topic, message.payload, message.qos);
        }

        for client in clients {
            if let Some(session) = session_from_stored(&client) {
                state.sessions.insert(client.client_id.clone(), session);
            } else {
                expired_clients.insert(client.client_id.clone());
                ops.push(WriteOp::DeleteClient(client.client_id));
            }
        }

        for subscription in subscriptions {
            if expired_clients.contains(&subscription.client_id) {
                continue;
            }
            ensure_shell(&mut state, &subscription.client_id);
            if let Some(session) = state.sessions.get_mut(&subscription.client_id) {
                session
                    .subscriptions
                    .insert(subscription.filter.clone(), sub_opts(&subscription));
            }
            state.topics.subscribe(
                &subscription.client_id,
                &subscription.filter,
                sub_opts(&subscription),
            );
        }

        for message in messages {
            if expired_clients.contains(&message.client_id) {
                continue;
            }
            ensure_shell(&mut state, &message.client_id);
            if let Some(session) = state.sessions.get_mut(&message.client_id) {
                restore_message(session, message);
            }
        }

        let mut immediate_wills = Vec::new();
        let mut timers = Vec::new();
        let crashed: Vec<String> = state
            .sessions
            .values()
            .filter(|session| session.connected)
            .map(|session| session.client_id.clone())
            .collect();

        for client_id in cracked_ids(&crashed) {
            let Some(session) = state.sessions.get_mut(&client_id) else {
                continue;
            };
            session.connected = false;
            session.disconnected_at_ms = Some(now_ms());
            if let Some(will) = session.will.take() {
                if will_delay(&will, session.session_expiry) == 0 {
                    immediate_wills.push((client_id.clone(), will));
                } else {
                    session.will_epoch = session.epoch;
                    let delay = will_delay(&will, session.session_expiry);
                    session.pending_will = Some(will);
                    timers.push(Timer::Will {
                        client_id: client_id.clone(),
                        epoch: session.epoch,
                        delay_secs: delay,
                    });
                }
            }
            if session.session_expiry == Some(0) {
                ops.push(WriteOp::DeleteClient(client_id.clone()));
                timers.retain(|timer| timer.client_id() != client_id);
            } else {
                if let Some(secs) = session.session_expiry {
                    timers.push(Timer::Expiry {
                        client_id: client_id.clone(),
                        epoch: session.epoch,
                        delay_secs: secs,
                    });
                }
                ops.push(WriteOp::UpsertClient(stored_client(session)));
            }
        }

        let expired: Vec<String> = state
            .sessions
            .iter()
            .filter(|(_, session)| session.session_expiry == Some(0) && !session.connected)
            .map(|(id, _)| id.clone())
            .collect();
        for client_id in expired {
            if let Some(session) = state.sessions.get_mut(&client_id)
                && let Some(will) = session.pending_will.take().or_else(|| session.will.take())
            {
                immediate_wills.push((client_id.clone(), will));
            }
            forget_session(&mut state, &client_id, &mut ops);
        }

        for (client_id, will) in immediate_wills {
            let publish = incoming_from_will(&will);
            route(&mut state, &config, &client_id, &publish, &mut ops);
        }

        // Sessions removed above must leave the map. `forget_session` does that.
        // Crash victims with expiry 0 were only marked in `ops`; remove them now.
        let remove_now: Vec<String> = ops
            .iter()
            .filter_map(|op| match op {
                WriteOp::DeleteClient(id) => Some(id.clone()),
                _ => None,
            })
            .collect();
        for client_id in remove_now {
            if state.sessions.contains_key(&client_id) {
                forget_session(&mut state, &client_id, &mut Vec::new());
            }
        }

        let broker = Self {
            inner: Arc::new(Inner {
                state: Mutex::new(state),
                storage,
                config,
            }),
        };
        broker.persist(ops).await?;
        for timer in timers {
            broker.spawn_timer(timer);
        }
        Ok(broker)
    }

    /// Limits used by listeners when advertising CONNACK properties.
    pub fn config(&self) -> BrokerConfig {
        self.inner.config.clone()
    }

    /// Accept CONNECT and attach a connection to the session.
    pub async fn connect(&self, request: ConnectRequest) -> Result<ConnectedSession, ConnectError> {
        if request.enhanced_auth {
            return Err(ConnectError::BadAuthenticationMethod);
        }
        let client_id = match resolve_client_id(&request) {
            Ok(client_id) => client_id,
            Err(err) => return Err(err),
        };
        let server_assigned_id = request.client_id.is_empty();

        let (connected, ops) = {
            let mut state = self.lock();
            let mut ops = Vec::new();
            let mut immediate_wills = Vec::new();

            if let Some(session) = state.sessions.get_mut(&client_id) {
                if session.connected {
                    let old_epoch = session.epoch;
                    session.superseded = Some((old_epoch, 0x8E));
                    session.connected = false;
                    session.notify.notify_one();
                    if let Some(will) = session.will.take()
                        && will.delay_secs == 0
                    {
                        immediate_wills.push(will);
                    }
                }
                session.pending_will = None;
            }

            let exists = state.sessions.contains_key(&client_id);
            if exists && request.clean_start {
                clear_session_state(&mut state, &client_id, &mut ops);
            }
            let session_present = exists && !request.clean_start;

            let session = if let Some(session) = state.sessions.get_mut(&client_id) {
                prepare_attached(session, &request, &client_id);
                session
            } else {
                state
                    .sessions
                    .insert(client_id.clone(), new_session(&client_id, &request));
                state
                    .sessions
                    .get_mut(&client_id)
                    .ok_or_else(|| ConnectError::ServerUnavailable("session missing".into()))?
            };
            arm_delivery(session);
            let connected = ConnectedSession {
                client_id: client_id.clone(),
                epoch: session.epoch,
                session_present,
                server_assigned_id,
                keep_alive: session.keep_alive,
                notify: Arc::clone(&session.notify),
            };
            ops.push(WriteOp::UpsertClient(stored_client(session)));

            for will in immediate_wills {
                let publish = incoming_from_will(&will);
                route(
                    &mut state,
                    &self.inner.config,
                    &client_id,
                    &publish,
                    &mut ops,
                );
            }
            (connected, ops)
        };
        self.persist(ops).await.map_err(ConnectError::from)?;
        Ok(connected)
    }

    /// Install subscriptions and queue matching retained messages.
    pub async fn subscribe(
        &self,
        client_id: &str,
        epoch: u64,
        filters: &[SubscribeFilter],
    ) -> Result<Vec<SubscribeAck>, CoreError> {
        let (acks, ops) = {
            let mut state = self.lock();
            let mut ops = Vec::new();
            let mut acks = Vec::with_capacity(filters.len());
            if !owns_session(state.sessions.get(client_id), epoch) {
                return Err(CoreError::SessionNotFound);
            }
            for filter in filters {
                acks.push(subscribe_one(
                    &mut state,
                    &self.inner.config,
                    client_id,
                    filter,
                    &mut ops,
                ));
            }
            (acks, ops)
        };
        self.persist(ops).await?;
        Ok(acks)
    }

    /// Remove subscriptions. Each flag is true when the filter was installed.
    pub async fn unsubscribe(
        &self,
        client_id: &str,
        epoch: u64,
        filters: &[String],
    ) -> Result<Vec<bool>, CoreError> {
        let (removed, ops) = {
            let mut state = self.lock();
            if !owns_session(state.sessions.get(client_id), epoch) {
                return Err(CoreError::SessionNotFound);
            }
            let mut ops = Vec::new();
            let mut removed = Vec::with_capacity(filters.len());
            for filter in filters {
                let existed = state
                    .sessions
                    .get_mut(client_id)
                    .is_some_and(|session| session.subscriptions.remove(filter).is_some());
                state.topics.unsubscribe(client_id, filter);
                if existed {
                    ops.push(WriteOp::DeleteSubscription {
                        client_id: client_id.to_owned(),
                        filter: filter.clone(),
                    });
                }
                removed.push(existed);
            }
            (removed, ops)
        };
        self.persist(ops).await?;
        Ok(removed)
    }

    /// Accept a client PUBLISH. QoS 2 is forwarded later, on PUBREL.
    pub async fn publish(
        &self,
        client_id: &str,
        epoch: u64,
        publish: IncomingPublish,
    ) -> Result<ClientFlow, PublishError> {
        if !valid_topic_name(&publish.topic) {
            return Err(PublishError::InvalidTopic);
        }
        if publish.qos > QoS::AtMostOnce && publish.packet_id.unwrap_or(0) == 0 {
            return Err(PublishError::MissingPacketId);
        }
        let ops = {
            let receive_limit = usize::from(self.inner.config.receive_maximum.max(1));
            let mut state = self.lock();
            let Some(session) = state.sessions.get_mut(client_id) else {
                return Err(PublishError::StaleSession);
            };
            if session.epoch != epoch {
                return Err(PublishError::StaleSession);
            }
            if publish.qos == QoS::ExactlyOnce {
                let packet_id = publish.packet_id.unwrap_or(0);
                if session.inbound.contains_key(&packet_id) {
                    return Ok(ClientFlow::PubRec);
                }
                if session.inbound.len() >= receive_limit {
                    return Err(PublishError::ReceiveMaximumExceeded);
                }
                session.inbound.insert(
                    packet_id,
                    AppMessage {
                        topic: publish.topic.clone(),
                        payload: publish.payload.clone(),
                        qos: publish.qos,
                        retain: publish.retain,
                    },
                );
                let stored = StoredMessage {
                    client_id: client_id.to_owned(),
                    packet_id,
                    topic: publish.topic,
                    payload: publish.payload,
                    qos: publish.qos,
                    retain: publish.retain,
                    direction: Direction::In,
                    state: MessageState::PubRec,
                };
                vec![WriteOp::UpsertMessage(stored)]
            } else if publish.retain && publish.payload.is_empty() {
                state
                    .topics
                    .set_retained(&publish.topic, Bytes::new(), publish.qos);
                vec![WriteOp::DeleteRetained(publish.topic)]
            } else {
                let mut ops = Vec::new();
                route(
                    &mut state,
                    &self.inner.config,
                    client_id,
                    &publish,
                    &mut ops,
                );
                ops
            }
        };
        if let Err(err) = self.persist(ops).await {
            tracing::warn!(error = %err, "持久化入站发布失败");
        }
        Ok(match publish.qos {
            QoS::AtMostOnce => ClientFlow::Accepted,
            QoS::AtLeastOnce => ClientFlow::PubAck,
            QoS::ExactlyOnce => ClientFlow::PubRec,
        })
    }

    /// Finish an inbound QoS 2 publication and forward it.
    pub async fn pubrel(
        &self,
        client_id: &str,
        epoch: u64,
        packet_id: u16,
    ) -> Result<ClientFlow, PublishError> {
        let ops = {
            let mut state = self.lock();
            let Some(session) = state.sessions.get_mut(client_id) else {
                return Err(PublishError::StaleSession);
            };
            if session.epoch != epoch {
                return Err(PublishError::StaleSession);
            }
            let message = session.inbound.remove(&packet_id);
            let mut ops = vec![WriteOp::DeleteMessage {
                client_id: client_id.to_owned(),
                packet_id,
                direction: Direction::In,
            }];
            if let Some(message) = message {
                let publish = IncomingPublish {
                    topic: message.topic,
                    payload: message.payload,
                    qos: message.qos,
                    retain: message.retain,
                    packet_id: Some(packet_id),
                };
                route(
                    &mut state,
                    &self.inner.config,
                    client_id,
                    &publish,
                    &mut ops,
                );
            }
            ops
        };
        let _ = self.persist(ops).await;
        Ok(ClientFlow::PubComp)
    }

    /// Record PUBACK, PUBREC, or PUBCOMP from a subscriber.
    pub async fn acknowledge(
        &self,
        client_id: &str,
        epoch: u64,
        packet_id: u16,
        ack: ClientAck,
    ) -> Result<(), CoreError> {
        let ops =
            {
                let mut state = self.lock();
                let Some(session) = state.sessions.get_mut(client_id) else {
                    return Ok(());
                };
                if session.epoch != epoch {
                    return Ok(());
                }
                let mut ops = Vec::new();
                match ack {
                    ClientAck::PubAck | ClientAck::PubComp => {
                        let expected = if ack == ClientAck::PubAck {
                            OutState::WaitPubAck
                        } else {
                            OutState::WaitPubComp
                        };
                        let matches = session
                            .outbound
                            .get(&packet_id)
                            .is_some_and(|record| record.state == expected);
                        if matches {
                            session.outbound.remove(&packet_id);
                            session.inflight_sent = session.inflight_sent.saturating_sub(1);
                            session.ready.retain(
                                |item| !matches!(item, ReadyItem::Reliable(id) if *id == packet_id),
                            );
                            ops.push(WriteOp::DeleteMessage {
                                client_id: client_id.to_owned(),
                                packet_id,
                                direction: Direction::Out,
                            });
                            session.notify.notify_one();
                        }
                    }
                    ClientAck::PubRec => {
                        if let Some(record) = session.outbound.get_mut(&packet_id)
                            && record.state == OutState::WaitPubRec
                        {
                            record.state = OutState::WaitPubComp;
                            record.on_wire = false;
                            ops.push(WriteOp::UpsertMessage(stored_outbound(
                                client_id, packet_id, record,
                            )));
                            if !session.ready.iter().any(
                                |item| matches!(item, ReadyItem::Reliable(id) if *id == packet_id),
                            ) {
                                session.ready.push_back(ReadyItem::Reliable(packet_id));
                            }
                            session.notify.notify_one();
                        }
                    }
                }
                ops
            };
        self.persist(ops).await
    }

    /// Pull packets the connection should write.
    pub async fn poll(&self, client_id: &str, epoch: u64, limit: usize) -> Vec<PollEvent> {
        let (events, ops) = {
            let mut state = self.lock();
            let Some(session) = state.sessions.get_mut(client_id) else {
                return vec![PollEvent::Disconnect { reason: None }];
            };
            if session.epoch != epoch {
                let reason = session
                    .superseded
                    .and_then(|(old, code)| (old == epoch).then_some(code));
                return vec![PollEvent::Disconnect { reason }];
            }
            let mut events = Vec::new();
            let mut ops = Vec::new();
            while events.len() < limit {
                let Some(position) = next_ready(session) else {
                    break;
                };
                let Some(item) = session.ready.remove(position) else {
                    break;
                };
                match item {
                    ReadyItem::QoS0(message) => {
                        events.push(PollEvent::Outbound(Outbound::Publish {
                            topic: message.topic,
                            payload: message.payload,
                            qos: QoS::AtMostOnce,
                            packet_id: None,
                            dup: false,
                            retain: message.retain,
                        }))
                    }
                    ReadyItem::Reliable(packet_id) => {
                        let Some(record) = session.outbound.get_mut(&packet_id) else {
                            continue;
                        };
                        if record.state == OutState::WaitPubComp {
                            record.on_wire = true;
                            events.push(PollEvent::Outbound(Outbound::PubRel { packet_id }));
                            continue;
                        }
                        let dup = record.on_wire || record.state != OutState::Ready;
                        if !record.on_wire {
                            record.on_wire = true;
                            session.inflight_sent = session.inflight_sent.saturating_add(1);
                        }
                        record.state = match record.message.qos {
                            QoS::ExactlyOnce => OutState::WaitPubRec,
                            _ => OutState::WaitPubAck,
                        };
                        let event = PollEvent::Outbound(Outbound::Publish {
                            topic: record.message.topic.clone(),
                            payload: record.message.payload.clone(),
                            qos: record.message.qos,
                            packet_id: Some(packet_id),
                            dup,
                            retain: record.message.retain,
                        });
                        ops.push(WriteOp::UpsertMessage(stored_outbound(
                            client_id, packet_id, record,
                        )));
                        events.push(event);
                    }
                }
            }
            (events, ops)
        };
        if let Err(err) = self.persist(ops).await {
            tracing::warn!(error = %err, "持久化出站状态失败");
        }
        events
    }

    /// Put unacknowledged QoS 1/2 packets back on the send queue.
    pub fn requeue_unacked(&self, client_id: &str, epoch: u64) {
        let mut state = self.lock();
        let Some(session) = state.sessions.get_mut(client_id) else {
            return;
        };
        if session.epoch != epoch {
            return;
        }
        let ids: Vec<u16> = session
            .outbound
            .iter()
            .filter_map(|(id, record)| {
                matches!(
                    record.state,
                    OutState::WaitPubAck | OutState::WaitPubRec | OutState::WaitPubComp
                )
                .then_some(*id)
            })
            .collect();
        let mut armed = false;
        for id in ids {
            let queued = session
                .ready
                .iter()
                .any(|item| matches!(item, ReadyItem::Reliable(ready) if *ready == id));
            if !queued {
                session.ready.push_back(ReadyItem::Reliable(id));
                armed = true;
            }
        }
        if armed {
            session.notify.notify_one();
        }
    }

    /// End a connection. A stale epoch is ignored so a replaced connection cannot
    /// delete the new session.
    pub async fn disconnect(&self, request: DisconnectRequest) -> Result<(), CoreError> {
        let planned = {
            let mut state = self.lock();
            let Some(session) = state.sessions.get_mut(&request.client_id) else {
                return Ok(());
            };
            if session.epoch != request.epoch || !session.connected {
                return Ok(());
            }
            session.connected = false;
            session.disconnected_at_ms = Some(now_ms());
            session
                .ready
                .retain(|item| matches!(item, ReadyItem::Reliable(_)));
            if let Some(expiry) = request.session_expiry {
                session.session_expiry = Some(expiry);
            }
            if request.graceful {
                session.will = None;
                session.pending_will = None;
            } else if let Some(will) = session.will.take() {
                session.will_epoch = session.epoch;
                session.pending_will = Some(will);
            }
            let expiry = session.session_expiry;
            let epoch = session.epoch;
            let delay = session
                .pending_will
                .as_ref()
                .map(|will| will_delay(will, expiry));
            let client_id = request.client_id.clone();
            Plan {
                client_id,
                epoch,
                expiry,
                will_delay: delay,
            }
        };

        if planned.expiry == Some(0) {
            self.expire_session(&planned.client_id, planned.epoch)
                .await?;
            return Ok(());
        }

        let persisted = {
            let state = self.lock();
            state
                .sessions
                .get(&planned.client_id)
                .filter(|session| session.epoch == planned.epoch)
                .map(stored_client)
                .map(WriteOp::UpsertClient)
        };
        if let Some(op) = persisted {
            self.persist(vec![op]).await?;
        }

        if let Some(delay) = planned.will_delay {
            self.spawn_timer(Timer::Will {
                client_id: planned.client_id.clone(),
                epoch: planned.epoch,
                delay_secs: delay,
            });
        }
        if let Some(secs) = planned.expiry {
            self.spawn_timer(Timer::Expiry {
                client_id: planned.client_id,
                epoch: planned.epoch,
                delay_secs: secs,
            });
        }
        Ok(())
    }

    async fn publish_pending_will(&self, client_id: &str, epoch: u64) -> Result<(), CoreError> {
        let ops = {
            let mut state = self.lock();
            let Some(session) = state.sessions.get_mut(client_id) else {
                return Ok(());
            };
            if session.epoch != epoch || session.connected || session.will_epoch != epoch {
                return Ok(());
            }
            let Some(will) = session.pending_will.take() else {
                return Ok(());
            };
            let mut ops = vec![WriteOp::UpsertClient(stored_client(session))];
            let publish = incoming_from_will(&will);
            route(
                &mut state,
                &self.inner.config,
                client_id,
                &publish,
                &mut ops,
            );
            ops
        };
        self.persist(ops).await
    }

    async fn expire_session(&self, client_id: &str, epoch: u64) -> Result<(), CoreError> {
        let ops = {
            let mut state = self.lock();
            let Some(session) = state.sessions.get_mut(client_id) else {
                return Ok(());
            };
            if session.epoch != epoch || session.connected {
                return Ok(());
            }
            let will = session.pending_will.take().or_else(|| session.will.take());
            let mut ops = Vec::new();
            if let Some(will) = will {
                let publish = incoming_from_will(&will);
                route(
                    &mut state,
                    &self.inner.config,
                    client_id,
                    &publish,
                    &mut ops,
                );
            }
            forget_session(&mut state, client_id, &mut ops);
            ops
        };
        self.persist(ops).await
    }

    fn spawn_timer(&self, timer: Timer) {
        let broker = self.clone();
        tokio::spawn(async move {
            if timer.delay_secs() > 0 {
                tokio::time::sleep(Duration::from_secs(u64::from(timer.delay_secs()))).await;
            }
            let result = match &timer {
                Timer::Will {
                    client_id, epoch, ..
                } => broker.publish_pending_will(client_id, *epoch).await,
                Timer::Expiry {
                    client_id, epoch, ..
                } => broker.expire_session(client_id, *epoch).await,
            };
            if let Err(err) = result {
                tracing::warn!(error = %err, "会话定时任务失败");
            }
        });
    }

    async fn persist(&self, ops: Vec<WriteOp>) -> Result<(), CoreError> {
        apply_all(self.inner.storage.as_ref(), &ops).await
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|err| err.into_inner())
    }
}

struct Plan {
    client_id: String,
    epoch: u64,
    expiry: Option<u32>,
    will_delay: Option<u32>,
}

enum Timer {
    Will {
        client_id: String,
        epoch: u64,
        delay_secs: u32,
    },
    Expiry {
        client_id: String,
        epoch: u64,
        delay_secs: u32,
    },
}

impl Timer {
    fn client_id(&self) -> &str {
        match self {
            Self::Will { client_id, .. } | Self::Expiry { client_id, .. } => client_id,
        }
    }

    fn delay_secs(&self) -> u32 {
        match self {
            Self::Will { delay_secs, .. } | Self::Expiry { delay_secs, .. } => *delay_secs,
        }
    }
}

fn cracked_ids(ids: &[String]) -> Vec<String> {
    ids.to_vec()
}

fn resolve_client_id(request: &ConnectRequest) -> Result<String, ConnectError> {
    if request.client_id.is_empty() {
        let v4_clean = request.protocol == ProtocolVersion::V4
            && request.clean_start
            && request.session_expiry == Some(0);
        if request.protocol == ProtocolVersion::V4 && !v4_clean {
            return Err(ConnectError::IdentifierRejected);
        }
        return Ok(generate_client_id());
    }
    if request.client_id.len() > 255 || request.client_id.contains('\0') {
        return Err(ConnectError::IdentifierRejected);
    }
    Ok(request.client_id.clone())
}

fn generate_client_id() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    format!("flow-{:x}-{n:x}", now_ms())
}

fn now_ms() -> i64 {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    i64::try_from(millis).unwrap_or(i64::MAX)
}

fn new_session(client_id: &str, request: &ConnectRequest) -> Session {
    Session {
        client_id: client_id.to_owned(),
        epoch: 1,
        connected: true,
        protocol: request.protocol,
        clean_start: request.clean_start,
        session_expiry: request.session_expiry,
        keep_alive: request.keep_alive,
        receive_maximum: normalize_receive_maximum(request.receive_maximum),
        max_packet_size: request.max_packet_size,
        will: request.will.clone(),
        pending_will: None,
        will_epoch: 0,
        subscriptions: HashMap::new(),
        outbound: HashMap::new(),
        ready: VecDeque::new(),
        inflight_sent: 0,
        inbound: HashMap::new(),
        next_packet_id: 1,
        notify: Arc::new(Notify::new()),
        superseded: None,
        peer: request.peer.clone(),
        disconnected_at_ms: None,
    }
}

fn prepare_attached(session: &mut Session, request: &ConnectRequest, client_id: &str) {
    session.client_id = client_id.to_owned();
    session.epoch = session.epoch.saturating_add(1);
    session.connected = true;
    session.protocol = request.protocol;
    session.clean_start = request.clean_start;
    session.session_expiry = request.session_expiry;
    session.keep_alive = request.keep_alive;
    session.receive_maximum = normalize_receive_maximum(request.receive_maximum);
    session.max_packet_size = request.max_packet_size;
    session.will = request.will.clone();
    session.pending_will = None;
    session.peer = request.peer.clone();
    session.disconnected_at_ms = None;
    session.inflight_sent = 0;
}

fn normalize_receive_maximum(value: u16) -> u16 {
    if value == 0 { 65_535 } else { value }
}

fn arm_delivery(session: &mut Session) {
    session.inflight_sent = 0;
    session.ready.clear();
    for record in session.outbound.values_mut() {
        record.on_wire = false;
    }
    let mut ids: Vec<u16> = session.outbound.keys().copied().collect();
    ids.sort_unstable();
    for id in ids {
        session.ready.push_back(ReadyItem::Reliable(id));
    }
    session.notify.notify_one();
}

fn clear_session_state(state: &mut State, client_id: &str, ops: &mut Vec<WriteOp>) {
    if let Some(session) = state.sessions.get_mut(client_id) {
        session.subscriptions.clear();
        session.outbound.clear();
        session.ready.clear();
        session.inbound.clear();
        session.inflight_sent = 0;
        session.pending_will = None;
        session.will = None;
    }
    state.topics.remove_client(client_id);
    ops.push(WriteOp::ClearSubscriptions(client_id.to_owned()));
    ops.push(WriteOp::ClearMessages(client_id.to_owned()));
}

fn owns_session(session: Option<&Session>, epoch: u64) -> bool {
    session.is_some_and(|session| session.epoch == epoch && session.connected)
}

fn subscribe_one(
    state: &mut State,
    config: &BrokerConfig,
    client_id: &str,
    filter: &SubscribeFilter,
    ops: &mut Vec<WriteOp>,
) -> SubscribeAck {
    if filter.opts.retain_handling > 2 {
        return SubscribeAck::Failed(SubscribeFailure::Unspecified);
    }
    match classify_filter(&filter.filter) {
        Err(FilterClass::Invalid) => SubscribeAck::Failed(SubscribeFailure::Unspecified),
        Err(FilterClass::Shared) => {
            SubscribeAck::Failed(SubscribeFailure::SharedSubscriptionsNotSupported)
        }
        Ok(()) => {
            let existed = state
                .sessions
                .get(client_id)
                .is_some_and(|session| session.subscriptions.contains_key(&filter.filter));
            if let Some(session) = state.sessions.get_mut(client_id) {
                session
                    .subscriptions
                    .insert(filter.filter.clone(), filter.opts);
            }
            state
                .topics
                .subscribe(client_id, &filter.filter, filter.opts);
            ops.push(WriteOp::UpsertSubscription(StoredSubscription {
                client_id: client_id.to_owned(),
                filter: filter.filter.clone(),
                qos: filter.opts.qos,
                no_local: filter.opts.no_local,
                retain_as_published: filter.opts.retain_as_published,
                retain_handling: filter.opts.retain_handling,
            }));
            let send_retained = match filter.opts.retain_handling {
                2 => false,
                1 => !existed,
                _ => true,
            };
            if send_retained {
                let retained = state.topics.retained_for(&filter.filter);
                for message in retained {
                    let deliver = IncomingPublish {
                        topic: message.topic,
                        payload: message.payload,
                        qos: message.qos.min(filter.opts.qos),
                        retain: true,
                        packet_id: None,
                    };
                    enqueue(state, config, client_id, &deliver, true, ops);
                }
            }
            SubscribeAck::Granted(filter.opts.qos)
        }
    }
}

fn route(
    state: &mut State,
    config: &BrokerConfig,
    publisher: &str,
    publish: &IncomingPublish,
    ops: &mut Vec<WriteOp>,
) {
    if publish.retain {
        if publish.payload.is_empty() {
            state
                .topics
                .set_retained(&publish.topic, Bytes::new(), publish.qos);
            ops.push(WriteOp::DeleteRetained(publish.topic.clone()));
            return;
        }
        state
            .topics
            .set_retained(&publish.topic, publish.payload.clone(), publish.qos);
        ops.push(WriteOp::UpsertRetained(StoredRetained {
            topic: publish.topic.clone(),
            payload: publish.payload.clone(),
            qos: publish.qos,
        }));
    }

    let matched = state.topics.match_topic(&publish.topic);
    for subscriber in matched {
        if subscriber.no_local && subscriber.client_id == publisher {
            continue;
        }
        let retain = publish.retain && subscriber.retain_as_published;
        let deliver = IncomingPublish {
            topic: publish.topic.clone(),
            payload: publish.payload.clone(),
            qos: publish.qos.min(subscriber.qos),
            retain,
            packet_id: None,
        };
        enqueue(state, config, &subscriber.client_id, &deliver, false, ops);
    }
}

fn enqueue(
    state: &mut State,
    config: &BrokerConfig,
    client_id: &str,
    publish: &IncomingPublish,
    force_retain: bool,
    ops: &mut Vec<WriteOp>,
) {
    let Some(session) = state.sessions.get_mut(client_id) else {
        return;
    };
    let retain = if force_retain { true } else { publish.retain };
    if session.max_packet_size > 0 {
        let size = publish
            .topic
            .len()
            .saturating_add(publish.payload.len())
            .saturating_add(16);
        if size > usize::try_from(session.max_packet_size).unwrap_or(usize::MAX) {
            tracing::debug!(client_id, topic = %publish.topic, "丢弃超过客户端最大报文的消息");
            return;
        }
    }
    let message = AppMessage {
        topic: publish.topic.clone(),
        payload: publish.payload.clone(),
        qos: publish.qos,
        retain,
    };
    if publish.qos == QoS::AtMostOnce {
        if !session.connected {
            return;
        }
        trim_queue(session, config.max_queued_messages);
        session.ready.push_back(ReadyItem::QoS0(message));
        session.notify.notify_one();
        return;
    }
    trim_queue(session, config.max_queued_messages);
    let Some(packet_id) = alloc_packet_id(session) else {
        tracing::warn!(client_id, "出站报文标识耗尽，丢弃消息");
        return;
    };
    session.outbound.insert(
        packet_id,
        OutRecord {
            message: message.clone(),
            state: OutState::Ready,
            on_wire: false,
        },
    );
    if session.connected {
        session.ready.push_back(ReadyItem::Reliable(packet_id));
        session.notify.notify_one();
    }
    ops.push(WriteOp::UpsertMessage(StoredMessage {
        client_id: client_id.to_owned(),
        packet_id,
        topic: message.topic,
        payload: message.payload,
        qos: message.qos,
        retain: message.retain,
        direction: Direction::Out,
        state: MessageState::Pending,
    }));
}

fn trim_queue(session: &mut Session, max_queued: usize) {
    while session.ready.len().saturating_add(session.outbound.len()) >= max_queued
        && !session.ready.is_empty()
    {
        let Some(front) = session.ready.front() else {
            break;
        };
        match front {
            ReadyItem::QoS0(_) => {
                session.ready.pop_front();
            }
            ReadyItem::Reliable(id) => {
                let id = *id;
                let droppable = session
                    .outbound
                    .get(&id)
                    .is_some_and(|record| record.state == OutState::Ready && !record.on_wire);
                if !droppable {
                    break;
                }
                session.ready.pop_front();
                session.outbound.remove(&id);
            }
        }
    }
}

fn alloc_packet_id(session: &mut Session) -> Option<u16> {
    for _ in 0..u16::MAX {
        let id = session.next_packet_id;
        session.next_packet_id = if id == u16::MAX {
            1
        } else {
            id.saturating_add(1)
        };
        if id != 0 && !session.outbound.contains_key(&id) {
            return Some(id);
        }
    }
    None
}

fn next_ready(session: &Session) -> Option<usize> {
    session.ready.iter().position(|item| match item {
        ReadyItem::QoS0(_) => true,
        ReadyItem::Reliable(id) => session
            .outbound
            .get(id)
            .is_none_or(|record| can_send(session, record)),
    })
}

fn can_send(session: &Session, record: &OutRecord) -> bool {
    if record.state == OutState::WaitPubComp || record.on_wire {
        return true;
    }
    session.inflight_sent < session.receive_maximum
}

fn stored_client(session: &Session) -> StoredClient {
    StoredClient {
        client_id: session.client_id.clone(),
        protocol: session.protocol,
        connected: session.connected,
        peer: session.peer.clone(),
        keep_alive: session.keep_alive,
        clean_start: session.clean_start,
        session_expiry: session.session_expiry,
        will: session
            .will
            .clone()
            .or_else(|| session.pending_will.clone()),
        disconnected_at_ms: session.disconnected_at_ms,
    }
}

fn stored_outbound(client_id: &str, packet_id: u16, record: &OutRecord) -> StoredMessage {
    let state = match record.state {
        OutState::Ready => MessageState::Pending,
        OutState::WaitPubAck => MessageState::PubAck,
        OutState::WaitPubRec => MessageState::PubRec,
        OutState::WaitPubComp => MessageState::PubRel,
    };
    StoredMessage {
        client_id: client_id.to_owned(),
        packet_id,
        topic: record.message.topic.clone(),
        payload: record.message.payload.clone(),
        qos: record.message.qos,
        retain: record.message.retain,
        direction: Direction::Out,
        state,
    }
}

fn sub_opts(subscription: &StoredSubscription) -> SubOpts {
    SubOpts {
        qos: subscription.qos,
        no_local: subscription.no_local,
        retain_as_published: subscription.retain_as_published,
        retain_handling: subscription.retain_handling,
    }
}

fn session_from_stored(client: &StoredClient) -> Option<Session> {
    if !client.connected
        && let Some(expiry) = client.session_expiry
        && let Some(disconnected_at) = client.disconnected_at_ms
    {
        let elapsed_ms = now_ms().saturating_sub(disconnected_at);
        if elapsed_ms >= i64::from(expiry).saturating_mul(1000) {
            return None;
        }
    }
    let mut session = Session {
        client_id: client.client_id.clone(),
        epoch: 1,
        connected: client.connected,
        protocol: client.protocol,
        clean_start: client.clean_start,
        session_expiry: client.session_expiry,
        keep_alive: client.keep_alive,
        receive_maximum: 65_535,
        max_packet_size: 0,
        will: client.will.clone(),
        pending_will: None,
        will_epoch: 1,
        subscriptions: HashMap::new(),
        outbound: HashMap::new(),
        ready: VecDeque::new(),
        inflight_sent: 0,
        inbound: HashMap::new(),
        next_packet_id: 1,
        notify: Arc::new(Notify::new()),
        superseded: None,
        peer: client.peer.clone(),
        disconnected_at_ms: client.disconnected_at_ms,
    };
    if !client.connected {
        session.pending_will = session.will.take();
    }
    Some(session)
}

fn ensure_shell(state: &mut State, client_id: &str) {
    if state.sessions.contains_key(client_id) {
        return;
    }
    state.sessions.insert(
        client_id.to_owned(),
        Session {
            client_id: client_id.to_owned(),
            epoch: 1,
            connected: false,
            protocol: ProtocolVersion::V4,
            clean_start: false,
            session_expiry: None,
            keep_alive: 0,
            receive_maximum: 65_535,
            max_packet_size: 0,
            will: None,
            pending_will: None,
            will_epoch: 0,
            subscriptions: HashMap::new(),
            outbound: HashMap::new(),
            ready: VecDeque::new(),
            inflight_sent: 0,
            inbound: HashMap::new(),
            next_packet_id: 1,
            notify: Arc::new(Notify::new()),
            superseded: None,
            peer: None,
            disconnected_at_ms: Some(now_ms()),
        },
    );
}

fn restore_message(session: &mut Session, message: StoredMessage) {
    let app = AppMessage {
        topic: message.topic,
        payload: message.payload,
        qos: message.qos,
        retain: message.retain,
    };
    match message.direction {
        Direction::In => {
            session.inbound.insert(message.packet_id, app);
        }
        Direction::Out => {
            let state = match message.state {
                MessageState::Pending => OutState::Ready,
                MessageState::PubAck => OutState::WaitPubAck,
                MessageState::PubRec => OutState::WaitPubRec,
                MessageState::PubRel => OutState::WaitPubComp,
            };
            session.outbound.insert(
                message.packet_id,
                OutRecord {
                    message: app,
                    state,
                    on_wire: false,
                },
            );
            let next = message.packet_id.saturating_add(1).max(1);
            if next > session.next_packet_id {
                session.next_packet_id = next;
            }
        }
    }
}

fn forget_session(state: &mut State, client_id: &str, ops: &mut Vec<WriteOp>) {
    state.topics.remove_client(client_id);
    state.sessions.remove(client_id);
    ops.push(WriteOp::DeleteClient(client_id.to_owned()));
}

fn incoming_from_will(will: &Will) -> IncomingPublish {
    IncomingPublish {
        topic: will.topic.clone(),
        payload: will.payload.clone(),
        qos: will.qos,
        retain: will.retain,
        packet_id: None,
    }
}

fn will_delay(will: &Will, expiry: Option<u32>) -> u32 {
    match expiry {
        Some(0) => 0,
        Some(secs) => will.delay_secs.min(secs),
        None => will.delay_secs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::SubscribeFilter;

    fn config() -> BrokerConfig {
        BrokerConfig::default()
    }

    async fn broker() -> Broker {
        Broker::open(Arc::new(crate::MemoryStorage::new()), config())
            .await
            .expect("open")
    }

    fn connect_req(id: &str, clean: bool) -> ConnectRequest {
        ConnectRequest {
            client_id: id.to_owned(),
            protocol: ProtocolVersion::V4,
            clean_start: clean,
            session_expiry: if clean { Some(0) } else { None },
            keep_alive: 30,
            will: None,
            receive_maximum: 16,
            max_packet_size: 0,
            enhanced_auth: false,
            peer: None,
        }
    }

    async fn connect(broker: &Broker, id: &str, clean: bool) -> ConnectedSession {
        broker
            .connect(connect_req(id, clean))
            .await
            .expect("connect")
    }

    async fn subscribe(broker: &Broker, session: &ConnectedSession, filter: &str, qos: QoS) {
        let acks = broker
            .subscribe(
                &session.client_id,
                session.epoch,
                &[SubscribeFilter {
                    filter: filter.to_owned(),
                    opts: SubOpts::v4(qos),
                }],
            )
            .await
            .expect("subscribe");
        assert_eq!(acks, vec![SubscribeAck::Granted(qos)]);
    }

    async fn publish_qos0(
        broker: &Broker,
        session: &ConnectedSession,
        topic: &str,
        payload: &[u8],
    ) {
        let flow = broker
            .publish(
                &session.client_id,
                session.epoch,
                IncomingPublish {
                    topic: topic.to_owned(),
                    payload: Bytes::copy_from_slice(payload),
                    qos: QoS::AtMostOnce,
                    retain: false,
                    packet_id: None,
                },
            )
            .await
            .expect("publish");
        assert_eq!(flow, ClientFlow::Accepted);
    }

    fn payload_of(event: &PollEvent) -> Option<Bytes> {
        match event {
            PollEvent::Outbound(Outbound::Publish { payload, .. }) => Some(payload.clone()),
            _ => None,
        }
    }

    #[tokio::test]
    async fn publish_should_fanout_to_matching_subscribers() {
        let broker = broker().await;
        let publisher = connect(&broker, "pub", true).await;
        let subscriber = connect(&broker, "sub", true).await;
        subscribe(&broker, &subscriber, "sensors/+/temp", QoS::AtMostOnce).await;
        publish_qos0(&broker, &publisher, "sensors/lab/temp", b"21").await;

        let events = broker
            .poll(&subscriber.client_id, subscriber.epoch, 8)
            .await;
        assert_eq!(
            payload_of(events.first().expect("event"))
                .expect("payload")
                .as_ref(),
            b"21"
        );
    }

    #[tokio::test]
    async fn qos1_should_queue_while_subscriber_is_offline_and_clear_on_ack() {
        let broker = broker().await;
        let subscriber = connect(&broker, "sub", false).await;
        subscribe(&broker, &subscriber, "a/b", QoS::AtLeastOnce).await;
        broker
            .disconnect(DisconnectRequest {
                client_id: subscriber.client_id.clone(),
                epoch: subscriber.epoch,
                graceful: true,
                session_expiry: None,
            })
            .await
            .expect("disconnect");

        let publisher = connect(&broker, "pub", true).await;
        let flow = broker
            .publish(
                &publisher.client_id,
                publisher.epoch,
                IncomingPublish {
                    topic: "a/b".into(),
                    payload: Bytes::from_static(b"queued"),
                    qos: QoS::AtLeastOnce,
                    retain: false,
                    packet_id: Some(7),
                },
            )
            .await
            .expect("publish");
        assert_eq!(flow, ClientFlow::PubAck);

        let resumed = connect(&broker, "sub", false).await;
        assert!(resumed.session_present);
        let events = broker.poll(&resumed.client_id, resumed.epoch, 4).await;
        let PollEvent::Outbound(Outbound::Publish {
            payload,
            qos,
            packet_id,
            dup,
            ..
        }) = events.first().expect("queued publish")
        else {
            panic!("expected publish");
        };
        assert_eq!(payload.as_ref(), b"queued");
        assert_eq!(*qos, QoS::AtLeastOnce);
        assert!(!dup);
        let packet_id = packet_id.expect("packet id");
        broker
            .acknowledge(
                &resumed.client_id,
                resumed.epoch,
                packet_id,
                ClientAck::PubAck,
            )
            .await
            .expect("ack");
        let again = broker.poll(&resumed.client_id, resumed.epoch, 4).await;
        assert!(again.is_empty());
    }

    #[tokio::test]
    async fn qos2_should_forward_only_after_pubrel() {
        let broker = broker().await;
        let publisher = connect(&broker, "pub", true).await;
        let subscriber = connect(&broker, "sub", true).await;
        subscribe(&broker, &subscriber, "q2", QoS::ExactlyOnce).await;

        let flow = broker
            .publish(
                &publisher.client_id,
                publisher.epoch,
                IncomingPublish {
                    topic: "q2".into(),
                    payload: Bytes::from_static(b"once"),
                    qos: QoS::ExactlyOnce,
                    retain: false,
                    packet_id: Some(3),
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

        assert_eq!(
            broker
                .pubrel(&publisher.client_id, publisher.epoch, 3)
                .await
                .expect("pubrel"),
            ClientFlow::PubComp
        );
        let events = broker
            .poll(&subscriber.client_id, subscriber.epoch, 4)
            .await;
        let PollEvent::Outbound(Outbound::Publish { packet_id, qos, .. }) =
            events.first().expect("delivery")
        else {
            panic!("expected publish");
        };
        assert_eq!(*qos, QoS::ExactlyOnce);
        let packet_id = packet_id.expect("id");
        broker
            .acknowledge(
                &subscriber.client_id,
                subscriber.epoch,
                packet_id,
                ClientAck::PubRec,
            )
            .await
            .expect("pubrec");
        let rel = broker
            .poll(&subscriber.client_id, subscriber.epoch, 4)
            .await;
        assert!(matches!(
            rel.first(),
            Some(PollEvent::Outbound(Outbound::PubRel { .. }))
        ));
    }

    #[tokio::test]
    async fn retained_message_should_be_sent_on_subscribe_and_cleared_by_empty_payload() {
        let broker = broker().await;
        let publisher = connect(&broker, "pub", true).await;
        broker
            .publish(
                &publisher.client_id,
                publisher.epoch,
                IncomingPublish {
                    topic: "status".into(),
                    payload: Bytes::from_static(b"on"),
                    qos: QoS::AtMostOnce,
                    retain: true,
                    packet_id: None,
                },
            )
            .await
            .expect("retain");
        let subscriber = connect(&broker, "sub", true).await;
        subscribe(&broker, &subscriber, "status", QoS::AtMostOnce).await;
        let events = broker
            .poll(&subscriber.client_id, subscriber.epoch, 4)
            .await;
        assert_eq!(
            payload_of(events.first().expect("retained"))
                .expect("bytes")
                .as_ref(),
            b"on"
        );

        broker
            .publish(
                &publisher.client_id,
                publisher.epoch,
                IncomingPublish {
                    topic: "status".into(),
                    payload: Bytes::new(),
                    qos: QoS::AtMostOnce,
                    retain: true,
                    packet_id: None,
                },
            )
            .await
            .expect("clear");
        let late = connect(&broker, "late", true).await;
        subscribe(&broker, &late, "status", QoS::AtMostOnce).await;
        assert!(broker.poll(&late.client_id, late.epoch, 4).await.is_empty());
    }

    #[tokio::test]
    async fn abrupt_disconnect_should_publish_will_and_graceful_disconnect_should_not() {
        let broker = broker().await;
        let watcher = connect(&broker, "watch", true).await;
        subscribe(&broker, &watcher, "will", QoS::AtMostOnce).await;
        let client = broker
            .connect(ConnectRequest {
                will: Some(Will {
                    topic: "will".into(),
                    payload: Bytes::from_static(b"gone"),
                    qos: QoS::AtMostOnce,
                    retain: false,
                    delay_secs: 0,
                }),
                ..connect_req("client", true)
            })
            .await
            .expect("connect");
        broker
            .disconnect(DisconnectRequest {
                client_id: client.client_id.clone(),
                epoch: client.epoch,
                graceful: false,
                session_expiry: None,
            })
            .await
            .expect("drop");
        let events = broker.poll(&watcher.client_id, watcher.epoch, 4).await;
        assert_eq!(
            payload_of(events.first().expect("will"))
                .expect("bytes")
                .as_ref(),
            b"gone"
        );

        let client = broker
            .connect(ConnectRequest {
                will: Some(Will {
                    topic: "will".into(),
                    payload: Bytes::from_static(b"nope"),
                    qos: QoS::AtMostOnce,
                    retain: false,
                    delay_secs: 0,
                }),
                ..connect_req("client", true)
            })
            .await
            .expect("reconnect");
        broker
            .disconnect(DisconnectRequest {
                client_id: client.client_id,
                epoch: client.epoch,
                graceful: true,
                session_expiry: None,
            })
            .await
            .expect("bye");
        assert!(
            broker
                .poll(&watcher.client_id, watcher.epoch, 4)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn persistent_session_should_survive_a_new_broker_process() {
        let storage = Arc::new(crate::MemoryStorage::new());
        let first = Broker::open(Arc::clone(&storage) as Arc<dyn Storage>, config())
            .await
            .expect("first");
        let subscriber = connect(&first, "sub", false).await;
        subscribe(&first, &subscriber, "keep/#", QoS::AtLeastOnce).await;
        first
            .disconnect(DisconnectRequest {
                client_id: subscriber.client_id,
                epoch: subscriber.epoch,
                graceful: true,
                session_expiry: None,
            })
            .await
            .expect("disconnect");
        drop(first);

        let second = Broker::open(storage, config()).await.expect("second");
        let publisher = connect(&second, "pub", true).await;
        second
            .publish(
                &publisher.client_id,
                publisher.epoch,
                IncomingPublish {
                    topic: "keep/1".into(),
                    payload: Bytes::from_static(b"back"),
                    qos: QoS::AtLeastOnce,
                    retain: false,
                    packet_id: Some(1),
                },
            )
            .await
            .expect("publish");
        let subscriber = connect(&second, "sub", false).await;
        assert!(subscriber.session_present);
        let events = second
            .poll(&subscriber.client_id, subscriber.epoch, 4)
            .await;
        assert_eq!(
            payload_of(events.first().expect("restored"))
                .expect("bytes")
                .as_ref(),
            b"back"
        );
    }

    #[tokio::test]
    async fn session_takeover_should_disconnect_the_previous_epoch() {
        let broker = broker().await;
        let first = connect(&broker, "same", true).await;
        let second = connect(&broker, "same", true).await;
        let events = broker.poll(&first.client_id, first.epoch, 4).await;
        assert!(matches!(
            events.first(),
            Some(PollEvent::Disconnect { reason: Some(0x8E) })
        ));
        assert!(
            broker
                .poll(&second.client_id, second.epoch, 4)
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn empty_v4_client_id_should_be_rejected_when_session_is_persistent() {
        let broker = broker().await;
        let err = broker
            .connect(ConnectRequest {
                client_id: String::new(),
                clean_start: false,
                session_expiry: None,
                ..connect_req("ignored", false)
            })
            .await
            .expect_err("empty id");
        assert_eq!(err, ConnectError::IdentifierRejected);
    }

    #[tokio::test]
    async fn no_local_should_suppress_the_publishers_own_subscription() {
        let broker = broker().await;
        let client = connect(&broker, "loop", true).await;
        broker
            .subscribe(
                &client.client_id,
                client.epoch,
                &[SubscribeFilter {
                    filter: "loop".into(),
                    opts: SubOpts {
                        qos: QoS::AtMostOnce,
                        no_local: true,
                        retain_as_published: false,
                        retain_handling: 0,
                    },
                }],
            )
            .await
            .expect("sub");
        publish_qos0(&broker, &client, "loop", b"self").await;
        assert!(
            broker
                .poll(&client.client_id, client.epoch, 4)
                .await
                .is_empty()
        );
    }
}
