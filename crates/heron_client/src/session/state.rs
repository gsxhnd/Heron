//! Persistent client session state across reconnects.

use std::collections::{HashMap, VecDeque};

use bytes::Bytes;

use crate::client::{AckReply, CodesReply, Command};
use crate::error::ClientError;
use crate::inflight::InflightStore;

/// An in-flight SUBSCRIBE / UNSUBSCRIBE awaiting its acknowledgement.
#[derive(Debug)]
pub(crate) enum PendingOp {
    Subscribe {
        filters: Vec<String>,
        reply: CodesReply,
    },
    Unsubscribe {
        /// Subscriptions removed optimistically; restored if the broker refuses.
        removed: Vec<(String, u8)>,
        reply: AckReply,
    },
}

/// Client-side session state retained across TCP/TLS reconnects.
#[derive(Debug)]
pub(crate) struct SessionState {
    pub next_packet_id: u16,
    /// Active subscriptions keyed by topic filter → requested QoS.
    pub subscriptions: HashMap<String, u8>,
    pub inflight: InflightStore,
    /// Incoming QoS 2 messages awaiting PUBREL.
    pub qos2_incoming: HashMap<u16, (String, Bytes, bool)>,
    /// Commands received while offline (reconnect backoff).
    pub pending_commands: VecDeque<Command>,
    /// SUBSCRIBE / UNSUBSCRIBE awaiting SUBACK / UNSUBACK, keyed by packet id.
    pub pending_ops: HashMap<u16, PendingOp>,
    /// Reply channels for outbound QoS 1/2 publishes, keyed by packet id.
    pub publish_replies: HashMap<u16, AckReply>,
    /// Client identifier assigned by the broker (v5.0 `Assigned Client Identifier`).
    pub assigned_client_id: Option<String>,
}

impl SessionState {
    pub fn new(max_inflight: u16) -> Self {
        Self {
            next_packet_id: 1,
            subscriptions: HashMap::new(),
            inflight: InflightStore::new(max_inflight),
            qos2_incoming: HashMap::new(),
            pending_commands: VecDeque::new(),
            pending_ops: HashMap::new(),
            publish_replies: HashMap::new(),
            assigned_client_id: None,
        }
    }

    /// Reset volatile state before a clean session / clean start.
    ///
    /// Subscriptions are intentionally preserved: they describe what the caller
    /// asked for, and are replayed on every new connection (the broker discards
    /// its own state for a clean session).
    pub fn clear_for_clean_start(&mut self) {
        self.fail_pending();
        self.inflight.clear();
        self.qos2_incoming.clear();
        self.next_packet_id = 1;
        self.assigned_client_id = None;
        // Keep pending_commands — user intent survives clean reconnect.
    }

    /// Allocate a packet identifier that is not currently in use.
    ///
    /// Skips ids still referenced by inflight publishes or pending operations so
    /// that a wrapped counter can never alias a live handshake.
    pub fn alloc_packet_id(&mut self) -> u16 {
        for _ in 0..=u16::MAX {
            let id = self.next_packet_id;
            self.next_packet_id = if id == u16::MAX { 1 } else { id + 1 };
            if id == 0 {
                continue;
            }
            let free = !self.inflight.contains(id)
                && !self.pending_ops.contains_key(&id)
                && !self.publish_replies.contains_key(&id);
            if free {
                return id;
            }
        }
        // Every id is busy — caller will observe the failure on the wire.
        self.next_packet_id
    }

    pub fn remember_subscription(&mut self, filter: String, qos: u8) {
        self.subscriptions.insert(filter, qos);
    }

    pub fn forget_subscriptions(&mut self, filters: &[String]) {
        for filter in filters {
            self.subscriptions.remove(filter);
        }
    }

    /// Restore subscriptions previously removed for an UNSUBSCRIBE.
    pub fn restore_subscriptions(&mut self, entries: Vec<(String, u8)>) {
        for (filter, qos) in entries {
            self.subscriptions.insert(filter, qos);
        }
    }

    /// Answer every outstanding operation with [`ClientError::Disconnected`].
    ///
    /// Called when a connection ends so callers awaiting an acknowledgement are
    /// never left hanging: message delivery itself still follows the session
    /// semantics and stays observable through the event stream.
    pub fn fail_pending(&mut self) {
        for pending in self.inflight.drain_pending() {
            let _ = pending.reply.send(Err(ClientError::Disconnected));
        }
        for (_, reply) in self.publish_replies.drain() {
            let _ = reply.send(Err(ClientError::Disconnected));
        }
        let ops: Vec<PendingOp> = self.pending_ops.drain().map(|(_, op)| op).collect();
        for op in ops {
            match op {
                PendingOp::Subscribe { reply, .. } => {
                    let _ = reply.send(Err(ClientError::Disconnected));
                }
                PendingOp::Unsubscribe { removed, reply } => {
                    self.restore_subscriptions(removed);
                    let _ = reply.send(Err(ClientError::Disconnected));
                }
            }
        }
    }

    /// Complete an outbound QoS 1/2 publish with `result`.
    pub fn complete_publish(&mut self, packet_id: u16, result: Result<(), ClientError>) {
        if let Some(reply) = self.publish_replies.remove(&packet_id) {
            let _ = reply.send(result);
        }
    }
}
