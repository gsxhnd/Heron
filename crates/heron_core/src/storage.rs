//! Persistence boundary for sessions, subscriptions, retained messages, and inflight QoS.

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::CoreError;
use crate::message::{ProtocolVersion, Will};
use crate::qos::QoS;

/// Which side owns the packet identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    /// Broker-to-client publication.
    Out,
    /// Client-to-broker QoS 2 publication waiting for PUBREL.
    In,
}

impl Direction {
    /// Stable column value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Out => "out",
            Self::In => "in",
        }
    }

    /// Parse a column value.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "out" => Some(Self::Out),
            "in" => Some(Self::In),
            _ => None,
        }
    }
}

/// Inflight or queued message state.
///
/// Names follow the database design: `pending`, `puback`, `pubrec`, `pubrel`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageState {
    /// Queued and not yet written to the socket.
    Pending,
    /// Outbound QoS 1 waiting for PUBACK.
    PubAck,
    /// Outbound QoS 2 waiting for PUBREC, or inbound QoS 2 waiting for PUBREL.
    PubRec,
    /// Outbound QoS 2 waiting for PUBCOMP.
    PubRel,
}

impl MessageState {
    /// Column value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::PubAck => "puback",
            Self::PubRec => "pubrec",
            Self::PubRel => "pubrel",
        }
    }

    /// Parse a column value.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "puback" => Some(Self::PubAck),
            "pubrec" => Some(Self::PubRec),
            "pubrel" => Some(Self::PubRel),
            _ => None,
        }
    }
}

/// Durable client and session row.
#[derive(Debug, Clone)]
pub struct StoredClient {
    /// Client identifier.
    pub client_id: String,
    /// Protocol version.
    pub protocol: ProtocolVersion,
    /// Whether a socket was attached when the row was written.
    pub connected: bool,
    /// Peer address, when known.
    pub peer: Option<String>,
    /// Keep-alive seconds.
    pub keep_alive: u16,
    /// Clean start / clean session of the latest CONNECT.
    pub clean_start: bool,
    /// Session expiry. `None` means the session does not expire.
    pub session_expiry: Option<u32>,
    /// Will, if one is still armed.
    pub will: Option<Will>,
    /// Unix milliseconds when the session last went offline.
    pub disconnected_at_ms: Option<i64>,
}

/// One persisted subscription.
#[derive(Debug, Clone)]
pub struct StoredSubscription {
    /// Owning client.
    pub client_id: String,
    /// Topic filter.
    pub filter: String,
    /// Granted QoS.
    pub qos: QoS,
    /// MQTT v5 no-local.
    pub no_local: bool,
    /// MQTT v5 retain-as-published.
    pub retain_as_published: bool,
    /// MQTT v5 retain handling.
    pub retain_handling: u8,
}

/// Retained message row.
#[derive(Debug, Clone)]
pub struct StoredRetained {
    /// Topic name.
    pub topic: String,
    /// Payload.
    pub payload: Bytes,
    /// QoS of the retained publication.
    pub qos: QoS,
}

/// Offline or inflight message row.
#[derive(Debug, Clone)]
pub struct StoredMessage {
    /// Target client for outbound rows, publisher for inbound rows.
    pub client_id: String,
    /// Packet identifier.
    pub packet_id: u16,
    /// Topic name.
    pub topic: String,
    /// Payload.
    pub payload: Bytes,
    /// QoS.
    pub qos: QoS,
    /// Retain flag stored with the message.
    pub retain: bool,
    /// Direction of the packet identifier.
    pub direction: Direction,
    /// Handshake state.
    pub state: MessageState,
}

/// Operations the broker emits after changing in-memory state.
#[derive(Debug, Clone)]
pub enum WriteOp {
    /// Insert or replace a client session.
    UpsertClient(StoredClient),
    /// Delete the client and every dependent row.
    DeleteClient(String),
    /// Insert or replace one subscription.
    UpsertSubscription(StoredSubscription),
    /// Remove one subscription.
    DeleteSubscription {
        /// Client identifier.
        client_id: String,
        /// Topic filter.
        filter: String,
    },
    /// Remove every subscription for a client.
    ClearSubscriptions(String),
    /// Insert or replace a retained message.
    UpsertRetained(StoredRetained),
    /// Delete a retained message.
    DeleteRetained(String),
    /// Insert or replace an inflight message.
    UpsertMessage(StoredMessage),
    /// Delete one inflight message.
    DeleteMessage {
        /// Client identifier.
        client_id: String,
        /// Packet identifier.
        packet_id: u16,
        /// Direction.
        direction: Direction,
    },
    /// Delete every inflight message for a client.
    ClearMessages(String),
}

/// Durable store. The in-memory broker is the hot path; this trait is write-through.
#[async_trait]
pub trait Storage: Send + Sync + 'static {
    /// Load every client row.
    async fn load_clients(&self) -> Result<Vec<StoredClient>, CoreError>;

    /// Load every subscription.
    async fn load_subscriptions(&self) -> Result<Vec<StoredSubscription>, CoreError>;

    /// Load every retained message.
    async fn load_retained(&self) -> Result<Vec<StoredRetained>, CoreError>;

    /// Load every inflight or queued message.
    async fn load_messages(&self) -> Result<Vec<StoredMessage>, CoreError>;

    /// Apply one write.
    async fn apply(&self, op: &WriteOp) -> Result<(), CoreError>;
}

/// Apply a batch, stopping on the first error.
pub async fn apply_all(storage: &dyn Storage, ops: &[WriteOp]) -> Result<(), CoreError> {
    for op in ops {
        storage.apply(op).await?;
    }
    Ok(())
}
