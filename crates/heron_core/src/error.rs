//! Errors returned by the broker core.

use crate::qos::QoS;

/// Failure while persisting or loading broker state.
#[derive(Debug, thiserror::Error)]
pub enum CoreError {
    /// The storage backend rejected the operation.
    #[error("storage error: {0}")]
    Storage(String),

    /// The client has no session, or the caller holds a stale epoch.
    #[error("session not found")]
    SessionNotFound,
}

/// CONNECT was refused before a session was attached.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConnectError {
    /// Client identifier is empty when a session must be resumed, or longer than 255 bytes.
    #[error("client identifier rejected")]
    IdentifierRejected,

    /// CONNECT carried an authentication method. Enhanced auth is not part of v0.2.0.
    #[error("bad authentication method")]
    BadAuthenticationMethod,

    /// The broker could not finish session setup.
    #[error("server unavailable: {0}")]
    ServerUnavailable(String),
}

impl From<CoreError> for ConnectError {
    fn from(value: CoreError) -> Self {
        match value {
            CoreError::Storage(message) => Self::ServerUnavailable(message),
            CoreError::SessionNotFound => Self::ServerUnavailable("session not found".into()),
        }
    }
}

/// A single SUBSCRIBE filter was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscribeFailure {
    /// Generic refusal (MQTT 0x80).
    Unspecified,
    /// `$share/...` filters are not implemented.
    SharedSubscriptionsNotSupported,
}

impl SubscribeFailure {
    /// MQTT v5 reason code for this failure.
    pub fn reason_code(self) -> u8 {
        match self {
            Self::Unspecified => 0x80,
            Self::SharedSubscriptionsNotSupported => 0x9E,
        }
    }
}

/// Result of one topic filter in SUBSCRIBE.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscribeAck {
    /// The filter was accepted at this maximum QoS.
    Granted(QoS),
    /// The filter was rejected.
    Failed(SubscribeFailure),
}

/// Why a PUBLISH cannot be accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishError {
    /// Topic name is empty or contains a wildcard.
    InvalidTopic,
    /// QoS > 0 without a non-zero packet identifier.
    MissingPacketId,
    /// More than the server receive maximum of unacked QoS 1/2 publishes are outstanding.
    ReceiveMaximumExceeded,
    /// The caller does not own the current session.
    StaleSession,
}

/// Packet the connection should write after handling a client publish or release.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientFlow {
    /// QoS 0 publish was routed.
    Accepted,
    /// Send PUBACK.
    PubAck,
    /// Send PUBREC.
    PubRec,
    /// Send PUBCOMP.
    PubComp,
}
