//! Client error types.

use thiserror::Error;

/// Errors that can occur during MQTT client operations.
#[derive(Debug, Error)]
pub enum ClientError {
    /// Network I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// TLS handshake or configuration error.
    #[error("TLS error: {0}")]
    Tls(String),

    /// Codec encode/decode error.
    #[error("codec error: {0}")]
    Codec(#[from] heron_codec::MqttError),

    /// Connection was refused by the broker.
    #[error("connection refused: {reason}")]
    ConnectionRefused { reason: String },

    /// Operation timed out.
    #[error("operation timed out")]
    Timeout,

    /// Client has been disconnected.
    #[error("client disconnected")]
    Disconnected,

    /// The internal channel was closed unexpectedly.
    #[error("internal channel closed")]
    ChannelClosed,

    /// An unexpected control packet was received.
    #[error("unexpected packet: {0}")]
    UnexpectedPacket(String),

    /// QoS must be 0, 1, or 2.
    #[error("invalid QoS level: {0}")]
    InvalidQoS(u8),

    /// The broker refused one or more topic filters in SUBSCRIBE.
    #[error("subscribe rejected by broker: {codes:?}")]
    SubscribeRejected { codes: Vec<u8> },

    /// The broker refused one or more topic filters in UNSUBSCRIBE.
    #[error("unsubscribe rejected by broker: {codes:?}")]
    UnsubscribeRejected { codes: Vec<u8> },
}
