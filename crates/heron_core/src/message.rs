//! Application messages and subscription options.

use bytes::Bytes;

use crate::qos::QoS;

/// MQTT protocol version negotiated on CONNECT.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolVersion {
    /// MQTT v3.1.1.
    V4,
    /// MQTT v5.0.
    V5,
}

impl ProtocolVersion {
    /// Protocol level byte.
    pub fn level(self) -> u8 {
        match self {
            Self::V4 => 4,
            Self::V5 => 5,
        }
    }
}

/// Last Will stored for a session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Will {
    /// Will topic.
    pub topic: String,
    /// Will payload.
    pub payload: Bytes,
    /// Will QoS.
    pub qos: QoS,
    /// Whether the will is retained.
    pub retain: bool,
    /// Seconds to wait before publishing. Zero publishes as soon as the session ends.
    pub delay_secs: u32,
}

/// A publication accepted by the broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppMessage {
    /// Topic name.
    pub topic: String,
    /// Payload bytes.
    pub payload: Bytes,
    /// QoS used for this hop.
    pub qos: QoS,
    /// Retain flag to put on the outgoing PUBLISH.
    pub retain: bool,
}

/// Options that apply to one subscription filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubOpts {
    /// Maximum QoS the subscriber is willing to receive.
    pub qos: QoS,
    /// Do not deliver messages published by this client.
    pub no_local: bool,
    /// Keep the publisher's retain flag when forwarding.
    pub retain_as_published: bool,
    /// MQTT v5 retain handling: 0 send, 1 send only on a new subscription, 2 never.
    pub retain_handling: u8,
}

impl SubOpts {
    /// v3.1.1 subscription: QoS only.
    pub fn v4(qos: QoS) -> Self {
        Self {
            qos,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
        }
    }
}

/// One filter inside SUBSCRIBE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeFilter {
    /// Topic filter, possibly containing `+` or `#`.
    pub filter: String,
    /// Subscription options.
    pub opts: SubOpts,
}

/// Fields from CONNECT that the session layer needs.
#[derive(Debug, Clone)]
pub struct ConnectRequest {
    /// Client identifier. Empty asks the server to assign one when the protocol allows it.
    pub client_id: String,
    /// Negotiated protocol.
    pub protocol: ProtocolVersion,
    /// MQTT v5 clean start, or MQTT v3.1.1 clean session.
    pub clean_start: bool,
    /// `None` keeps the session until it is replaced (MQTT v3.1.1 persistent).
    /// `Some(0)` discards the session when the connection closes.
    pub session_expiry: Option<u32>,
    /// Client keep-alive in seconds. Zero disables the broker-side timer.
    pub keep_alive: u16,
    /// Optional will.
    pub will: Option<Will>,
    /// Client receive maximum. Zero is a protocol error and is treated as the default.
    pub receive_maximum: u16,
    /// Client maximum packet size. Zero means unlimited.
    pub max_packet_size: u32,
    /// CONNECT included an authentication method property.
    pub enhanced_auth: bool,
    /// Peer address for diagnostics, when the listener knows it.
    pub peer: Option<String>,
    /// MQTT username. Empty is treated as absent.
    pub username: Option<String>,
    /// MQTT password or token.
    pub password: Option<Bytes>,
    /// Identity from a verified client certificate.
    pub certificate_identity: Option<String>,
}

/// A running connection attached to a session.
#[derive(Debug, Clone)]
pub struct ConnectedSession {
    /// Client identifier, including one assigned by the server.
    pub client_id: String,
    /// Epoch the connection must present on later calls.
    pub epoch: u64,
    /// True when a previous session's subscriptions were kept.
    pub session_present: bool,
    /// True when `client_id` was generated because CONNECT omitted it.
    pub server_assigned_id: bool,
    /// Keep-alive accepted for this connection.
    pub keep_alive: u16,
    /// Wakes the connection when outbound data or a kick is available.
    pub notify: std::sync::Arc<tokio::sync::Notify>,
}

/// PUBLISH arriving from a connected client.
#[derive(Debug, Clone)]
pub struct IncomingPublish {
    /// Topic name.
    pub topic: String,
    /// Payload.
    pub payload: Bytes,
    /// Requested QoS.
    pub qos: QoS,
    /// Retain flag.
    pub retain: bool,
    /// Packet identifier. Required when `qos` is greater than 0.
    pub packet_id: Option<u16>,
}

/// Bytes the connection should write to the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outbound {
    /// PUBLISH towards the subscriber.
    Publish {
        /// Topic name.
        topic: String,
        /// Payload.
        payload: Bytes,
        /// Effective QoS.
        qos: QoS,
        /// Present when `qos` is 1 or 2.
        packet_id: Option<u16>,
        /// Duplicate delivery of an unacked publication.
        dup: bool,
        /// Retain flag on the wire.
        retain: bool,
    },
    /// PUBREL for an outbound QoS 2 publication.
    PubRel {
        /// Packet identifier of the publication.
        packet_id: u16,
    },
}

/// Something the connection task should do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PollEvent {
    /// Encode and write this packet.
    Outbound(Outbound),
    /// The session was taken over or closed. `reason` is an MQTT v5 disconnect reason.
    Disconnect {
        /// `None` closes the socket without a DISCONNECT packet.
        reason: Option<u8>,
    },
}

/// How the subscriber acknowledged an outbound publication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientAck {
    /// QoS 1 PUBACK.
    PubAck,
    /// QoS 2 PUBREC.
    PubRec,
    /// QoS 2 PUBCOMP.
    PubComp,
}

/// Parameters for ending a connection.
#[derive(Debug, Clone)]
pub struct DisconnectRequest {
    /// Client identifier.
    pub client_id: String,
    /// Epoch captured at CONNECT.
    pub epoch: u64,
    /// True when the client sent DISCONNECT. Will is discarded.
    pub graceful: bool,
    /// MQTT v5 session expiry override carried by DISCONNECT.
    pub session_expiry: Option<u32>,
}
