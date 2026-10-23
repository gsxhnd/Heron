//! Async MQTT client library supporting v3.1.1 and v5.0 protocols.
//!
//! Built on `mqtt_codec`, with TCP/TLS transport, automatic reconnect,
//! QoS 1/2 inflight tracking, and a CLI binary (`mqtt`).
//!
//! ```text
//! cargo run -p mqtt_client -- pub -t test/topic -m hello
//! cargo run -p mqtt_client -- --protocol v5 --tls sub -H broker.emqx.io -p 8883 -t test/topic
//! ```
//!
//! # Architecture
//!
//! - [`MqttClient`] — cloneable handle; operations enqueue to a background event loop
//! - [`ClientConfig`] — broker, TLS, reconnect, inflight, and session options
//! - [`Event`] — connected / messages / acks / reconnect / disconnect
//!
//! # Operation semantics
//!
//! [`MqttClient::publish`], [`MqttClient::subscribe`] and
//! [`MqttClient::unsubscribe`] are request/response style: they resolve once the
//! broker acknowledges the operation, bounded by
//! [`ClientConfig::request_timeout`].
//!
//! - QoS 0 resolves after the packet is written; QoS 1/2 after PUBACK / PUBCOMP.
//! - `subscribe` returns the granted QoS (v3.1.1) or reason codes (v5.0), one
//!   per requested filter, and fails with [`ClientError::SubscribeRejected`]
//!   when a filter is refused.
//! - Inbound messages are still delivered through the [`Event`] stream.
//! - When the connection ends, outstanding operations fail with
//!   [`ClientError::Disconnected`] instead of hanging.

mod client;
mod config;
mod error;
mod event;
mod inflight;
mod session;
mod transport;

pub use client::MqttClient;
pub use config::{
    ClientConfig, Credentials, LastWill, ProtocolVersion, ReconnectOptions, TlsOptions,
};
pub use error::ClientError;
pub use event::Event;
