//! MQTT listeners for a single Heron node.
//!
//! TCP speaks MQTT directly. WebSocket requires the `mqtt` subprotocol and
//! carries each encoded burst in a binary frame.

#![deny(clippy::unwrap_used)]

mod connection;
mod server;
mod tls;

pub use server::Server;
pub use tls::{ClientCertMode, acceptor_from_pem, certificate_identity};
pub use tokio_rustls::TlsAcceptor;
