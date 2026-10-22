//! MQTT listeners for a single Heron node.
//!
//! TCP speaks MQTT directly. WebSocket requires the `mqtt` subprotocol and
//! carries each encoded burst in a binary frame.

#![deny(clippy::unwrap_used)]

mod connection;
mod server;

pub use server::Server;
