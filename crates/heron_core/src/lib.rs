//! Broker core for a single Heron node.
//!
//! Listeners accept TCP or WebSocket connections and drive this crate. The hot
//! path keeps sessions and the topic tree in memory. [`Storage`] is write-through
//! so sessions, subscriptions, retained messages, and QoS 1/2 inflight survive
//! a process restart.

#![deny(clippy::unwrap_used)]

mod broker;
mod error;
mod memory;
mod message;
mod qos;
mod storage;
mod topic;

pub use broker::{Broker, BrokerConfig};
pub use error::{
    ClientFlow, ConnectError, CoreError, PublishError, SubscribeAck, SubscribeFailure,
};
pub use memory::MemoryStorage;
pub use message::{
    AppMessage, ClientAck, ConnectRequest, ConnectedSession, DisconnectRequest, IncomingPublish,
    Outbound, PollEvent, ProtocolVersion, SubOpts, SubscribeFilter, Will,
};
pub use qos::QoS;
pub use storage::{
    Direction, MessageState, Storage, StoredClient, StoredMessage, StoredRetained,
    StoredSubscription, WriteOp,
};
pub use topic::{topic_matches, valid_topic_name};
