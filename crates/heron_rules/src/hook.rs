//! Hook plugins invoked before the built-in rules.

use bytes::Bytes;

use crate::error::HookError;

/// Which broker lifecycle point produced a [`HookContext`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookEvent {
    /// CONNECT was accepted by authentication.
    Connect,
    /// A live connection is ending.
    Disconnect,
    /// A client or internal publisher produced a message.
    Publish,
    /// A subscription filter is about to be installed.
    Subscribe,
    /// A subscription filter was removed.
    Unsubscribe,
}

impl HookEvent {
    /// Stable name used by SQL conditions (`event = 'publish'`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::Disconnect => "disconnect",
            Self::Publish => "publish",
            Self::Subscribe => "subscribe",
            Self::Unsubscribe => "unsubscribe",
        }
    }

    /// Topic filter target for events that are not MQTT publishes.
    pub fn topic(self) -> Option<&'static str> {
        match self {
            Self::Connect => Some("$events/client_connected"),
            Self::Disconnect => Some("$events/client_disconnected"),
            Self::Publish => None,
            Self::Subscribe => Some("$events/client_subscribed"),
            Self::Unsubscribe => Some("$events/client_unsubscribed"),
        }
    }
}

/// Fields a hook or rule can read.
#[derive(Debug, Clone)]
pub struct HookContext {
    /// Lifecycle point.
    pub event: HookEvent,
    /// Client identifier. Management publishes use `$heron`.
    pub client_id: String,
    /// Authenticated username.
    pub username: Option<String>,
    /// MQTT topic, or an `$events/...` topic for lifecycle events.
    pub topic: String,
    /// MQTT payload, or a JSON object for lifecycle events.
    pub payload: Bytes,
    /// QoS. Lifecycle events use 0.
    pub qos: u8,
    /// Retain flag. Lifecycle events use false.
    pub retain: bool,
    /// Peer address when the listener captured one.
    pub peer: Option<String>,
    /// Subscription filter for subscribe and unsubscribe events.
    pub filter: Option<String>,
}

/// Extension invoked on broker lifecycle events.
///
/// `Err(HookError::Reject)` refuses connect, subscribe, or the current publish.
/// `Retry` and `Internal` are logged and do not stop the broker.
#[async_trait::async_trait]
pub trait Hook: Send + Sync {
    /// CONNECT passed authentication.
    async fn on_connect(&self, _ctx: &HookContext) -> Result<(), HookError> {
        Ok(())
    }

    /// A connection is ending.
    async fn on_disconnect(&self, _ctx: &HookContext) -> Result<(), HookError> {
        Ok(())
    }

    /// A publication is about to be routed.
    async fn on_publish(&self, _ctx: &HookContext) -> Result<(), HookError> {
        Ok(())
    }

    /// A subscription filter passed ACL.
    async fn on_subscribe(&self, _ctx: &HookContext) -> Result<(), HookError> {
        Ok(())
    }

    /// A subscription filter was removed.
    async fn on_unsubscribe(&self, _ctx: &HookContext) -> Result<(), HookError> {
        Ok(())
    }
}
