//! Lifecycle callbacks invoked around session and publish operations.
//!
//! The default implementation accepts every event and forwards every publish.
//! [`crate::Broker::install_hooks`] replaces it. Hook futures run outside the
//! broker lock so they can perform I/O without stalling other sessions.

use bytes::Bytes;

/// A hook refused a connect or subscribe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookReject {
    /// Operator-facing reason. It is logged and not sent to the MQTT client.
    pub reason: String,
}

/// CONNECT has passed authentication and will-topic authorization.
#[derive(Debug, Clone)]
pub struct ConnectEvent {
    /// Client identifier, including one the server is about to assign.
    pub client_id: String,
    /// Authenticated username. `None` is anonymous.
    pub username: Option<String>,
    /// Peer address captured by the listener.
    pub peer: Option<String>,
}

/// A live connection is about to be marked disconnected.
#[derive(Debug, Clone)]
pub struct DisconnectEvent {
    /// Client identifier.
    pub client_id: String,
    /// Authenticated username.
    pub username: Option<String>,
    /// Peer address captured at CONNECT.
    pub peer: Option<String>,
}

/// A publication that is about to be routed to subscribers.
#[derive(Debug, Clone)]
pub struct PublishEvent {
    /// Publisher client id, `$heron` for the management API, or the will's owner.
    pub client_id: String,
    /// Authenticated username when the publisher has a session.
    pub username: Option<String>,
    /// Topic name.
    pub topic: String,
    /// Payload bytes.
    pub payload: Bytes,
    /// QoS 0, 1, or 2.
    pub qos: u8,
    /// Retain flag from the publisher.
    pub retain: bool,
}

/// One SUBSCRIBE filter that has already passed ACL.
#[derive(Debug, Clone)]
pub struct SubscribeEvent {
    /// Subscriber.
    pub client_id: String,
    /// Authenticated username.
    pub username: Option<String>,
    /// Topic filter from SUBSCRIBE.
    pub filter: String,
}

/// A filter that was actually installed and is now removed.
#[derive(Debug, Clone)]
pub struct UnsubscribeEvent {
    /// Subscriber.
    pub client_id: String,
    /// Authenticated username.
    pub username: Option<String>,
    /// Topic filter that was removed.
    pub filter: String,
}

/// Extra publication produced while handling a publish. It is routed once and
/// does not re-enter hooks.
#[derive(Debug, Clone)]
pub struct HookPublish {
    /// Destination topic name.
    pub topic: String,
    /// Payload.
    pub payload: Bytes,
    /// QoS 0, 1, or 2.
    pub qos: u8,
    /// Retain flag for the extra publication.
    pub retain: bool,
}

/// What to do with the publication that triggered the hook.
#[derive(Debug, Clone)]
pub struct PublishDecision {
    /// When false, current subscribers do not receive this publication.
    pub forward: bool,
    /// Replacement payload used when `forward` is true.
    pub payload: Option<Bytes>,
    /// Additional publications routed after the original decision.
    pub republish: Vec<HookPublish>,
}

impl PublishDecision {
    /// Forward the original payload and add no extra publications.
    pub fn forward() -> Self {
        Self {
            forward: true,
            payload: None,
            republish: Vec::new(),
        }
    }
}

/// Broker lifecycle extension point.
#[async_trait::async_trait]
pub trait BrokerHooks: Send + Sync {
    /// Called after authentication and before the session is attached.
    ///
    /// `Ok` publications are routed without re-entering hooks. `Err` refuses CONNECT.
    async fn on_connect(&self, _event: ConnectEvent) -> Result<Vec<HookPublish>, HookReject> {
        Ok(Vec::new())
    }

    /// Called before disconnect mutates the session. Returned publications are routed
    /// without re-entering hooks.
    async fn on_disconnect(&self, _event: DisconnectEvent) -> Vec<HookPublish> {
        Vec::new()
    }

    /// Called before the publication is fanned out. QoS 2 runs this on PUBREL.
    async fn on_publish(&self, _event: PublishEvent) -> PublishDecision {
        PublishDecision::forward()
    }

    /// Called before one filter is installed.
    async fn on_subscribe(&self, _event: SubscribeEvent) -> Result<Vec<HookPublish>, HookReject> {
        Ok(Vec::new())
    }

    /// Called after one filter has been removed.
    async fn on_unsubscribe(&self, _event: UnsubscribeEvent) -> Vec<HookPublish> {
        Vec::new()
    }
}

/// Accepts every event and forwards every publication.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopHooks;

#[async_trait::async_trait]
impl BrokerHooks for NoopHooks {}
