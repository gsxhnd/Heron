//! MQTT client handle and event loop.

use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tokio::time;

use crate::config::ClientConfig;
use crate::error::ClientError;
use crate::event::Event;
use crate::session;

/// Reply channel for an operation that only reports success or failure.
pub(crate) type AckReply = oneshot::Sender<Result<(), ClientError>>;

/// Reply channel for SUBSCRIBE, which reports the granted QoS / reason codes.
pub(crate) type CodesReply = oneshot::Sender<Result<Vec<u8>, ClientError>>;

/// Commands sent from the client handle to the event loop.
///
/// Operations other than [`Command::Disconnect`] carry a oneshot reply channel
/// so the caller observes the broker's acknowledgement instead of guessing from
/// the event stream.
#[derive(Debug)]
pub(crate) enum Command {
    Publish {
        topic: String,
        payload: Bytes,
        qos: u8,
        retain: bool,
        reply: AckReply,
    },
    Subscribe {
        topics: Vec<(String, u8)>,
        reply: CodesReply,
    },
    Unsubscribe {
        topics: Vec<String>,
        reply: AckReply,
    },
    Disconnect,
}

/// The MQTT client handle.
///
/// This is cheaply cloneable and can be shared across tasks.
/// Publish / subscribe / unsubscribe wait for the broker acknowledgement
/// (bounded by [`ClientConfig::request_timeout`]); incoming traffic is still
/// delivered through the event channel.
#[derive(Debug, Clone)]
pub struct MqttClient {
    command_tx: mpsc::Sender<Command>,
    request_timeout: Duration,
}

impl MqttClient {
    /// Create a new MQTT client and start the event loop.
    ///
    /// Returns the client handle and a receiver for incoming events.
    /// The event loop runs as a background tokio task, so this must be called
    /// from within a tokio runtime.
    pub fn new(config: ClientConfig) -> (Self, mpsc::Receiver<Event>) {
        let request_timeout = config.request_timeout;
        let (command_tx, command_rx) = mpsc::channel(256);
        let (event_tx, event_rx) = mpsc::channel(256);

        tokio::spawn(async move {
            let _ = session::run_event_loop(config, command_rx, event_tx).await;
        });

        let client = Self {
            command_tx,
            request_timeout,
        };
        (client, event_rx)
    }

    /// Publish a message to the given topic.
    ///
    /// QoS 0 resolves once the packet is written to the socket; QoS 1 and 2
    /// resolve when the broker returns PUBACK / PUBCOMP.
    pub async fn publish(
        &self,
        topic: impl Into<String>,
        payload: impl Into<Bytes>,
        qos: u8,
        retain: bool,
    ) -> Result<(), ClientError> {
        if qos > 2 {
            return Err(ClientError::InvalidQoS(qos));
        }

        let (reply, rx) = oneshot::channel();
        self.command_tx
            .send(Command::Publish {
                topic: topic.into(),
                payload: payload.into(),
                qos,
                retain,
                reply,
            })
            .await
            .map_err(|_| ClientError::ChannelClosed)?;

        self.await_reply(rx).await
    }

    /// Subscribe to one or more topic filters.
    ///
    /// Resolves with the granted QoS (v3.1.1) or reason codes (v5.0) — one entry
    /// per requested filter. Returns [`ClientError::SubscribeRejected`] when any
    /// filter is refused by the broker.
    pub async fn subscribe(&self, topics: Vec<(String, u8)>) -> Result<Vec<u8>, ClientError> {
        for (_, qos) in &topics {
            if *qos > 2 {
                return Err(ClientError::InvalidQoS(*qos));
            }
        }

        let (reply, rx) = oneshot::channel();
        self.command_tx
            .send(Command::Subscribe { topics, reply })
            .await
            .map_err(|_| ClientError::ChannelClosed)?;

        self.await_reply(rx).await
    }

    /// Unsubscribe from one or more topic filters.
    ///
    /// Resolves when the broker returns UNSUBACK.
    pub async fn unsubscribe(&self, topics: Vec<String>) -> Result<(), ClientError> {
        let (reply, rx) = oneshot::channel();
        self.command_tx
            .send(Command::Unsubscribe { topics, reply })
            .await
            .map_err(|_| ClientError::ChannelClosed)?;

        self.await_reply(rx).await
    }

    /// Gracefully disconnect from the broker.
    pub async fn disconnect(&self) -> Result<(), ClientError> {
        self.command_tx
            .send(Command::Disconnect)
            .await
            .map_err(|_| ClientError::ChannelClosed)
    }

    async fn await_reply<T>(
        &self,
        rx: oneshot::Receiver<Result<T, ClientError>>,
    ) -> Result<T, ClientError> {
        match time::timeout(self.request_timeout, rx).await {
            Ok(Ok(result)) => result,
            // Event loop terminated before answering (disconnect / channel closed).
            Ok(Err(_)) => Err(ClientError::Disconnected),
            Err(_) => Err(ClientError::Timeout),
        }
    }
}
