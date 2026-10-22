//! MQTT session drivers (v3.1.1 and v5.0) with reconnect.

mod state;
mod v4;
mod v5;

use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time;

use crate::client::Command;
use crate::config::{ClientConfig, ProtocolVersion};
use crate::error::ClientError;
use crate::event::Event;
use crate::transport::{self, BrokerStream};
use state::SessionState;

/// How a single connection attempt ended.
#[derive(Debug)]
pub(crate) enum SessionOutcome {
    /// Client sent DISCONNECT (or command channel closed).
    GracefulDisconnect,
    /// TCP/TLS dropped or protocol error.
    ConnectionLost(Option<String>),
}

/// Instruction produced after handling an inbound control packet.
#[derive(Debug)]
pub(super) enum Flow {
    /// Keep the connection open.
    Continue,
    /// Terminate the connection, optionally with a reason.
    Close(Option<String>),
}

/// What the broker told us during the CONNECT handshake.
#[derive(Debug, Clone, Copy)]
pub(super) struct ConnAckInfo {
    /// Whether the broker restored a previous session.
    pub session_present: bool,
    /// Keep-alive to use: the broker may override ours (v5.0 Server Keep Alive).
    pub keep_alive: u16,
}

/// Drives the MQTT connection, reconnecting when configured.
pub(crate) async fn run_event_loop(
    config: ClientConfig,
    mut command_rx: mpsc::Receiver<Command>,
    event_tx: mpsc::Sender<Event>,
) -> Result<(), ClientError> {
    let mut state = SessionState::new(config.max_inflight);
    let mut attempt: u32 = 0;

    loop {
        if config.clean_session {
            state.clear_for_clean_start();
        }

        let stream = match connect_broker(&config, &event_tx).await {
            Ok(stream) => {
                attempt = 0;
                stream
            }
            Err(e) => {
                state.fail_pending();
                if !config.reconnect.enabled {
                    return Err(e);
                }
                attempt = attempt.saturating_add(1);
                if let Some(max) = config.reconnect.max_attempts
                    && attempt > max
                {
                    return Err(e);
                }
                let delay = backoff_delay(&config, attempt);
                let _ = event_tx
                    .send(Event::Reconnecting {
                        attempt,
                        delay_ms: delay.as_millis() as u64,
                    })
                    .await;
                if wait_reconnect_delay(delay, &mut command_rx, &mut state).await? {
                    emit_disconnected(&event_tx, None).await;
                    return Ok(());
                }
                continue;
            }
        };

        let outcome = match config.protocol_version {
            ProtocolVersion::V4 => {
                v4::run(stream, &config, &mut command_rx, &event_tx, &mut state).await
            }
            ProtocolVersion::V5 => {
                v5::run(stream, &config, &mut command_rx, &event_tx, &mut state).await
            }
        };

        // Operations awaiting an acknowledgement belong to the connection that
        // just ended; report them instead of leaving callers blocked.
        state.fail_pending();

        match outcome {
            Ok(SessionOutcome::GracefulDisconnect) => {
                emit_disconnected(&event_tx, None).await;
                return Ok(());
            }
            Ok(SessionOutcome::ConnectionLost(reason)) => {
                emit_disconnected(&event_tx, reason).await;
            }
            Err(e) => {
                emit_disconnected(&event_tx, Some(e.to_string())).await;
            }
        }

        if !config.reconnect.enabled {
            return Ok(());
        }

        attempt = attempt.saturating_add(1);
        if let Some(max) = config.reconnect.max_attempts
            && attempt > max
        {
            return Err(ClientError::Disconnected);
        }

        let delay = backoff_delay(&config, attempt);
        let _ = event_tx
            .send(Event::Reconnecting {
                attempt,
                delay_ms: delay.as_millis() as u64,
            })
            .await;

        if wait_reconnect_delay(delay, &mut command_rx, &mut state).await? {
            emit_disconnected(&event_tx, None).await;
            return Ok(());
        }
    }
}

async fn connect_broker(
    config: &ClientConfig,
    event_tx: &mpsc::Sender<Event>,
) -> Result<BrokerStream, ClientError> {
    match time::timeout(
        config.connect_timeout,
        transport::connect(&config.broker_addr, &config.tls),
    )
    .await
    {
        Ok(Ok(stream)) => Ok(stream),
        Ok(Err(e)) => {
            emit_disconnected(event_tx, Some(e.to_string())).await;
            Err(e)
        }
        Err(_) => {
            emit_disconnected(event_tx, Some("connection timed out".into())).await;
            Err(ClientError::Timeout)
        }
    }
}

/// Returns `true` if the client requested a graceful stop during the delay.
async fn wait_reconnect_delay(
    delay: Duration,
    command_rx: &mut mpsc::Receiver<Command>,
    state: &mut SessionState,
) -> Result<bool, ClientError> {
    let sleep = time::sleep(delay);
    tokio::pin!(sleep);

    loop {
        tokio::select! {
            () = &mut sleep => return Ok(false),
            cmd = command_rx.recv() => {
                match cmd {
                    Some(Command::Disconnect) | None => return Ok(true),
                    Some(other) => state.pending_commands.push_back(other),
                }
            }
        }
    }
}

fn backoff_delay(config: &ClientConfig, attempt: u32) -> Duration {
    let initial = config.reconnect.initial_delay;
    let max = config.reconnect.max_delay;
    let shift = attempt.saturating_sub(1).min(16);
    let factor = 1u32 << shift;
    let scaled = initial.saturating_mul(factor);
    if scaled > max { max } else { scaled }
}

/// Client identifier sent in CONNECT.
///
/// Prefers the configured id, then the id assigned by the broker (v5.0), and
/// falls back to a process-derived id.
pub(super) fn client_id(config: &ClientConfig, state: &SessionState) -> String {
    if !config.client_id.is_empty() {
        return config.client_id.clone();
    }
    if let Some(assigned) = &state.assigned_client_id {
        return assigned.clone();
    }
    format!("mqtt-{}", std::process::id())
}

pub(super) fn keep_alive_secs(keep_alive: u16) -> Duration {
    if keep_alive == 0 {
        Duration::MAX
    } else {
        Duration::from_secs(u64::from(keep_alive))
    }
}

/// Grace period before a silent broker is considered dead: 1.5 × keep alive.
pub(super) fn keep_alive_grace(keep_alive: u16) -> Duration {
    if keep_alive == 0 {
        Duration::MAX
    } else {
        Duration::from_millis(u64::from(keep_alive) * 1500)
    }
}

/// (Re)arm a deadline; `Duration::MAX` disables it.
pub(super) fn reset_deadline(deadline: &mut Option<time::Instant>, every: Duration) {
    if every == Duration::MAX {
        *deadline = None;
    } else {
        *deadline = Some(time::Instant::now() + every);
    }
}

/// Arm the PINGRESP deadline after sending a PINGREQ.
///
/// Only armed while a request is unanswered, so a client that keeps publishing
/// (and therefore never pings) is never declared dead. Any inbound packet
/// clears it.
pub(super) fn arm_pingresp_deadline(deadline: &mut Option<time::Instant>, grace: Duration) {
    if deadline.is_none() && grace != Duration::MAX {
        *deadline = Some(time::Instant::now() + grace);
    }
}

pub(super) async fn emit_disconnected(event_tx: &mpsc::Sender<Event>, reason: Option<String>) {
    let _ = event_tx.send(Event::Disconnected { reason }).await;
}
