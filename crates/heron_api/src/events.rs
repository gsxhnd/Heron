//! WebSocket snapshots for the management UI.
//!
//! `GET /api/v1/events` pushes a status snapshot once a second, then client
//! connect and disconnect notices after that baseline. Browsers cannot set
//! `Authorization` on a WebSocket, so this route also accepts `?token=`.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::Response;
use heron_core::Broker;
use serde_json::json;

use crate::AppState;

/// Upgrade an authenticated request and stream broker snapshots.
pub(crate) async fn events(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    let broker = state.broker.clone();
    ws.on_upgrade(move |socket| stream_events(socket, broker))
}

async fn stream_events(mut socket: WebSocket, broker: Broker) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut previous: Option<Counters> = None;
    let mut baseline: Option<HashMap<String, u8>> = None;

    loop {
        interval.tick().await;
        let now = Instant::now();
        let status = broker.status();
        let (receive_per_sec, send_per_sec) = match previous {
            Some(sample) => {
                let elapsed = now.saturating_duration_since(sample.at).as_secs_f64();
                (
                    per_sec(sample.received, status.messages_received, elapsed),
                    per_sec(sample.sent, status.messages_sent, elapsed),
                )
            }
            None => (0.0, 0.0),
        };
        previous = Some(Counters {
            at: now,
            received: status.messages_received,
            sent: status.messages_sent,
        });

        let snapshot = json!({
            "type": "snapshot",
            "uptime": status.uptime_secs,
            "connections": status.connections,
            "sessions": status.sessions,
            "messages_received": status.messages_received,
            "messages_sent": status.messages_sent,
            "receive_per_sec": receive_per_sec,
            "send_per_sec": send_per_sec,
            "topics": broker.list_topics().len(),
            "subscriptions": broker.list_subscriptions().len(),
        });
        if send_json(&mut socket, &snapshot).await.is_err() {
            break;
        }

        let current = connected_clients(&broker);
        if let Some(seen) = baseline.as_ref() {
            for (client_id, protocol_version) in &current {
                if seen.contains_key(client_id) {
                    continue;
                }
                let event = json!({
                    "type": "client",
                    "action": "connected",
                    "client_id": client_id,
                    "protocol_version": protocol_version,
                });
                if send_json(&mut socket, &event).await.is_err() {
                    return;
                }
            }
            for client_id in seen.keys() {
                if current.contains_key(client_id) {
                    continue;
                }
                let event = json!({
                    "type": "client",
                    "action": "disconnected",
                    "client_id": client_id,
                });
                if send_json(&mut socket, &event).await.is_err() {
                    return;
                }
            }
        }
        baseline = Some(current);
    }
}

struct Counters {
    at: Instant,
    received: u64,
    sent: u64,
}

fn per_sec(previous: u64, current: u64, elapsed: f64) -> f64 {
    if elapsed <= f64::EPSILON {
        return 0.0;
    }
    let delta = current.saturating_sub(previous) as f64;
    (delta / elapsed * 100.0).round() / 100.0
}

fn connected_clients(broker: &Broker) -> HashMap<String, u8> {
    let mut found = HashMap::new();
    let mut page = 1usize;
    loop {
        let (total, clients) = broker.list_clients(Some(true), None, None, page, 200);
        if clients.is_empty() {
            break;
        }
        for client in clients {
            found.insert(client.client_id, client.protocol_version);
        }
        if found.len() >= total || page >= 10_000 {
            break;
        }
        page += 1;
    }
    found
}

async fn send_json(socket: &mut WebSocket, value: &serde_json::Value) -> Result<(), ()> {
    let Ok(text) = serde_json::to_string(value) else {
        return Err(());
    };
    socket
        .send(Message::Text(text.into()))
        .await
        .map_err(|_| ())
}
