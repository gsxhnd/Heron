//! Process-local [`Storage`](crate::Storage) used by tests and the default broker.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::error::CoreError;
use crate::storage::{
    Direction, Storage, StoredClient, StoredMessage, StoredRetained, StoredSubscription, WriteOp,
};

#[derive(Default)]
struct MemoryInner {
    clients: HashMap<String, StoredClient>,
    subscriptions: HashMap<(String, String), StoredSubscription>,
    retained: HashMap<String, StoredRetained>,
    messages: HashMap<(String, u16, Direction), StoredMessage>,
}

/// In-memory storage. State survives for as long as this value is kept alive.
#[derive(Default)]
pub struct MemoryStorage {
    inner: Mutex<MemoryInner>,
}

impl MemoryStorage {
    /// Empty storage.
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MemoryInner> {
        self.inner.lock().unwrap_or_else(|err| err.into_inner())
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    async fn load_clients(&self) -> Result<Vec<StoredClient>, CoreError> {
        Ok(self.lock().clients.values().cloned().collect())
    }

    async fn load_subscriptions(&self) -> Result<Vec<StoredSubscription>, CoreError> {
        Ok(self.lock().subscriptions.values().cloned().collect())
    }

    async fn load_retained(&self) -> Result<Vec<StoredRetained>, CoreError> {
        Ok(self.lock().retained.values().cloned().collect())
    }

    async fn load_messages(&self) -> Result<Vec<StoredMessage>, CoreError> {
        Ok(self.lock().messages.values().cloned().collect())
    }

    async fn apply(&self, op: &WriteOp) -> Result<(), CoreError> {
        let mut guard = self.lock();
        match op {
            WriteOp::UpsertClient(client) => {
                guard
                    .clients
                    .insert(client.client_id.clone(), client.clone());
            }
            WriteOp::DeleteClient(client_id) => {
                guard.clients.remove(client_id);
                guard.subscriptions.retain(|(id, _), _| id != client_id);
                guard.messages.retain(|(id, _, _), _| id != client_id);
            }
            WriteOp::UpsertSubscription(subscription) => {
                guard.subscriptions.insert(
                    (subscription.client_id.clone(), subscription.filter.clone()),
                    subscription.clone(),
                );
            }
            WriteOp::DeleteSubscription { client_id, filter } => {
                guard
                    .subscriptions
                    .remove(&(client_id.clone(), filter.clone()));
            }
            WriteOp::ClearSubscriptions(client_id) => {
                guard.subscriptions.retain(|(id, _), _| id != client_id);
            }
            WriteOp::UpsertRetained(message) => {
                guard
                    .retained
                    .insert(message.topic.clone(), message.clone());
            }
            WriteOp::DeleteRetained(topic) => {
                guard.retained.remove(topic);
            }
            WriteOp::UpsertMessage(message) => {
                guard.messages.insert(
                    (
                        message.client_id.clone(),
                        message.packet_id,
                        message.direction,
                    ),
                    message.clone(),
                );
            }
            WriteOp::DeleteMessage {
                client_id,
                packet_id,
                direction,
            } => {
                guard
                    .messages
                    .remove(&(client_id.clone(), *packet_id, *direction));
            }
            WriteOp::ClearMessages(client_id) => {
                guard.messages.retain(|(id, _, _), _| id != client_id);
            }
        }
        Ok(())
    }
}
