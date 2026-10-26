//! SeaORM-backed [`Storage`](heron_core::Storage).

use std::collections::HashMap;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, Utc};
use heron_core::{
    CoreError, Direction, MessageState, ProtocolVersion, QoS, Storage, StoredClient, StoredMessage,
    StoredRetained, StoredRule, StoredSubscription, Will, WriteOp,
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, ConnectionTrait, Database, DatabaseConnection,
    EntityTrait, QueryFilter, Set,
};

use crate::entity::{client, message, retained, rule, session, subscription};
use crate::schema::SCHEMA;

/// PostgreSQL storage using SeaORM.
#[derive(Clone)]
pub struct PostgresStorage {
    db: DatabaseConnection,
}

impl PostgresStorage {
    /// Connect and create tables when they are missing.
    pub async fn connect(url: &str) -> Result<Self, CoreError> {
        let db = Database::connect(url)
            .await
            .map_err(|err| CoreError::Storage(err.to_string()))?;
        let storage = Self { db };
        storage.migrate().await?;
        Ok(storage)
    }

    async fn migrate(&self) -> Result<(), CoreError> {
        for statement in SCHEMA {
            self.db
                .execute_unprepared(statement)
                .await
                .map_err(|err| CoreError::Storage(err.to_string()))?;
        }
        Ok(())
    }
}

#[async_trait]
impl Storage for PostgresStorage {
    async fn load_clients(&self) -> Result<Vec<StoredClient>, CoreError> {
        let clients = client::Entity::find().all(&self.db).await.map_err(db_err)?;
        let sessions = session::Entity::find()
            .all(&self.db)
            .await
            .map_err(db_err)?;
        let by_client: HashMap<_, _> = sessions
            .into_iter()
            .map(|row| (row.client_id.clone(), row))
            .collect();
        Ok(clients
            .into_iter()
            .map(|row| stored_client(&row, by_client.get(&row.client_id)))
            .collect())
    }

    async fn load_subscriptions(&self) -> Result<Vec<StoredSubscription>, CoreError> {
        let rows = subscription::Entity::find()
            .all(&self.db)
            .await
            .map_err(db_err)?;
        Ok(rows.into_iter().filter_map(stored_subscription).collect())
    }

    async fn load_retained(&self) -> Result<Vec<StoredRetained>, CoreError> {
        let rows = retained::Entity::find()
            .all(&self.db)
            .await
            .map_err(db_err)?;
        Ok(rows.into_iter().map(stored_retained).collect())
    }

    async fn load_messages(&self) -> Result<Vec<StoredMessage>, CoreError> {
        let rows = message::Entity::find()
            .all(&self.db)
            .await
            .map_err(db_err)?;
        Ok(rows.into_iter().filter_map(stored_message).collect())
    }

    async fn load_rules(&self) -> Result<Vec<StoredRule>, CoreError> {
        let rows = rule::Entity::find().all(&self.db).await.map_err(db_err)?;
        Ok(rows.into_iter().map(stored_rule).collect())
    }

    async fn apply(&self, op: &WriteOp) -> Result<(), CoreError> {
        match op {
            WriteOp::UpsertClient(client) => self.upsert_client(client).await,
            WriteOp::DeleteClient(client_id) => {
                client::Entity::delete_many()
                    .filter(client::Column::ClientId.eq(client_id))
                    .exec(&self.db)
                    .await
                    .map_err(db_err)?;
                Ok(())
            }
            WriteOp::UpsertSubscription(row) => self.upsert_subscription(row).await,
            WriteOp::DeleteSubscription { client_id, filter } => {
                subscription::Entity::delete_many()
                    .filter(
                        Condition::all()
                            .add(subscription::Column::ClientId.eq(client_id))
                            .add(subscription::Column::Topic.eq(filter)),
                    )
                    .exec(&self.db)
                    .await
                    .map_err(db_err)?;
                Ok(())
            }
            WriteOp::ClearSubscriptions(client_id) => {
                subscription::Entity::delete_many()
                    .filter(subscription::Column::ClientId.eq(client_id))
                    .exec(&self.db)
                    .await
                    .map_err(db_err)?;
                Ok(())
            }
            WriteOp::UpsertRetained(row) => self.upsert_retained(row).await,
            WriteOp::DeleteRetained(topic) => {
                retained::Entity::delete_many()
                    .filter(retained::Column::Topic.eq(topic))
                    .exec(&self.db)
                    .await
                    .map_err(db_err)?;
                Ok(())
            }
            WriteOp::UpsertMessage(row) => self.upsert_message(row).await,
            WriteOp::DeleteMessage {
                client_id,
                packet_id,
                direction,
            } => {
                message::Entity::delete_many()
                    .filter(
                        Condition::all()
                            .add(message::Column::ClientId.eq(client_id))
                            .add(message::Column::PacketId.eq(i32::from(*packet_id)))
                            .add(message::Column::Direction.eq(direction.as_str())),
                    )
                    .exec(&self.db)
                    .await
                    .map_err(db_err)?;
                Ok(())
            }
            WriteOp::ClearMessages(client_id) => {
                message::Entity::delete_many()
                    .filter(message::Column::ClientId.eq(client_id))
                    .exec(&self.db)
                    .await
                    .map_err(db_err)?;
                Ok(())
            }
            WriteOp::UpsertRule(rule) => self.upsert_rule(rule).await,
            WriteOp::DeleteRule(rule_id) => {
                rule::Entity::delete_many()
                    .filter(rule::Column::Id.eq(i64::try_from(*rule_id).unwrap_or(0)))
                    .exec(&self.db)
                    .await
                    .map_err(db_err)?;
                Ok(())
            }
        }
    }
}

impl PostgresStorage {
    async fn upsert_client(&self, client: &StoredClient) -> Result<(), CoreError> {
        let now = Utc::now().naive_utc();
        let (will_topic, will_qos, will_retain, will_payload, will_delay) = match &client.will {
            Some(will) => (
                Some(will.topic.clone()),
                Some(i16::from(will.qos.as_u8())),
                Some(will.retain),
                Some(will.payload.to_vec()),
                Some(i32::try_from(will.delay_secs).unwrap_or(i32::MAX)),
            ),
            None => (None, None, None, None, None),
        };
        let existing = client::Entity::find()
            .filter(client::Column::ClientId.eq(&client.client_id))
            .one(&self.db)
            .await
            .map_err(db_err)?;
        if let Some(existing) = existing {
            let mut active: client::ActiveModel = existing.into();
            active.protocol_version = Set(protocol_level(client.protocol));
            active.ip_address = Set(client.peer.clone());
            active.connected = Set(client.connected);
            active.connected_at = Set(client.connected.then_some(now));
            active.disconnected_at = Set(client.disconnected_at_ms.and_then(ms_to_naive));
            active.keep_alive = Set(Some(i32::from(client.keep_alive)));
            active.clean_session = Set(Some(client.clean_start));
            active.will_topic = Set(will_topic);
            active.will_qos = Set(will_qos);
            active.will_retain = Set(will_retain);
            active.will_payload = Set(will_payload);
            active.will_delay = Set(will_delay);
            active.updated_at = Set(now);
            active.update(&self.db).await.map_err(db_err)?;
        } else {
            client::ActiveModel {
                client_id: Set(client.client_id.clone()),
                protocol_version: Set(protocol_level(client.protocol)),
                ip_address: Set(client.peer.clone()),
                connected: Set(client.connected),
                connected_at: Set(client.connected.then_some(now)),
                disconnected_at: Set(client.disconnected_at_ms.and_then(ms_to_naive)),
                keep_alive: Set(Some(i32::from(client.keep_alive))),
                clean_session: Set(Some(client.clean_start)),
                will_topic: Set(will_topic),
                will_qos: Set(will_qos),
                will_retain: Set(will_retain),
                will_payload: Set(will_payload),
                will_delay: Set(will_delay),
                created_at: Set(now),
                updated_at: Set(now),
                ..Default::default()
            }
            .insert(&self.db)
            .await
            .map_err(db_err)?;
        }

        let existing = session::Entity::find()
            .filter(session::Column::ClientId.eq(&client.client_id))
            .one(&self.db)
            .await
            .map_err(db_err)?;
        let expiry = client
            .session_expiry
            .and_then(|value| i32::try_from(value).ok());
        if let Some(existing) = existing {
            let mut active: session::ActiveModel = existing.into();
            active.clean_session = Set(client.clean_start);
            active.session_expiry_interval = Set(expiry);
            active.updated_at = Set(now);
            active.update(&self.db).await.map_err(db_err)?;
        } else {
            session::ActiveModel {
                client_id: Set(client.client_id.clone()),
                clean_session: Set(client.clean_start),
                session_expiry_interval: Set(expiry),
                created_at: Set(now),
                updated_at: Set(now),
                ..Default::default()
            }
            .insert(&self.db)
            .await
            .map_err(db_err)?;
        }
        Ok(())
    }

    async fn upsert_subscription(&self, row: &StoredSubscription) -> Result<(), CoreError> {
        let existing = subscription::Entity::find()
            .filter(
                Condition::all()
                    .add(subscription::Column::ClientId.eq(&row.client_id))
                    .add(subscription::Column::Topic.eq(&row.filter)),
            )
            .one(&self.db)
            .await
            .map_err(db_err)?;
        if let Some(existing) = existing {
            let mut active: subscription::ActiveModel = existing.into();
            active.qos = Set(i16::from(row.qos.as_u8()));
            active.no_local = Set(row.no_local);
            active.retain_as_published = Set(row.retain_as_published);
            active.retain_handling = Set(i16::from(row.retain_handling));
            active.update(&self.db).await.map_err(db_err)?;
        } else {
            subscription::ActiveModel {
                client_id: Set(row.client_id.clone()),
                topic: Set(row.filter.clone()),
                qos: Set(i16::from(row.qos.as_u8())),
                no_local: Set(row.no_local),
                retain_as_published: Set(row.retain_as_published),
                retain_handling: Set(i16::from(row.retain_handling)),
                created_at: Set(Utc::now().naive_utc()),
                ..Default::default()
            }
            .insert(&self.db)
            .await
            .map_err(db_err)?;
        }
        Ok(())
    }

    async fn upsert_retained(&self, row: &StoredRetained) -> Result<(), CoreError> {
        let now = Utc::now().naive_utc();
        let size = i32::try_from(row.payload.len()).ok();
        let existing = retained::Entity::find()
            .filter(retained::Column::Topic.eq(&row.topic))
            .one(&self.db)
            .await
            .map_err(db_err)?;
        if let Some(existing) = existing {
            let mut active: retained::ActiveModel = existing.into();
            active.qos = Set(i16::from(row.qos.as_u8()));
            active.payload = Set(Some(row.payload.to_vec()));
            active.payload_size = Set(size);
            active.updated_at = Set(now);
            active.update(&self.db).await.map_err(db_err)?;
        } else {
            retained::ActiveModel {
                topic: Set(row.topic.clone()),
                qos: Set(i16::from(row.qos.as_u8())),
                payload: Set(Some(row.payload.to_vec())),
                payload_size: Set(size),
                created_at: Set(now),
                updated_at: Set(now),
                ..Default::default()
            }
            .insert(&self.db)
            .await
            .map_err(db_err)?;
        }
        Ok(())
    }

    async fn upsert_rule(&self, row: &StoredRule) -> Result<(), CoreError> {
        let now = Utc::now().naive_utc();
        let id = i64::try_from(row.id).unwrap_or(0);
        let existing = rule::Entity::find_by_id(id)
            .one(&self.db)
            .await
            .map_err(db_err)?;
        let conditions = match row.conditions.as_str() {
            "" | "null" => None,
            other => Some(other.to_owned()),
        };
        if let Some(existing) = existing {
            let mut active: rule::ActiveModel = existing.into();
            active.name = Set(row.name.clone());
            active.description = Set(row.description.clone());
            active.enabled = Set(row.enabled);
            active.priority = Set(row.priority);
            active.topic_filter = Set(row.topic_filter.clone());
            active.conditions = Set(conditions);
            active.actions = Set(row.actions.clone());
            active.updated_at = Set(now);
            active.update(&self.db).await.map_err(db_err)?;
        } else {
            rule::ActiveModel {
                id: Set(id),
                name: Set(row.name.clone()),
                description: Set(row.description.clone()),
                enabled: Set(row.enabled),
                priority: Set(row.priority),
                topic_filter: Set(row.topic_filter.clone()),
                conditions: Set(conditions),
                actions: Set(row.actions.clone()),
                created_at: Set(parse_rfc3339(&row.created_at).unwrap_or(now)),
                updated_at: Set(now),
            }
            .insert(&self.db)
            .await
            .map_err(db_err)?;
        }
        Ok(())
    }

    async fn upsert_message(&self, row: &StoredMessage) -> Result<(), CoreError> {
        let now = Utc::now().naive_utc();
        let size = i32::try_from(row.payload.len()).ok();
        let existing = message::Entity::find()
            .filter(
                Condition::all()
                    .add(message::Column::ClientId.eq(&row.client_id))
                    .add(message::Column::PacketId.eq(i32::from(row.packet_id)))
                    .add(message::Column::Direction.eq(row.direction.as_str())),
            )
            .one(&self.db)
            .await
            .map_err(db_err)?;
        if let Some(existing) = existing {
            let mut active: message::ActiveModel = existing.into();
            active.topic = Set(row.topic.clone());
            active.qos = Set(i16::from(row.qos.as_u8()));
            active.payload = Set(Some(row.payload.to_vec()));
            active.payload_size = Set(size);
            active.retain = Set(row.retain);
            active.state = Set(row.state.as_str().to_owned());
            active.updated_at = Set(now);
            active.update(&self.db).await.map_err(db_err)?;
        } else {
            message::ActiveModel {
                client_id: Set(row.client_id.clone()),
                packet_id: Set(i32::from(row.packet_id)),
                topic: Set(row.topic.clone()),
                qos: Set(i16::from(row.qos.as_u8())),
                payload: Set(Some(row.payload.to_vec())),
                payload_size: Set(size),
                retain: Set(row.retain),
                dup: Set(false),
                direction: Set(row.direction.as_str().to_owned()),
                state: Set(row.state.as_str().to_owned()),
                created_at: Set(now),
                updated_at: Set(now),
                ..Default::default()
            }
            .insert(&self.db)
            .await
            .map_err(db_err)?;
        }
        Ok(())
    }
}

fn stored_client(row: &client::Model, session: Option<&session::Model>) -> StoredClient {
    let will = row.will_topic.clone().map(|topic| Will {
        topic,
        payload: Bytes::from(row.will_payload.clone().unwrap_or_default()),
        qos: qos_from_i16(row.will_qos.unwrap_or(0)),
        retain: row.will_retain.unwrap_or(false),
        delay_secs: u32::try_from(row.will_delay.unwrap_or(0)).unwrap_or(0),
    });
    StoredClient {
        client_id: row.client_id.clone(),
        protocol: protocol_from_level(row.protocol_version),
        connected: row.connected,
        peer: row.ip_address.clone(),
        keep_alive: u16::try_from(row.keep_alive.unwrap_or(0)).unwrap_or(0),
        clean_start: session
            .map(|session| session.clean_session)
            .or(row.clean_session)
            .unwrap_or(true),
        session_expiry: session.and_then(|session| {
            session
                .session_expiry_interval
                .and_then(|value| u32::try_from(value).ok())
        }),
        will,
        disconnected_at_ms: row.disconnected_at.map(naive_to_ms),
    }
}

fn stored_subscription(row: subscription::Model) -> Option<StoredSubscription> {
    Some(StoredSubscription {
        client_id: row.client_id,
        filter: row.topic,
        qos: qos_from_i16(row.qos),
        no_local: row.no_local,
        retain_as_published: row.retain_as_published,
        retain_handling: u8::try_from(row.retain_handling).unwrap_or(0),
    })
}

fn stored_retained(row: retained::Model) -> StoredRetained {
    StoredRetained {
        topic: row.topic,
        payload: Bytes::from(row.payload.unwrap_or_default()),
        qos: qos_from_i16(row.qos),
    }
}

fn stored_rule(row: rule::Model) -> StoredRule {
    StoredRule {
        id: u64::try_from(row.id).unwrap_or(0),
        name: row.name,
        description: row.description,
        enabled: row.enabled,
        priority: row.priority,
        topic_filter: row.topic_filter,
        conditions: row.conditions.unwrap_or_else(|| "null".to_owned()),
        actions: row.actions,
        created_at: row
            .created_at
            .and_utc()
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        updated_at: row
            .updated_at
            .and_utc()
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    }
}

fn parse_rfc3339(text: &str) -> Option<NaiveDateTime> {
    DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|time| time.with_timezone(&Utc).naive_utc())
}

fn stored_message(row: message::Model) -> Option<StoredMessage> {
    let packet_id = u16::try_from(row.packet_id).ok()?;
    Some(StoredMessage {
        client_id: row.client_id,
        packet_id,
        topic: row.topic,
        payload: Bytes::from(row.payload.unwrap_or_default()),
        qos: qos_from_i16(row.qos),
        retain: row.retain,
        direction: Direction::parse(&row.direction)?,
        state: MessageState::parse(&row.state)?,
    })
}

fn protocol_level(protocol: ProtocolVersion) -> i16 {
    i16::from(protocol.level())
}

fn protocol_from_level(level: i16) -> ProtocolVersion {
    if level == 5 {
        ProtocolVersion::V5
    } else {
        ProtocolVersion::V4
    }
}

fn qos_from_i16(value: i16) -> QoS {
    u8::try_from(value)
        .ok()
        .and_then(|value| QoS::try_from(value).ok())
        .unwrap_or(QoS::AtMostOnce)
}

fn ms_to_naive(ms: i64) -> Option<NaiveDateTime> {
    let secs = ms.div_euclid(1000);
    let nanos = u32::try_from(ms.rem_euclid(1000)).ok()? * 1_000_000;
    DateTime::from_timestamp(secs, nanos).map(|time| time.naive_utc())
}

fn naive_to_ms(time: NaiveDateTime) -> i64 {
    time.and_utc().timestamp_millis()
}

fn db_err(err: sea_orm::DbErr) -> CoreError {
    CoreError::Storage(err.to_string())
}
