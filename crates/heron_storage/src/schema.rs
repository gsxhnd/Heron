//! PostgreSQL schema for the v0.2.0 broker tables.
//!
//! `ip_address` is text rather than `INET` so the SeaORM model stays a `String`.
//! Subscription rows carry the MQTT v5 options the in-memory session needs after a restart.
//! `messages.direction` separates outbound inflight from inbound QoS 2 handshakes.

pub const SCHEMA: &[&str] = &[
    r#"
    CREATE TABLE IF NOT EXISTS clients (
        id BIGSERIAL PRIMARY KEY,
        client_id VARCHAR(255) NOT NULL UNIQUE,
        protocol_version SMALLINT NOT NULL,
        ip_address VARCHAR(128),
        port INTEGER,
        connected BOOLEAN NOT NULL DEFAULT FALSE,
        connected_at TIMESTAMP,
        disconnected_at TIMESTAMP,
        keep_alive INTEGER,
        clean_session BOOLEAN,
        will_topic VARCHAR(512),
        will_qos SMALLINT,
        will_retain BOOLEAN,
        will_payload BYTEA,
        will_delay INTEGER,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    "#,
    "CREATE INDEX IF NOT EXISTS idx_clients_client_id ON clients(client_id)",
    "CREATE INDEX IF NOT EXISTS idx_clients_connected ON clients(connected)",
    r#"
    CREATE TABLE IF NOT EXISTS sessions (
        id BIGSERIAL PRIMARY KEY,
        client_id VARCHAR(255) NOT NULL UNIQUE REFERENCES clients(client_id) ON DELETE CASCADE,
        clean_session BOOLEAN NOT NULL,
        session_expiry_interval INTEGER,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    "#,
    r#"
    CREATE TABLE IF NOT EXISTS subscriptions (
        id BIGSERIAL PRIMARY KEY,
        client_id VARCHAR(255) NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
        topic VARCHAR(512) NOT NULL,
        qos SMALLINT NOT NULL,
        no_local BOOLEAN NOT NULL DEFAULT FALSE,
        retain_as_published BOOLEAN NOT NULL DEFAULT FALSE,
        retain_handling SMALLINT NOT NULL DEFAULT 0,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(client_id, topic)
    )
    "#,
    "CREATE INDEX IF NOT EXISTS idx_subscriptions_client_id ON subscriptions(client_id)",
    "CREATE INDEX IF NOT EXISTS idx_subscriptions_topic ON subscriptions(topic)",
    r#"
    CREATE TABLE IF NOT EXISTS retained_messages (
        id BIGSERIAL PRIMARY KEY,
        topic VARCHAR(512) NOT NULL UNIQUE,
        qos SMALLINT NOT NULL,
        payload BYTEA,
        payload_size INTEGER,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    "#,
    "CREATE INDEX IF NOT EXISTS idx_retained_messages_topic ON retained_messages(topic)",
    r#"
    CREATE TABLE IF NOT EXISTS messages (
        id BIGSERIAL PRIMARY KEY,
        client_id VARCHAR(255) NOT NULL REFERENCES clients(client_id) ON DELETE CASCADE,
        packet_id INTEGER NOT NULL,
        topic VARCHAR(512) NOT NULL,
        qos SMALLINT NOT NULL,
        payload BYTEA,
        payload_size INTEGER,
        retain BOOLEAN NOT NULL DEFAULT FALSE,
        dup BOOLEAN NOT NULL DEFAULT FALSE,
        direction VARCHAR(8) NOT NULL DEFAULT 'out',
        state VARCHAR(20) NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(client_id, packet_id, direction)
    )
    "#,
    "CREATE INDEX IF NOT EXISTS idx_messages_client_id ON messages(client_id)",
    "CREATE INDEX IF NOT EXISTS idx_messages_state ON messages(state)",
];
