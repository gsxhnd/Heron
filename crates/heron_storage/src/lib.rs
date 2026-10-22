//! SeaORM persistence for Heron sessions, subscriptions, retained messages,
//! and QoS inflight rows.
//!
//! The broker keeps its hot path in memory. This crate writes that state through
//! to PostgreSQL and loads it again when a process starts.

mod entity;
mod postgres;
mod schema;

pub use postgres::PostgresStorage;
