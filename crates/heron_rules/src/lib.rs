//! Rule execution for a single Heron node.
//!
//! Rules live in memory and are evaluated when the broker publishes, connects,
//! or changes subscriptions. A rule matches an MQTT topic filter, then an
//! optional condition, then runs actions: `log`, `drop`, `reject`, `republish`,
//! `webhook`, and `transform`.
//!
//! Conditions accept a boolean expression:
//!
//! ```text
//! payload.temp > 30 AND client_id = 'sensor-1'
//! ```
//!
//! A full routing statement is available through [`parse_rule_sql`]:
//!
//! ```text
//! SELECT payload.temp AS temp FROM "sensors/+/temp" WHERE payload.temp > 30
//! ```
//!
//! Lifecycle hooks see the topics `$events/client_connected`,
//! `$events/client_disconnected`, `$events/client_subscribed`, and
//! `$events/client_unsubscribed`.

#![deny(clippy::unwrap_used)]

mod engine;
mod error;
mod eval;
mod hook;
mod parse;

pub use engine::{RuleEngine, RuleInput, RuleRecord};
pub use error::{HookError, RuleError};
pub use hook::{Hook, HookContext, HookEvent};
pub use parse::{ParsedRule, parse_rule_sql};
