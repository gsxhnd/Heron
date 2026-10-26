//! Errors from rule configuration and hook plugins.

/// A stored rule could not be created or updated.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RuleError {
    /// The JSON or SQL does not describe an executable rule.
    #[error("{0}")]
    Invalid(String),
    /// No rule has this id.
    #[error("rule not found")]
    NotFound,
}

/// Failure reported by a [`crate::Hook`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HookError {
    /// Stop the connect, subscribe, or publish.
    #[error("hook rejected the event")]
    Reject,
    /// The hook wants another attempt. The broker logs this and continues.
    #[error("hook asked to retry")]
    Retry,
    /// The hook failed. The broker logs this and continues.
    #[error("{0}")]
    Internal(String),
}
