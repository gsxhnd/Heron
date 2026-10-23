//! Authentication and authorization failures.

/// Failure from an [`crate::Authenticator`] or [`crate::Authorizer`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    /// Username and password, or token, did not match a known identity.
    #[error("bad username or password")]
    BadCredentials,

    /// The client is not allowed to connect or use the topic.
    #[error("not authorized")]
    NotAuthorized,

    /// The backend could not complete the check.
    #[error("authentication backend failed: {0}")]
    Internal(String),
}
