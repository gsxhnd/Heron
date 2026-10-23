//! Authentication and topic authorization for a Heron node.
//!
//! The interfaces stay independent of the network listeners and the storage
//! backend. [`AllowAll`] preserves the v0.2.0 open broker. [`BuiltInAuthenticator`]
//! checks username/password, bearer-style tokens carried in the CONNECT password,
//! and a client-certificate identity. [`AclAuthorizer`] applies priority-ordered
//! allow and deny rules.

mod error;
mod topic;

use std::collections::{HashMap, HashSet};

use argon2::Argon2;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};

pub use error::AuthError;
pub use topic::{filter_covers, topic_matches};

/// Who the client authenticated as. `username` is `None` for an anonymous session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// Authenticated username, token owner, or certificate identity.
    pub username: Option<String>,
}

/// Credentials presented on CONNECT, plus an optional verified certificate identity.
#[derive(Debug, Clone, Copy)]
pub struct AuthRequest<'a> {
    /// Client identifier from CONNECT. Empty when the server will assign one.
    pub client_id: &'a str,
    /// MQTT username. Empty is treated as absent.
    pub username: Option<&'a str>,
    /// MQTT password or token bytes.
    pub password: Option<&'a [u8]>,
    /// Identity taken from a verified client certificate. Empty is treated as absent.
    pub certificate_identity: Option<&'a str>,
}

/// Decides whether CONNECT credentials are acceptable.
#[async_trait::async_trait]
pub trait Authenticator: Send + Sync {
    /// Authenticate one CONNECT attempt.
    async fn authenticate(&self, request: AuthRequest<'_>) -> Result<Identity, AuthError>;
}

/// Publish or subscribe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Client PUBLISH, or a will topic checked at CONNECT.
    Publish,
    /// Client SUBSCRIBE filter.
    Subscribe,
}

/// Decides whether an authenticated session may publish or subscribe.
#[async_trait::async_trait]
pub trait Authorizer: Send + Sync {
    /// Return `Ok(true)` when the operation is allowed.
    async fn authorize(
        &self,
        client_id: &str,
        username: Option<&str>,
        topic: &str,
        action: Action,
    ) -> Result<bool, AuthError>;
}

/// Accept every CONNECT and every topic operation.
#[derive(Debug, Default, Clone, Copy)]
pub struct AllowAll;

#[async_trait::async_trait]
impl Authenticator for AllowAll {
    async fn authenticate(&self, request: AuthRequest<'_>) -> Result<Identity, AuthError> {
        Ok(Identity {
            username: present(request.username).map(str::to_owned),
        })
    }
}

#[async_trait::async_trait]
impl Authorizer for AllowAll {
    async fn authorize(
        &self,
        _client_id: &str,
        _username: Option<&str>,
        _topic: &str,
        _action: Action,
    ) -> Result<bool, AuthError> {
        Ok(true)
    }
}

/// How client-certificate identities are accepted after rustls has verified the chain.
#[derive(Debug, Clone)]
pub struct CertificatePolicy {
    allow_any: bool,
    identities: HashSet<String>,
}

impl CertificatePolicy {
    /// Accept any identity extracted from a verified client certificate.
    pub fn allow_any() -> Self {
        Self {
            allow_any: true,
            identities: HashSet::new(),
        }
    }

    /// Accept only these identities (certificate CN, or DNS SAN when present).
    pub fn allowlist(identities: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            allow_any: false,
            identities: identities.into_iter().map(Into::into).collect(),
        }
    }

    fn permits(&self, identity: &str) -> bool {
        self.allow_any || self.identities.contains(identity)
    }
}

/// One ACL row. `None` username or client id matches every value.
#[derive(Debug, Clone)]
pub struct AclRule {
    /// Match this authenticated username. `None` matches every username, including anonymous.
    pub username: Option<String>,
    /// Match this client id. `None` matches every client.
    pub client_id: Option<String>,
    /// MQTT topic filter.
    pub topic: String,
    /// Which operations the row applies to.
    pub action: AclAction,
    /// `true` allows the operation. `false` denies it.
    pub allow: bool,
    /// Higher values win. Equal priority resolves to deny.
    pub priority: i32,
}

/// Operations an ACL row covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclAction {
    /// PUBLISH and will topics.
    Publish,
    /// SUBSCRIBE filters.
    Subscribe,
    /// Both publish and subscribe.
    PubSub,
}

impl AclAction {
    fn covers(self, action: Action) -> bool {
        match (self, action) {
            (Self::PubSub, _)
            | (Self::Publish, Action::Publish)
            | (Self::Subscribe, Action::Subscribe) => true,
            (Self::Publish, Action::Subscribe) | (Self::Subscribe, Action::Publish) => false,
        }
    }
}

/// Username/password, token, and certificate checks.
#[derive(Clone)]
pub struct BuiltInAuthenticator {
    allow_anonymous: bool,
    users: HashMap<String, String>,
    dummy_hash: String,
    tokens: Vec<(String, String)>,
    certificates: Option<CertificatePolicy>,
}

impl BuiltInAuthenticator {
    /// Build an authenticator.
    ///
    /// `users` stores PHC password hashes. `tokens` are `(token, username)` pairs
    /// compared with the CONNECT password. `certificates` is `None` when a client
    /// certificate does not authenticate the session.
    pub fn new(
        allow_anonymous: bool,
        users: HashMap<String, String>,
        tokens: Vec<(String, String)>,
        certificates: Option<CertificatePolicy>,
    ) -> Result<Self, AuthError> {
        let dummy_hash = hash_password("heron-timing-equalizer")?;
        Ok(Self {
            allow_anonymous,
            users,
            dummy_hash,
            tokens,
            certificates,
        })
    }

    async fn password_matches(&self, username: &str, password: &[u8]) -> Result<bool, AuthError> {
        let hash = self
            .users
            .get(username)
            .cloned()
            .unwrap_or_else(|| self.dummy_hash.clone());
        let password = password.to_vec();
        let known = self.users.contains_key(username);
        let verified = tokio::task::spawn_blocking(move || verify_password(&password, &hash))
            .await
            .map_err(|err| AuthError::Internal(err.to_string()))??;
        Ok(known && verified)
    }

    fn token_username(&self, presented_username: Option<&str>, password: &[u8]) -> Option<String> {
        self.tokens.iter().find_map(|(token, owner)| {
            if !constant_time_eq(token.as_bytes(), password) {
                return None;
            }
            match presented_username {
                Some(username) if username != owner => None,
                _ => Some(owner.clone()),
            }
        })
    }
}

#[async_trait::async_trait]
impl Authenticator for BuiltInAuthenticator {
    async fn authenticate(&self, request: AuthRequest<'_>) -> Result<Identity, AuthError> {
        let username = present(request.username);
        let password = request.password.filter(|password| !password.is_empty());
        let certificate = present(request.certificate_identity);

        if let (Some(certificate), Some(policy)) = (certificate, &self.certificates)
            && !policy.permits(certificate)
        {
            return Err(AuthError::NotAuthorized);
        }

        if let Some(username) = username
            && self.users.contains_key(username)
        {
            let matches = self
                .password_matches(username, password.unwrap_or(b""))
                .await?;
            if !matches {
                return Err(AuthError::BadCredentials);
            }
            ensure_certificate_agrees(self.certificates.as_ref(), certificate, username)?;
            return Ok(Identity {
                username: Some(username.to_owned()),
            });
        }

        if let Some(password) = password
            && let Some(owner) = self.token_username(username, password)
        {
            ensure_certificate_agrees(self.certificates.as_ref(), certificate, &owner)?;
            return Ok(Identity {
                username: Some(owner),
            });
        }

        if let (Some(certificate), Some(policy)) = (certificate, &self.certificates)
            && policy.permits(certificate)
            && password.is_none()
        {
            return Ok(Identity {
                username: Some(certificate.to_owned()),
            });
        }

        if username.is_none() && password.is_none() && self.allow_anonymous {
            return Ok(Identity { username: None });
        }
        if username.is_some() || password.is_some() {
            return Err(AuthError::BadCredentials);
        }
        Err(AuthError::NotAuthorized)
    }
}

fn ensure_certificate_agrees(
    policy: Option<&CertificatePolicy>,
    certificate: Option<&str>,
    username: &str,
) -> Result<(), AuthError> {
    let (Some(_policy), Some(certificate)) = (policy, certificate) else {
        return Ok(());
    };
    if certificate == username {
        Ok(())
    } else {
        Err(AuthError::NotAuthorized)
    }
}

/// Priority ACL. The highest matching priority wins. A tie resolves to deny.
#[derive(Debug, Clone)]
pub struct AclAuthorizer {
    rules: Vec<AclRule>,
    default_allow: bool,
}

impl AclAuthorizer {
    /// `default_allow` applies when no rule matches.
    pub fn new(rules: Vec<AclRule>, default_allow: bool) -> Self {
        Self {
            rules,
            default_allow,
        }
    }
}

#[async_trait::async_trait]
impl Authorizer for AclAuthorizer {
    async fn authorize(
        &self,
        client_id: &str,
        username: Option<&str>,
        topic: &str,
        action: Action,
    ) -> Result<bool, AuthError> {
        let mut best: Option<(i32, bool)> = None;
        for rule in &self.rules {
            if !rule_matches(rule, client_id, username, topic, action) {
                continue;
            }
            match best {
                Some((priority, _)) if rule.priority < priority => {}
                Some((priority, allow)) if rule.priority == priority => {
                    best = Some((priority, allow && rule.allow));
                }
                _ => best = Some((rule.priority, rule.allow)),
            }
        }
        Ok(best.map(|(_, allow)| allow).unwrap_or(self.default_allow))
    }
}

fn rule_matches(
    rule: &AclRule,
    client_id: &str,
    username: Option<&str>,
    topic: &str,
    action: Action,
) -> bool {
    if !rule.action.covers(action) {
        return false;
    }
    if let Some(expected) = &rule.username
        && username != Some(expected.as_str())
    {
        return false;
    }
    if let Some(expected) = &rule.client_id
        && expected != client_id
    {
        return false;
    }
    match action {
        Action::Publish => topic_matches(&rule.topic, topic),
        Action::Subscribe => filter_covers(&rule.topic, topic),
    }
}

fn present(value: Option<&str>) -> Option<&str> {
    value.filter(|value| !value.is_empty())
}

/// Hash a password with Argon2id. The result is a PHC string safe to store in config.
pub fn hash_password(password: &str) -> Result<String, AuthError> {
    let mut salt_bytes = [0u8; 16];
    getrandom::getrandom(&mut salt_bytes).map_err(|err| AuthError::Internal(err.to_string()))?;
    let salt =
        SaltString::encode_b64(&salt_bytes).map_err(|err| AuthError::Internal(err.to_string()))?;
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|err| AuthError::Internal(err.to_string()))
}

fn verify_password(password: &[u8], encoded: &str) -> Result<bool, AuthError> {
    let parsed = PasswordHash::new(encoded).map_err(|err| AuthError::Internal(err.to_string()))?;
    Ok(Argon2::default().verify_password(password, &parsed).is_ok())
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0u8;
    for (left, right) in left.iter().zip(right.iter()) {
        diff |= left ^ right;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn auth(allow_anonymous: bool) -> BuiltInAuthenticator {
        let hash = hash_password("secret").expect("hash");
        let mut users = HashMap::new();
        users.insert("alice".to_owned(), hash);
        BuiltInAuthenticator::new(
            allow_anonymous,
            users,
            vec![("device-token".to_owned(), "device-1".to_owned())],
            Some(CertificatePolicy::allowlist(["sensor-7"])),
        )
        .expect("authenticator")
    }

    fn request<'a>(
        username: Option<&'a str>,
        password: Option<&'a [u8]>,
        certificate: Option<&'a str>,
    ) -> AuthRequest<'a> {
        AuthRequest {
            client_id: "client",
            username,
            password,
            certificate_identity: certificate,
        }
    }

    #[tokio::test]
    async fn password_should_authenticate_and_reject_a_wrong_secret() {
        let auth = auth(false);
        let ok = auth
            .authenticate(request(Some("alice"), Some(b"secret"), None))
            .await
            .expect("password");
        assert_eq!(ok.username.as_deref(), Some("alice"));
        let err = auth
            .authenticate(request(Some("alice"), Some(b"nope"), None))
            .await
            .expect_err("wrong password");
        assert_eq!(err, AuthError::BadCredentials);
    }

    #[tokio::test]
    async fn token_should_bind_the_configured_username() {
        let auth = auth(false);
        let ok = auth
            .authenticate(request(None, Some(b"device-token"), None))
            .await
            .expect("token");
        assert_eq!(ok.username.as_deref(), Some("device-1"));
        let mismatch = auth
            .authenticate(request(Some("alice"), Some(b"device-token"), None))
            .await
            .expect_err("token username");
        assert_eq!(mismatch, AuthError::BadCredentials);
    }

    #[tokio::test]
    async fn certificate_should_authenticate_an_allowlisted_identity() {
        let auth = auth(false);
        let ok = auth
            .authenticate(request(None, None, Some("sensor-7")))
            .await
            .expect("cert");
        assert_eq!(ok.username.as_deref(), Some("sensor-7"));
        let denied = auth
            .authenticate(request(None, None, Some("other")))
            .await
            .expect_err("cert");
        assert_eq!(denied, AuthError::NotAuthorized);
    }

    #[tokio::test]
    async fn anonymous_should_require_the_allow_flag() {
        let closed = auth(false);
        let err = closed
            .authenticate(request(None, None, None))
            .await
            .expect_err("anonymous");
        assert_eq!(err, AuthError::NotAuthorized);
        let open = auth(true);
        let ok = open
            .authenticate(request(None, None, None))
            .await
            .expect("anonymous");
        assert_eq!(ok.username, None);
    }

    #[tokio::test]
    async fn acl_should_prefer_higher_priority_and_deny_ties() {
        let authorizer = AclAuthorizer::new(
            vec![
                AclRule {
                    username: Some("alice".into()),
                    client_id: None,
                    topic: "#".into(),
                    action: AclAction::PubSub,
                    allow: true,
                    priority: 1,
                },
                AclRule {
                    username: Some("alice".into()),
                    client_id: None,
                    topic: "secret/#".into(),
                    action: AclAction::Publish,
                    allow: false,
                    priority: 10,
                },
                AclRule {
                    username: None,
                    client_id: Some("locked".into()),
                    topic: "open".into(),
                    action: AclAction::Subscribe,
                    allow: true,
                    priority: 0,
                },
                AclRule {
                    username: None,
                    client_id: Some("locked".into()),
                    topic: "open".into(),
                    action: AclAction::Subscribe,
                    allow: false,
                    priority: 0,
                },
            ],
            false,
        );
        assert!(
            authorizer
                .authorize("c", Some("alice"), "sensors/temp", Action::Publish)
                .await
                .expect("allow")
        );
        assert!(
            !authorizer
                .authorize("c", Some("alice"), "secret/x", Action::Publish)
                .await
                .expect("deny")
        );
        assert!(
            !authorizer
                .authorize("locked", Some("bob"), "open", Action::Subscribe)
                .await
                .expect("tie deny")
        );
        assert!(
            !authorizer
                .authorize("stranger", None, "sensors/temp", Action::Subscribe)
                .await
                .expect("default deny")
        );
    }
}
