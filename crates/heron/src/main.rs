//! Heron 单节点进程。
//!
//! ```text
//! cargo run -p heron
//! cargo run -p heron -- --config conf/broker.yaml
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use heron_api::ApiServer;
use heron_auth::{
    AclAction, AclAuthorizer, AclRule, Authenticator, Authorizer, BuiltInAuthenticator,
    CertificatePolicy,
};
use heron_core::{Broker, BrokerConfig, MemoryStorage, Storage};
use heron_mqtt::{ClientCertMode, Server, TlsAcceptor, acceptor_from_pem};
use heron_rules::RuleEngine;
use heron_storage::PostgresStorage;
use serde::Deserialize;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "heron", about = "Heron 单节点 MQTT Broker")]
struct Cli {
    /// YAML 配置文件。省略时使用内存存储，并监听 1883 与 8083。
    #[arg(long)]
    config: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct FileConfig {
    #[serde(default)]
    tcp: ListenerConfig,
    #[serde(default)]
    websocket: ListenerConfig,
    #[serde(default)]
    mqtts: OptionalListener,
    #[serde(default)]
    wss: OptionalListener,
    #[serde(default)]
    tls: Option<TlsFile>,
    #[serde(default)]
    storage: StorageConfig,
    #[serde(default)]
    auth: Option<AuthFile>,
    #[serde(default)]
    api: ApiFile,
}

#[derive(Debug, Deserialize)]
struct ListenerConfig {
    #[serde(default = "default_true")]
    enabled: bool,
    #[serde(default)]
    bind: String,
}

impl Default for ListenerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind: String::new(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct StorageConfig {
    /// `memory` 或 `postgres`。
    #[serde(default = "default_memory")]
    kind: String,
    #[serde(default)]
    url: Option<String>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            kind: default_memory(),
            url: None,
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_memory() -> String {
    "memory".to_owned()
}

#[derive(Debug, Deserialize, Default)]
struct OptionalListener {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    bind: String,
}

#[derive(Debug, Deserialize)]
struct TlsFile {
    cert_file: String,
    key_file: String,
    #[serde(default)]
    client_ca_file: Option<String>,
    /// `none`, `optional`, or `required`.
    #[serde(default = "default_client_auth")]
    client_auth: String,
}

fn default_client_auth() -> String {
    "none".to_owned()
}

#[derive(Debug, Deserialize)]
struct ApiFile {
    #[serde(default = "default_true")]
    enabled: bool,
    #[serde(default)]
    bind: String,
    #[serde(default)]
    token: Option<String>,
}

impl Default for ApiFile {
    fn default() -> Self {
        Self {
            enabled: true,
            bind: String::new(),
            token: None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct AuthFile {
    #[serde(default)]
    allow_anonymous: bool,
    #[serde(default)]
    users: Vec<UserFile>,
    #[serde(default)]
    tokens: Vec<TokenFile>,
    /// Empty list accepts any verified client certificate. Omit to ignore certificates.
    #[serde(default)]
    certificates: Option<Vec<String>>,
    /// `allow` or `deny`. Rules imply deny when this is omitted.
    #[serde(default)]
    acl_default: Option<String>,
    #[serde(default)]
    acl: Vec<AclFile>,
}

#[derive(Debug, Deserialize)]
struct UserFile {
    username: String,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    password_hash: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TokenFile {
    token: String,
    username: String,
}

#[derive(Debug, Deserialize)]
struct AclFile {
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    client_id: Option<String>,
    topic: String,
    action: String,
    allow: bool,
    #[serde(default)]
    priority: i32,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let file = match &cli.config {
        Some(path) => {
            let text = std::fs::read_to_string(path)?;
            serde_yaml::from_str(&text)?
        }
        None => FileConfig {
            tcp: ListenerConfig {
                enabled: true,
                bind: "0.0.0.0:1883".to_owned(),
            },
            websocket: ListenerConfig {
                enabled: true,
                bind: "0.0.0.0:8083".to_owned(),
            },
            mqtts: OptionalListener::default(),
            wss: OptionalListener::default(),
            tls: None,
            storage: StorageConfig::default(),
            auth: None,
            api: ApiFile::default(),
        },
    };

    let storage = open_storage(&file.storage).await?;
    let broker = Broker::open(storage.clone(), BrokerConfig::default()).await?;
    let engine = RuleEngine::new();
    engine.set_storage(storage.clone());
    let stored_rules = storage.load_rules().await?;
    if !stored_rules.is_empty() {
        engine.restore(stored_rules).await;
        tracing::info!("规则引擎已从存储恢复规则");
    }
    broker.install_hooks(engine.hooks());
    if let Some(auth) = &file.auth {
        let security = build_security(auth)?;
        broker.install_security(security.authenticator, security.authorizer);
    }
    let tcp = bind_addr(&file.tcp, "0.0.0.0:1883")?;
    let websocket = bind_addr(&file.websocket, "0.0.0.0:8083")?;
    let mut server = Server::bind(broker.clone(), tcp, websocket).await?;
    if file.mqtts.enabled || file.wss.enabled {
        let tls = file
            .tls
            .as_ref()
            .ok_or("启用 MQTTS 或 WSS 时必须配置 tls")?;
        let acceptor = build_acceptor(tls)?;
        let mqtts = optional_addr(&file.mqtts, "0.0.0.0:8883")?;
        let wss = optional_addr(&file.wss, "0.0.0.0:8084")?;
        server.bind_secure(mqtts, wss, acceptor).await?;
    }
    if let Some(addr) = server.tcp_addr() {
        tracing::info!(%addr, "MQTT TCP 监听已启动");
    }
    if let Some(addr) = server.websocket_addr() {
        tracing::info!(%addr, "MQTT WebSocket 监听已启动");
    }
    if let Some(addr) = server.mqtts_addr() {
        tracing::info!(%addr, "MQTTS 监听已启动");
    }
    if let Some(addr) = server.wss_addr() {
        tracing::info!(%addr, "WSS 监听已启动");
    }

    let api = if file.api.enabled {
        let bind = if file.api.bind.is_empty() {
            "0.0.0.0:8080"
        } else {
            &file.api.bind
        };
        let addr: SocketAddr = bind.parse()?;
        let token = std::env::var("HERON_API_TOKEN")
            .ok()
            .filter(|token| !token.is_empty())
            .or_else(|| file.api.token.clone());
        if token.is_none() {
            tracing::warn!("管理 API 未配置 bearer token，接口在本机以外也可匿名访问");
        }
        let api = ApiServer::bind(broker.clone(), engine.clone(), addr, token).await?;
        let bound = api.local_addr()?;
        tracing::info!(%bound, "管理 API 已启动");
        Some(api)
    } else {
        None
    };

    match api {
        Some(api) => {
            tokio::select! {
                result = server.serve(shutdown_signal()) => result?,
                result = api.serve(shutdown_signal()) => result?,
            }
        }
        None => server.serve(shutdown_signal()).await?,
    }
    Ok(())
}

async fn open_storage(
    config: &StorageConfig,
) -> Result<Arc<dyn Storage>, Box<dyn std::error::Error>> {
    match config.kind.as_str() {
        "memory" => Ok(Arc::new(MemoryStorage::new())),
        "postgres" => {
            let url = config.url.as_deref().ok_or("storage.url 未配置")?;
            let storage = PostgresStorage::connect(url).await?;
            Ok(Arc::new(storage))
        }
        other => Err(format!("未知存储类型 {other}").into()),
    }
}

fn bind_addr(
    config: &ListenerConfig,
    default: &str,
) -> Result<Option<SocketAddr>, std::net::AddrParseError> {
    if !config.enabled {
        return Ok(None);
    }
    let text = if config.bind.is_empty() {
        default
    } else {
        &config.bind
    };
    Ok(Some(text.parse()?))
}

fn optional_addr(
    config: &OptionalListener,
    default: &str,
) -> Result<Option<SocketAddr>, std::net::AddrParseError> {
    if !config.enabled {
        return Ok(None);
    }
    let text = if config.bind.is_empty() {
        default
    } else {
        &config.bind
    };
    Ok(Some(text.parse()?))
}

fn build_acceptor(config: &TlsFile) -> Result<TlsAcceptor, Box<dyn std::error::Error>> {
    let cert = std::fs::read(&config.cert_file)?;
    let key = std::fs::read(&config.key_file)?;
    let client_ca = match &config.client_ca_file {
        Some(path) => Some(std::fs::read(path)?),
        None => None,
    };
    let mode = match config.client_auth.as_str() {
        "none" => ClientCertMode::Disabled,
        "optional" => ClientCertMode::Optional,
        "required" => ClientCertMode::Required,
        other => return Err(format!("未知 client_auth {other}").into()),
    };
    Ok(acceptor_from_pem(&cert, &key, client_ca.as_deref(), mode)?)
}

fn build_security(config: &AuthFile) -> Result<SecurityStack, Box<dyn std::error::Error>> {
    let mut users = HashMap::new();
    for user in &config.users {
        let hash = match (&user.password_hash, &user.password) {
            (Some(hash), _) => hash.clone(),
            (None, Some(password)) => heron_auth::hash_password(password)?,
            (None, None) => {
                return Err(
                    format!("用户 {} 缺少 password 或 password_hash", user.username).into(),
                );
            }
        };
        users.insert(user.username.clone(), hash);
    }
    let tokens = config
        .tokens
        .iter()
        .map(|token| (token.token.clone(), token.username.clone()))
        .collect();
    let certificates = config.certificates.as_ref().map(|identities| {
        if identities.is_empty() {
            CertificatePolicy::allow_any()
        } else {
            CertificatePolicy::allowlist(identities.iter().cloned())
        }
    });
    let authenticator =
        BuiltInAuthenticator::new(config.allow_anonymous, users, tokens, certificates)?;
    let default_allow = match config.acl_default.as_deref() {
        Some("allow") => true,
        Some("deny") => false,
        None => config.acl.is_empty(),
        Some(other) => return Err(format!("未知 acl_default {other}").into()),
    };
    let mut rules = Vec::with_capacity(config.acl.len());
    for rule in &config.acl {
        let action = match rule.action.as_str() {
            "publish" => AclAction::Publish,
            "subscribe" => AclAction::Subscribe,
            "pubsub" => AclAction::PubSub,
            other => return Err(format!("未知 ACL action {other}").into()),
        };
        rules.push(AclRule {
            username: rule.username.clone(),
            client_id: rule.client_id.clone(),
            topic: rule.topic.clone(),
            action,
            allow: rule.allow,
            priority: rule.priority,
        });
    }
    let authorizer: Arc<dyn Authorizer> = if config.acl.is_empty() && default_allow {
        Arc::new(heron_auth::AllowAll)
    } else {
        Arc::new(AclAuthorizer::new(rules, default_allow))
    };
    Ok(SecurityStack {
        authenticator: Arc::new(authenticator),
        authorizer,
    })
}

struct SecurityStack {
    authenticator: Arc<dyn Authenticator>,
    authorizer: Arc<dyn Authorizer>,
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("收到退出信号，停止接受新连接");
}
