//! Heron 单节点进程。
//!
//! ```text
//! cargo run -p heron
//! cargo run -p heron -- --config conf/broker.yaml
//! ```

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use heron_core::{Broker, BrokerConfig, MemoryStorage, Storage};
use heron_mqtt::Server;
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
    storage: StorageConfig,
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
            storage: StorageConfig::default(),
        },
    };

    let storage = open_storage(&file.storage).await?;
    let broker = Broker::open(storage, BrokerConfig::default()).await?;
    let tcp = bind_addr(&file.tcp, "0.0.0.0:1883")?;
    let websocket = bind_addr(&file.websocket, "0.0.0.0:8083")?;
    let server = Server::bind(broker, tcp, websocket).await?;
    if let Some(addr) = server.tcp_addr() {
        tracing::info!(%addr, "MQTT TCP 监听已启动");
    }
    if let Some(addr) = server.websocket_addr() {
        tracing::info!(%addr, "MQTT WebSocket 监听已启动");
    }
    server.serve(shutdown_signal()).await?;
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

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("收到退出信号，停止接受新连接");
}
