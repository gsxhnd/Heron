//! MQTT CLI — publish and subscribe against a broker.

use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::{Parser, Subcommand};
use mqtt_client::{ClientConfig, Event, MqttClient, ReconnectOptions, TlsOptions};
use tokio::sync::mpsc;

#[derive(Parser)]
#[command(name = "mqtt", about = "MQTT CLI client (v3.1.1 and v5.0)", version)]
struct Cli {
    /// Protocol version: v4 / 3.1.1 or v5 / 5.0.
    #[arg(
        long = "protocol",
        alias = "mqtt-version",
        global = true,
        default_value = "v4",
        value_parser = clap::value_parser!(mqtt_client::ProtocolVersion)
    )]
    protocol: mqtt_client::ProtocolVersion,

    /// Broker hostname.
    #[arg(short = 'H', long, global = true, default_value = "127.0.0.1")]
    host: String,

    /// Broker port.
    #[arg(short, long, global = true, default_value_t = 1883)]
    port: u16,

    /// Client identifier (auto-generated if empty).
    #[arg(short, long, global = true, default_value = "")]
    client_id: String,

    /// Username for authentication.
    #[arg(short, long, global = true)]
    username: Option<String>,

    /// Password for authentication.
    #[arg(short = 'P', long, global = true)]
    password: Option<String>,

    /// Keep-alive interval in seconds.
    #[arg(long, global = true, default_value_t = 60)]
    keep_alive: u16,

    /// Start a clean session.
    #[arg(long, global = true, default_value_t = true)]
    clean: bool,

    /// Connection timeout in seconds.
    #[arg(long, global = true, default_value_t = 5)]
    timeout: u64,

    /// How long to wait for broker acknowledgements (seconds).
    #[arg(long, global = true, default_value_t = 30)]
    request_timeout: u64,

    /// Enable TLS.
    #[arg(long, global = true, default_value_t = false)]
    tls: bool,

    /// Skip TLS certificate verification (lab only).
    #[arg(long, global = true, default_value_t = false)]
    tls_insecure: bool,

    /// Disable automatic reconnect.
    #[arg(long, global = true, default_value_t = false)]
    no_reconnect: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Publish a message to a topic.
    Pub {
        /// Topic name.
        #[arg(short, long)]
        topic: String,

        /// Message payload.
        #[arg(short, long)]
        message: String,

        /// QoS level (0, 1, or 2).
        #[arg(short, long, default_value_t = 0)]
        qos: u8,

        /// Retain flag.
        #[arg(short, long, default_value_t = false)]
        retain: bool,

        /// Publish the message this many times.
        #[arg(short = 'n', long, default_value_t = 1)]
        count: u32,

        /// Delay between repeated publishes (milliseconds).
        #[arg(long, default_value_t = 0)]
        interval: u64,
    },
    /// Subscribe to one or more topic filters and print incoming messages.
    Sub {
        /// Topic filters (repeatable).
        #[arg(short, long = "topic", required = true)]
        topics: Vec<String>,

        /// Requested QoS (0, 1, or 2).
        #[arg(short, long, default_value_t = 0)]
        qos: u8,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = build_config(&cli);

    match cli.command {
        Commands::Pub {
            topic,
            message,
            qos,
            retain,
            count,
            interval,
        } => run_publish(config, &topic, &message, qos, retain, count, interval).await,
        Commands::Sub { topics, qos } => run_subscribe(config, &topics, qos).await,
    }
}

fn build_config(cli: &Cli) -> ClientConfig {
    let mut config = ClientConfig::new(format!("{}:{}", cli.host, cli.port), cli.client_id.clone())
        .keep_alive(cli.keep_alive)
        .clean_session(cli.clean)
        .connect_timeout(Duration::from_secs(cli.timeout))
        .request_timeout(Duration::from_secs(cli.request_timeout))
        .protocol_version(cli.protocol)
        .tls(TlsOptions {
            enabled: cli.tls,
            insecure_skip_verify: cli.tls_insecure,
        })
        .reconnect(ReconnectOptions {
            enabled: !cli.no_reconnect,
            ..ReconnectOptions::default()
        });

    // One-shot pub should not spin reconnect forever on failure.
    if matches!(cli.command, Commands::Pub { .. }) {
        config.reconnect.enabled = false;
    }

    if let Some(username) = &cli.username {
        let password = cli.password.clone().map(Bytes::from);
        config = config.credentials(username.clone(), password);
    }

    config
}

async fn run_publish(
    config: ClientConfig,
    topic: &str,
    message: &str,
    qos: u8,
    retain: bool,
    count: u32,
    interval: u64,
) -> Result<()> {
    let (client, mut events) = MqttClient::new(config);
    wait_connected(&mut events).await?;

    let payload = Bytes::from(message.to_string());
    let total = count.max(1);
    for i in 0..total {
        client
            .publish(topic, payload.clone(), qos, retain)
            .await
            .with_context(|| format!("publish #{}/{} failed", i + 1, total))?;

        if i + 1 < total && interval > 0 {
            tokio::time::sleep(Duration::from_millis(interval)).await;
        }
    }

    match qos {
        0 => eprintln!("published {total} message(s) to {topic} (QoS 0)"),
        1 => eprintln!("published {total} message(s) to {topic} (PUBACK received)"),
        _ => eprintln!("published {total} message(s) to {topic} (PUBCOMP received)"),
    }

    client.disconnect().await.ok();
    Ok(())
}

async fn run_subscribe(config: ClientConfig, topics: &[String], qos: u8) -> Result<()> {
    let (client, mut events) = MqttClient::new(config);
    wait_connected(&mut events).await?;

    let filters: Vec<(String, u8)> = topics.iter().map(|t| (t.clone(), qos)).collect();
    let granted = client
        .subscribe(filters)
        .await
        .context("subscribe failed")?;

    for (filter, code) in topics.iter().zip(granted.iter()) {
        eprintln!("subscribed to {filter} (granted {code})");
    }
    eprintln!("Ctrl+C to exit");

    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);

    loop {
        tokio::select! {
            event = events.recv() => {
                match event {
                    Some(Event::Message { topic, payload, .. }) => {
                        let body = String::from_utf8_lossy(&payload);
                        println!("{topic} {body}");
                    }
                    Some(Event::Reconnecting { attempt, delay_ms }) => {
                        eprintln!("reconnecting (attempt {attempt}, delay {delay_ms}ms)...");
                    }
                    Some(Event::Connected { session_present }) => {
                        eprintln!("reconnected (session_present={session_present})");
                    }
                    Some(Event::Disconnected { reason }) => {
                        eprintln!(
                            "disconnected: {}",
                            reason.unwrap_or_else(|| "unknown".into())
                        );
                    }
                    None => anyhow::bail!("event channel closed"),
                    _ => {}
                }
            }
            _ = &mut ctrl_c => {
                client.disconnect().await.ok();
                break;
            }
        }
    }

    Ok(())
}

async fn wait_connected(events: &mut mpsc::Receiver<Event>) -> Result<()> {
    loop {
        match events.recv().await {
            Some(Event::Connected { .. }) => return Ok(()),
            Some(Event::Reconnecting { attempt, delay_ms }) => {
                eprintln!("connecting (attempt {attempt}, delay {delay_ms}ms)...");
            }
            Some(Event::Disconnected { reason }) => {
                // With reconnect disabled, treat as failure; with reconnect, keep waiting.
                eprintln!(
                    "connection attempt failed: {}",
                    reason.unwrap_or_else(|| "unknown".into())
                );
            }
            None => anyhow::bail!("event channel closed before connect"),
            _ => {}
        }
    }
}
