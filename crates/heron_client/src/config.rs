//! Client configuration.

use std::time::Duration;

use bytes::Bytes;

/// MQTT protocol version to use for the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProtocolVersion {
    /// MQTT v3.1.1 (protocol level 4).
    #[default]
    V4,
    /// MQTT v5.0 (protocol level 5).
    V5,
}

impl std::str::FromStr for ProtocolVersion {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "v4" | "3.1.1" | "311" | "mqtt311" | "mqtt3" => Ok(Self::V4),
            "v5" | "5.0" | "mqtt5" | "mqttv5" => Ok(Self::V5),
            other => Err(format!(
                "unknown protocol version '{other}' (use v4/3.1.1 or v5/5.0)"
            )),
        }
    }
}

impl ProtocolVersion {
    /// Wire protocol level sent in CONNECT.
    pub fn protocol_level(self) -> u8 {
        match self {
            Self::V4 => 4,
            Self::V5 => 5,
        }
    }

    /// Human-readable label for logs and CLI help.
    pub fn label(self) -> &'static str {
        match self {
            Self::V4 => "MQTT v3.1.1",
            Self::V5 => "MQTT v5.0",
        }
    }
}

/// Authentication credentials.
#[derive(Debug, Clone)]
pub struct Credentials {
    pub username: String,
    pub password: Option<Bytes>,
}

/// Last Will and Testament configuration.
#[derive(Debug, Clone)]
pub struct LastWill {
    pub topic: String,
    pub message: Bytes,
    pub qos: u8,
    pub retain: bool,
}

/// TLS options for the broker connection.
#[derive(Debug, Clone, Default)]
pub struct TlsOptions {
    /// Enable TLS (typically broker port 8883).
    pub enabled: bool,
    /// Skip certificate verification (tests / lab only).
    pub insecure_skip_verify: bool,
}

/// Automatic reconnect behaviour after an unexpected disconnect.
#[derive(Debug, Clone)]
pub struct ReconnectOptions {
    /// When `true`, the event loop reconnects after connection loss.
    pub enabled: bool,
    /// Delay before the first reconnect attempt.
    pub initial_delay: Duration,
    /// Upper bound for exponential backoff.
    pub max_delay: Duration,
    /// Maximum reconnect attempts; `None` means unlimited.
    pub max_attempts: Option<u32>,
}

impl Default for ReconnectOptions {
    fn default() -> Self {
        Self {
            enabled: true,
            initial_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            max_attempts: None,
        }
    }
}

/// Configuration for the MQTT client.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Broker address in the form `host:port`.
    pub broker_addr: String,
    /// Client identifier. If empty, a process-based id is generated.
    pub client_id: String,
    /// Protocol version to use.
    pub protocol_version: ProtocolVersion,
    /// Keep-alive interval in seconds.
    pub keep_alive: u16,
    /// Clean session (v3.1.1) / clean start (v5.0).
    pub clean_session: bool,
    /// Optional credentials.
    pub credentials: Option<Credentials>,
    /// Optional Last Will and Testament.
    pub last_will: Option<LastWill>,
    /// Connection timeout (TCP + TLS handshake + first CONNACK wait uses this for TCP).
    pub connect_timeout: Duration,
    /// Maximum number of in-flight QoS 1/2 publishes.
    pub max_inflight: u16,
    /// Retransmit unacked QoS 1/2 packets after this duration.
    pub ack_timeout: Duration,
    /// How long `publish` / `subscribe` / `unsubscribe` wait for the broker's
    /// acknowledgement before failing with [`crate::ClientError::Timeout`].
    pub request_timeout: Duration,
    /// TLS settings.
    pub tls: TlsOptions,
    /// Reconnect settings.
    pub reconnect: ReconnectOptions,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            broker_addr: "127.0.0.1:1883".to_string(),
            client_id: String::new(),
            protocol_version: ProtocolVersion::default(),
            keep_alive: 60,
            clean_session: true,
            credentials: None,
            last_will: None,
            connect_timeout: Duration::from_secs(5),
            max_inflight: 16,
            ack_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(30),
            tls: TlsOptions::default(),
            reconnect: ReconnectOptions::default(),
        }
    }
}

impl ClientConfig {
    /// Create a new configuration with the given broker address and client ID.
    pub fn new(broker_addr: impl Into<String>, client_id: impl Into<String>) -> Self {
        Self {
            broker_addr: broker_addr.into(),
            client_id: client_id.into(),
            ..Default::default()
        }
    }

    /// Set the protocol version.
    pub fn protocol_version(mut self, version: ProtocolVersion) -> Self {
        self.protocol_version = version;
        self
    }

    /// Set the keep-alive interval in seconds.
    pub fn keep_alive(mut self, seconds: u16) -> Self {
        self.keep_alive = seconds;
        self
    }

    /// Set clean session / clean start flag.
    pub fn clean_session(mut self, clean: bool) -> Self {
        self.clean_session = clean;
        self
    }

    /// Set authentication credentials.
    pub fn credentials(mut self, username: impl Into<String>, password: Option<Bytes>) -> Self {
        self.credentials = Some(Credentials {
            username: username.into(),
            password,
        });
        self
    }

    /// Set the Last Will and Testament.
    pub fn last_will(mut self, will: LastWill) -> Self {
        self.last_will = Some(will);
        self
    }

    /// Set the connection timeout.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set the maximum in-flight QoS 1/2 publishes.
    pub fn max_inflight(mut self, max: u16) -> Self {
        self.max_inflight = max.max(1);
        self
    }

    /// Set the QoS acknowledgement retransmit timeout.
    pub fn ack_timeout(mut self, timeout: Duration) -> Self {
        self.ack_timeout = timeout;
        self
    }

    /// Set how long operations wait for the broker's acknowledgement.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Enable or configure TLS.
    pub fn tls(mut self, tls: TlsOptions) -> Self {
        self.tls = tls;
        self
    }

    /// Configure automatic reconnect.
    pub fn reconnect(mut self, reconnect: ReconnectOptions) -> Self {
        self.reconnect = reconnect;
        self
    }
}
