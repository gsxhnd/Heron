//! TCP and WebSocket listeners.

use std::future::Future;
use std::net::SocketAddr;

use heron_core::Broker;
use rustls::pki_types::CertificateDer;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::tungstenite::handshake::server::Request;
use tokio_tungstenite::tungstenite::http::{HeaderValue, Response, StatusCode, header};

use crate::connection::{TcpStreamIo, WebSocketIo, serve_connection};
use crate::tls::certificate_identity;

/// Bound MQTT listeners.
pub struct Server {
    broker: Broker,
    tcp: Option<TcpListener>,
    websocket: Option<TcpListener>,
    mqtts: Option<SecureListener>,
    wss: Option<SecureListener>,
}

struct SecureListener {
    listener: TcpListener,
    acceptor: TlsAcceptor,
}

#[derive(Clone, Copy)]
enum SecureKind {
    Mqtt,
    WebSocket,
}

impl Server {
    /// Bind the requested listeners. `None` skips that transport.
    pub async fn bind(
        broker: Broker,
        tcp: Option<SocketAddr>,
        websocket: Option<SocketAddr>,
    ) -> std::io::Result<Self> {
        let tcp = match tcp {
            Some(addr) => Some(TcpListener::bind(addr).await?),
            None => None,
        };
        let websocket = match websocket {
            Some(addr) => Some(TcpListener::bind(addr).await?),
            None => None,
        };
        Ok(Self {
            broker,
            tcp,
            websocket,
            mqtts: None,
            wss: None,
        })
    }

    /// Bind MQTTS and WSS listeners that share one TLS acceptor.
    pub async fn bind_secure(
        &mut self,
        mqtts: Option<SocketAddr>,
        wss: Option<SocketAddr>,
        acceptor: TlsAcceptor,
    ) -> std::io::Result<()> {
        if let Some(addr) = mqtts {
            self.mqtts = Some(SecureListener {
                listener: TcpListener::bind(addr).await?,
                acceptor: acceptor.clone(),
            });
        }
        if let Some(addr) = wss {
            self.wss = Some(SecureListener {
                listener: TcpListener::bind(addr).await?,
                acceptor,
            });
        }
        Ok(())
    }

    /// Local TCP address after the kernel assigns a port.
    pub fn tcp_addr(&self) -> Option<SocketAddr> {
        self.tcp
            .as_ref()
            .and_then(|listener| listener.local_addr().ok())
    }

    /// Local WebSocket address after the kernel assigns a port.
    pub fn websocket_addr(&self) -> Option<SocketAddr> {
        self.websocket
            .as_ref()
            .and_then(|listener| listener.local_addr().ok())
    }

    /// Local MQTTS address.
    pub fn mqtts_addr(&self) -> Option<SocketAddr> {
        self.mqtts
            .as_ref()
            .and_then(|listener| listener.listener.local_addr().ok())
    }

    /// Local WSS address.
    pub fn wss_addr(&self) -> Option<SocketAddr> {
        self.wss
            .as_ref()
            .and_then(|listener| listener.listener.local_addr().ok())
    }

    /// Accept connections until `shutdown` completes.
    pub async fn serve(self, shutdown: impl Future<Output = ()> + Send) -> std::io::Result<()> {
        let (stop_tx, stop_rx) = watch::channel(false);
        tokio::pin!(shutdown);
        let tcp = self.tcp;
        let websocket = self.websocket;
        let mqtts = self.mqtts;
        let wss = self.wss;
        let broker = self.broker;

        let tcp_task = async {
            if let Some(listener) = tcp {
                accept_tcp(listener, broker.clone(), stop_rx.clone()).await;
            }
        };
        let ws_task = async {
            if let Some(listener) = websocket {
                accept_ws(listener, broker.clone(), stop_rx.clone()).await;
            }
        };
        let mqtts_task = async {
            if let Some(listener) = mqtts {
                accept_tls(listener, broker.clone(), stop_rx.clone(), SecureKind::Mqtt).await;
            }
        };
        let wss_task = async {
            if let Some(listener) = wss {
                accept_tls(
                    listener,
                    broker.clone(),
                    stop_rx.clone(),
                    SecureKind::WebSocket,
                )
                .await;
            }
        };

        tokio::select! {
            _ = shutdown => {
                let _ = stop_tx.send(true);
            }
            _ = async {
                tokio::join!(tcp_task, ws_task, mqtts_task, wss_task);
            } => {}
        }
        Ok(())
    }
}

async fn accept_tcp(listener: TcpListener, broker: Broker, mut stop: watch::Receiver<bool>) {
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            accepted = listener.accept() => {
                let Ok((stream, peer)) = accepted else {
                    continue;
                };
                let _ = stream.set_nodelay(true);
                let broker = broker.clone();
                tokio::spawn(async move {
                    let mut io = TcpStreamIo::new(stream);
                    if let Err(err) = serve_connection(&mut io, broker, Some(peer.to_string()), None).await {
                        tracing::debug!(%err, %peer, "TCP 连接关闭");
                    }
                });
            }
        }
    }
}

async fn accept_ws(listener: TcpListener, broker: Broker, mut stop: watch::Receiver<bool>) {
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            accepted = listener.accept() => {
                let Ok((stream, peer)) = accepted else {
                    continue;
                };
                let broker = broker.clone();
                tokio::spawn(async move {
                    if let Err(err) = serve_ws(stream, broker, peer, None).await {
                        tracing::debug!(%err, %peer, "WebSocket 连接关闭");
                    }
                });
            }
        }
    }
}

async fn accept_tls(
    secure: SecureListener,
    broker: Broker,
    mut stop: watch::Receiver<bool>,
    kind: SecureKind,
) {
    loop {
        tokio::select! {
            _ = stop.changed() => break,
            accepted = secure.listener.accept() => {
                let Ok((stream, peer)) = accepted else {
                    continue;
                };
                let _ = stream.set_nodelay(true);
                let acceptor = secure.acceptor.clone();
                let broker = broker.clone();
                tokio::spawn(async move {
                    let Ok(tls) = acceptor.accept(stream).await else {
                        return;
                    };
                    let identity = peer_identity(&tls);
                    if let Err(err) = match kind {
                        SecureKind::Mqtt => {
                            let mut io = TcpStreamIo::new(tls);
                            serve_connection(&mut io, broker, Some(peer.to_string()), identity).await
                        }
                        SecureKind::WebSocket => serve_ws(tls, broker, peer, identity).await,
                    } {
                        tracing::debug!(%err, %peer, "TLS 连接关闭");
                    }
                });
            }
        }
    }
}

fn peer_identity(stream: &tokio_rustls::server::TlsStream<TcpStream>) -> Option<String> {
    let (_, connection) = stream.get_ref();
    connection
        .peer_certificates()
        .and_then(|certs| certs.first())
        .and_then(identity_of)
}

fn identity_of(cert: &CertificateDer<'_>) -> Option<String> {
    certificate_identity(cert.as_ref())
}

async fn serve_ws<S>(
    stream: S,
    broker: Broker,
    peer: SocketAddr,
    certificate_identity: Option<String>,
) -> std::io::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    let socket = tokio_tungstenite::accept_hdr_async(stream, require_mqtt_subprotocol)
        .await
        .map_err(std::io::Error::other)?;
    let mut io = WebSocketIo::new(socket);
    serve_connection(
        &mut io,
        broker,
        Some(peer.to_string()),
        certificate_identity,
    )
    .await
}

#[expect(
    clippy::result_large_err,
    reason = "tungstenite 的握手回调要求错误类型为完整的 HTTP Response"
)]
fn require_mqtt_subprotocol(
    request: &Request,
    mut response: Response<()>,
) -> Result<Response<()>, Response<Option<String>>> {
    let offers_mqtt = request
        .headers()
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|part| part.trim().eq_ignore_ascii_case("mqtt"))
        });
    if !offers_mqtt {
        let mut rejected = Response::new(None);
        *rejected.status_mut() = StatusCode::BAD_REQUEST;
        return Err(rejected);
    }
    let Ok(protocol) = HeaderValue::from_str("mqtt") else {
        let mut rejected = Response::new(None);
        *rejected.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
        return Err(rejected);
    };
    response
        .headers_mut()
        .insert(header::SEC_WEBSOCKET_PROTOCOL, protocol);
    Ok(response)
}
