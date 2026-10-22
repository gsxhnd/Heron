//! TCP and WebSocket listeners.

use std::future::Future;
use std::net::SocketAddr;

use heron_core::Broker;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::handshake::server::Request;
use tokio_tungstenite::tungstenite::http::{HeaderValue, Response, StatusCode, header};

use crate::connection::{TcpStreamIo, WebSocketIo, serve_connection};

/// Bound MQTT listeners.
pub struct Server {
    broker: Broker,
    tcp: Option<TcpListener>,
    websocket: Option<TcpListener>,
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
        })
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

    /// Accept connections until `shutdown` completes.
    pub async fn serve(self, shutdown: impl Future<Output = ()> + Send) -> std::io::Result<()> {
        let (stop_tx, stop_rx) = watch::channel(false);
        tokio::pin!(shutdown);
        let tcp = self.tcp;
        let websocket = self.websocket;
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

        tokio::select! {
            _ = shutdown => {
                let _ = stop_tx.send(true);
            }
            _ = async {
                tokio::join!(tcp_task, ws_task);
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
                    if let Err(err) = serve_connection(&mut io, broker, Some(peer.to_string())).await {
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
                    if let Err(err) = serve_ws(stream, broker, peer).await {
                        tracing::debug!(%err, %peer, "WebSocket 连接关闭");
                    }
                });
            }
        }
    }
}

async fn serve_ws(stream: TcpStream, broker: Broker, peer: SocketAddr) -> std::io::Result<()> {
    let socket = tokio_tungstenite::accept_hdr_async(stream, require_mqtt_subprotocol)
        .await
        .map_err(std::io::Error::other)?;
    let mut io = WebSocketIo::new(socket);
    serve_connection(&mut io, broker, Some(peer.to_string())).await
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
