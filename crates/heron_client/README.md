# heron_client

异步 MQTT 客户端，支持 **MQTT v3.1.1** 与 **MQTT v5.0**，基于 `heron_codec`。

## 能力

| 能力 | 说明 |
|------|------|
| TCP / TLS | `TlsOptions`；CLI `--tls` / `--tls-insecure` |
| 自动重连 | 指数退避；`ReconnectOptions`；CLI `--no-reconnect` |
| QoS 0/1/2 | 出站 inflight 窗口、`max_inflight`、超时重传（DUP） |
| 会话 | clean session / clean start；重连后自动重订阅与重传 |
| 认证 / Will | 用户名密码、Last Will |
| 请求-响应 | `publish` / `subscribe` / `unsubscribe` 等待 broker 确认并返回结果 |
| 事件 | `Connected` / `Reconnecting` / `Message` / ACK / `Disconnected` |

## 操作语义

三个操作都是**等待确认**的，超时由 `ClientConfig::request_timeout` 控制：

| 方法 | 何时返回 | 返回值 / 错误 |
|------|----------|---------------|
| `publish(qos=0)` | 报文写入 socket 后 | `Ok(())` |
| `publish(qos=1)` | 收到 PUBACK | `Ok(())` |
| `publish(qos=2)` | 收到 PUBCOMP | `Ok(())` |
| `subscribe` | 收到 SUBACK | `Vec<u8>`：每个过滤器的授权 QoS / reason code；被拒时 `SubscribeRejected` |
| `unsubscribe` | 收到 UNSUBACK | `Ok(())`；v5 失败时 `UnsubscribeRejected` |

连接中断时，尚未确认的操作会立即返回 `ClientError::Disconnected`，不会一直挂起。
入站消息仍然通过事件流（`Event::Message`）投递。

其他行为细节：

- **CONNACK 超时**：`connect_timeout` 同时覆盖握手，broker 不应答会直接判定连接失败
- **keep-alive**：空闲时按 keep-alive 发送 PINGREQ；超过 1.5 × keep-alive 收不到任何报文即判定断线
- **v5 服务端属性**：采纳 `Server Keep Alive`、`Receive Maximum`（收敛 inflight 窗口）、`Assigned Client Identifier`
- **SUBACK 失败码**：被拒的过滤器会从本地订阅表中移除，避免重连后重复订阅无效主题

## 库用法

```rust
use std::time::Duration;
use heron_client::{ClientConfig, Event, MqttClient, ProtocolVersion, TlsOptions};

#[tokio::main]
async fn main() -> Result<(), heron_client::ClientError> {
    let config = ClientConfig::new("broker.emqx.io:8883", "demo")
        .protocol_version(ProtocolVersion::V5)
        .tls(TlsOptions {
            enabled: true,
            insecure_skip_verify: false,
        })
        .max_inflight(16)
        .ack_timeout(Duration::from_secs(30))
        .request_timeout(Duration::from_secs(20));

    let (client, mut events) = MqttClient::new(config);

    // 等待连接就绪
    while let Some(event) = events.recv().await {
        if matches!(event, Event::Connected { .. }) {
            break;
        }
    }

    // subscribe 直接返回授权结果
    let granted = client.subscribe(vec![("test/#".into(), 1)]).await?;
    println!("granted: {granted:?}");

    // QoS 1 发布会等到 PUBACK
    client.publish("test/topic", "hello", 1, false).await?;

    // 入站消息走事件流
    tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            if let Event::Message { topic, payload, .. } = event {
                println!("{topic}: {}", String::from_utf8_lossy(&payload));
            }
        }
    });

    client.unsubscribe(vec!["test/#".into()]).await?;
    client.disconnect().await.ok();
    Ok(())
}
```

## CLI

```bash
# 发布（QoS 1，等 PUBACK）
cargo run -p heron_client -- pub -t test/topic -m hello -q 1

# 连续发布 5 条，间隔 500ms
cargo run -p heron_client -- pub -t test/topic -m hello -n 5 --interval 500

# 订阅多个主题
cargo run -p heron_client -- --protocol v5 sub -H 127.0.0.1 -t 'test/#' -t 'other/+'

# TLS + v5
cargo run -p heron_client -- --tls -H broker.emqx.io -p 8883 --protocol v5 sub -t test/#
```

常用全局参数：`--protocol`、`-H/--host`、`-p/--port`、`--client-id`、`-u/-P`（用户名密码）、
`--keep-alive`、`--clean`、`--timeout`、`--request-timeout`、`--tls`、`--tls-insecure`、`--no-reconnect`。

## 测试

```bash
cargo test -p heron_client
cargo clippy -p heron_client --all-targets -- -D warnings
```

集成测试位于 `tests/`，通过 `tests/support` 中的内存 mock broker（真实 TCP + `heron_codec`）驱动：

- `tests/v4.rs`：v3.1.1 的连接、QoS 0/1/2 发布、订阅/收消息/退订、订阅被拒、CONNACK 超时、keep-alive、重连重订阅
- `tests/v5.rs`：v5.0 的同样场景，外加 Server Keep Alive、Assigned Client Identifier、Receive Maximum、UNSUBACK 失败码
