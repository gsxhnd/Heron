# MQTT 客户端使用说明

> 完整的能力矩阵、操作语义与库示例见 [crates/mqtt_client/README.md](../../crates/mqtt_client/README.md)，本文是入口与速查。

## 库用法（简版）

```rust
use mqtt_client::{ClientConfig, Event, MqttClient, ProtocolVersion};

#[tokio::main]
async fn main() -> Result<(), mqtt_client::ClientError> {
    let config = ClientConfig::new("127.0.0.1:1883", "demo-1")
        .protocol_version(ProtocolVersion::V5)
        .keep_alive(30);

    let (client, mut events) = MqttClient::new(config);

    // 等待连接就绪
    while let Some(event) = events.recv().await {
        if matches!(event, Event::Connected { .. }) {
            break;
        }
    }

    client.subscribe(vec![("test/#".into(), 1)]).await?;
    client.publish("test/topic", "hello", 1, false).await?;
    client.disconnect().await.ok();
    Ok(())
}
```

入站消息、重连与 ACK 通过 `events` 流投递；`publish`/`subscribe`/`unsubscribe` 都等待 Broker 确认，超时由 `ClientConfig::request_timeout` 控制，连接中断时立即返回 `ClientError::Disconnected`。

## CLI 速查

```bash
cargo run -p mqtt_client -- pub -t test/topic -m hello -q 1
cargo run -p mqtt_client -- sub -H 127.0.0.1 -p 1883 -t 'test/#'
cargo run -p mqtt_client -- pub -t test/topic -m hello -n 5 --interval 500
cargo run -p mqtt_client -- --protocol v5 sub -H 127.0.0.1 -t 'test/#' -t 'other/+'
```

常用参数：

| 参数 | 说明 |
|------|------|
| `-H` / `-p` | Broker 地址 / 端口（默认 `127.0.0.1:1883`） |
| `-u` / `-P` | 用户名 / 密码 |
| `-q` | QoS 0/1/2 |
| `--protocol v5` | 使用 MQTT 5.0（默认 `v4`） |
| `--tls` / `--tls-insecure` | 启用 TLS / 跳过证书校验（仅用于实验） |
| `--keep-alive` / `--clean` | Keep-alive 秒数 / 请求 Clean Session（默认开启） |
| `--request-timeout` / `--timeout` | 等待确认 / 连接超时秒数 |
| `--no-reconnect` | 关闭自动重连 |

`pub` 另有 `-n`（发布条数）、`--interval`（重发间隔毫秒）、`--retain`；`sub` 可重复 `-t` 订阅多个过滤器，并用 `-q` 指定 QoS。

完整参数用 `cargo run -p mqtt_client -- pub --help` 查看。

## 集成测试

客户端的集成测试直接对 `tests/support` 里的进程内 mock Broker 跑真实 TCP + `mqtt_codec`：

```bash
cargo test -p mqtt_client
```
