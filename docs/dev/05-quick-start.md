# 快速开始

> 当前仓库可以构建并启动单节点 Broker。认证、TLS 和管理 API 通过 `conf/broker.yaml` 启用。Web GUI 仍未实现。

## 环境要求

- Rust 1.85+（edition 2024）
- 使用 PostgreSQL 持久化时需要 PostgreSQL 14+

## 获取代码

```bash
git clone https://github.com/gsxhnd/Heron.git
cd Heron
```

## 构建与测试

```bash
cargo build --workspace
cargo test --workspace
```

## 启动 Broker

默认使用内存存储，TCP `0.0.0.0:1883`，WebSocket `0.0.0.0:8083`：

```bash
cargo run -p heron
cargo run -p heron -- --config conf/broker.yaml
```

PostgreSQL 在配置里把 `storage.kind` 设为 `postgres` 并填写 `storage.url`。进程启动时会创建会话相关的表。

认证、MQTTS 和管理 API 默认关闭。在 `conf/broker.yaml` 中设置 `auth`、`tls`/`mqtts` 和 `api.enabled`。管理接口示例：

```bash
curl -s http://127.0.0.1:8080/health
curl -s -H "Authorization: Bearer change-me" http://127.0.0.1:8080/api/v1/system/status
curl -s -H "Authorization: Bearer change-me" http://127.0.0.1:8080/metrics
```

用仓库里的客户端连接本机 Broker：

```bash
cargo run -p mqtt_client -- sub -H 127.0.0.1 -p 1883 -t test/#
cargo run -p mqtt_client -- pub -H 127.0.0.1 -p 1883 -t test/topic -m hello -q 1
```

## 了解更多

- `mqtt_codec/README.md` — Builder、Codec 和校验接口
- `mqtt_codec/examples/` — 使用示例
- [编解码实现](06-codec-implementation.md) — 模块架构与核心 Trait
