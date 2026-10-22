# 快速开始

> 当前仓库可以构建并启动单节点 Broker。认证、REST API 与 Web GUI 仍未实现。

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

PostgreSQL 在配置里把 `storage.kind` 设为 `postgres` 并填写 `storage.url`。进程启动时会创建 v0.2.0 所需的表。

用仓库里的客户端连接本机 Broker：

```bash
cargo run -p heron_client -- sub -H 127.0.0.1 -p 1883 -t test/#
cargo run -p heron_client -- pub -H 127.0.0.1 -p 1883 -t test/topic -m hello -q 1
```

## 了解更多

- `heron_codec/README.md` — Builder、Codec 和校验接口
- `heron_codec/examples/` — 使用示例
- [编解码实现](06-codec-implementation.md) — 模块架构与核心 Trait
