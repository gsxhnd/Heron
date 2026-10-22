# Heron

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Heron 是一个使用 Rust 编写的 MQTT 项目，目标是逐步演进为完整的 Broker、API、插件系统和管理界面。

## 当前状态

当前仓库已经落地的模块包括：

| Crate | 说明 |
|-------|------|
| `heron_codec` | MQTT v3.1.1 / v5.0 控制报文编解码与协议校验 |
| `heron_client` | 异步 MQTT 客户端（TCP/TLS、自动重连、QoS 0/1/2、v3.1.1 与 v5.0） |
| `heron_core` | 会话、主题树、QoS 0/1/2 路由与存储接口 |
| `heron_mqtt` | TCP 与 WebSocket 接入 |
| `heron_storage` | SeaORM + PostgreSQL 持久化 |
| `heron` | 单节点 Broker 进程 |

- 已实现：单节点 Broker（TCP/WebSocket、会话、主题、QoS 0/1/2、保留消息、遗嘱、内存或 PostgreSQL 持久化）
- 已实现：`heron_codec` 编解码，以及 `heron_client` 发布/订阅、TLS、自动重连
- 规划中：认证与 ACL、REST API、Web GUI、Docker 与集群

```bash
cargo run -p heron
cargo run -p heron_client -- sub -H 127.0.0.1 -p 1883 -t test/#
```

## 快速开始

```bash
git clone https://github.com/gsxhnd/Heron.git
cd Heron
cargo build --workspace
cargo test --workspace
```

使用命令行客户端前，先在另一个终端启动 Broker：

```bash
cargo run -p heron
cargo run -p heron_client -- pub -t test/topic -m hello -q 1
cargo run -p heron_client -- --protocol v5 sub -H 127.0.0.1 -t test/#
```

更详细的说明见 [docs/dev/README.md](docs/dev/README.md) 与 [crates/heron_codec/README.md](crates/heron_codec/README.md)。

## 文档入口

- [开发文档索引](docs/dev/README.md)
- [项目概述](docs/dev/01-project-overview.md)
- [系统架构](docs/dev/03-architecture.md)
- [快速开始](docs/dev/05-quick-start.md)

## 当前项目结构

```text
Heron/
├── crates/
│   ├── heron_codec/          # MQTT 编解码库
│   ├── heron_client/         # MQTT 客户端库（库 + CLI + 集成测试）
│   ├── heron_core/    # 会话、主题树与路由
│   ├── heron_mqtt/    # TCP / WebSocket
│   ├── heron_storage/ # SeaORM + PostgreSQL
│   └── heron/         # Broker 进程
├── conf/broker.yaml         # 示例配置
├── docs/dev/                # 设计与开发文档
└── Cargo.toml               # workspace 配置
```

## 目标技术方向

- 后端: Rust、Tokio、Axum、Tonic、SeaORM、PostgreSQL
- 前端: Vue 3、Reka UI、Pinia、Vite

## 贡献

欢迎贡献，开始前建议先阅读开发文档。

## License

本项目采用 [MIT License](LICENSE) 授权。

## 相关链接

- [MQTT 3.1.1 规范](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html)
- [MQTT 5.0 规范](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html)
