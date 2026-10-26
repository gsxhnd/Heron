# Heron

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Heron 是一个使用 Rust 编写的 MQTT 项目，目标是逐步演进为完整的 Broker、API、插件系统和管理界面。

## 当前状态

当前仓库已经落地的模块包括：

| Crate | 说明 |
|-------|------|
| `mqtt_codec` | MQTT v3.1.1 / v5.0 控制报文编解码与协议校验 |
| `mqtt_client` | 异步 MQTT 客户端（TCP/TLS、自动重连、QoS 0/1/2、v3.1.1 与 v5.0） |
| `heron_core` | 会话、主题树、QoS 0/1/2 路由与存储接口 |
| `heron_mqtt` | TCP 与 WebSocket 接入 |
| `heron_auth` | 用户名/密码、Token、客户端证书与 ACL |
| `heron_api` | REST 管理接口、健康检查、Prometheus |
| `heron_storage` | SeaORM + PostgreSQL 持久化 |
| `heron` | 单节点 Broker 进程 |

- 已实现：单节点 Broker（TCP/WebSocket/TLS、会话、主题、QoS 0/1/2、保留消息、遗嘱、内存或 PostgreSQL 持久化）
- 已实现：用户名/密码、Token、客户端证书认证与 ACL；管理 REST API、健康检查和 Prometheus
- 已实现：`mqtt_codec` 编解码，以及 `mqtt_client` 发布/订阅、TLS、自动重连
- 已实现：`web/` 管理界面（React、Zustand、TanStack Router、shadcn/ui）
- 已实现：规则引擎（SQL 与条件表达式、republish/transform/webhook 等动作、生命周期钩子、Storage 持久化）
- 规划中：Docker 与集群（Raft）

```bash
cargo run -p heron
cargo run -p mqtt_client -- sub -H 127.0.0.1 -p 1883 -t test/#
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
cargo run -p mqtt_client -- pub -t test/topic -m hello -q 1
cargo run -p mqtt_client -- --protocol v5 sub -H 127.0.0.1 -t test/#
```

更详细的说明见 [docs/wiki](docs/wiki/README.md)（使用与实现）与 [docs/dev](docs/dev/README.md)（计划与设计）。

## 文档入口

- [wiki 索引](docs/wiki/README.md) — 快速开始、MQTT 客户端、管理 API、Web GUI 与实现说明
- [开发文档索引](docs/dev/README.md) — 需求、架构、路线图与设计规范
- [快速开始](docs/wiki/01-quick-start.md)
- [系统架构](docs/dev/03-architecture.md)

## 当前项目结构

```text
Heron/
├── crates/
│   ├── mqtt_codec/          # MQTT 编解码库
│   ├── mqtt_client/         # MQTT 客户端库（库 + CLI + 集成测试）
│   ├── heron_core/    # 会话、主题树与路由
│   ├── heron_mqtt/    # TCP / WebSocket
│   ├── heron_storage/ # SeaORM + PostgreSQL
│   └── heron/         # Broker 进程
├── conf/broker.yaml         # 示例配置
├── docs/dev/                # 开发计划与设计文档
├── docs/wiki/               # 使用与实现文档
└── Cargo.toml               # workspace 配置
```

## 目标技术方向

- 后端: Rust、Tokio、Axum、Tonic、SeaORM、PostgreSQL
- 前端: React、Zustand、TanStack Router、shadcn/ui、Vite

## 贡献

欢迎贡献，开始前建议先阅读开发文档。

## License

本项目采用 [MIT License](LICENSE) 授权。

## 相关链接

- [MQTT 3.1.1 规范](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html)
- [MQTT 5.0 规范](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html)
