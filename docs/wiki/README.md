# Heron Wiki

本目录收录**使用说明与实现说明**：如何构建、启动、连接、配置，以及各模块的实现细节。

开发相关的计划与设计（需求、架构设计、路线图、数据库与 UI 设计规范）见 [docs/dev](../dev/README.md)。

## 使用说明

- [快速开始](01-quick-start.md) — 获取代码、构建测试、启动 Broker 与 Web GUI
- [MQTT 客户端](02-mqtt-client.md) — `mqtt_client` 库与 CLI 用法
- [管理 API](03-management-api.md) — REST 与 WebSocket 接口
- [Web GUI](04-web-gui.md) — 管理界面页面与操作

## 实现说明

- [协议编解码](05-implementation-codec.md) — `mqtt_codec` 模块结构
- [Broker 实现](06-implementation-broker.md) — 接入、会话、路由与存储
- [认证与插件](07-implementation-auth-plugins.md) — Authenticator / Authorizer / Hook

## 外部资源

- [MQTT 3.1.1 规范](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html)
- [MQTT 5.0 规范](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html)
- [Rust 官方文档](https://doc.rust-lang.org/)
- [Tokio 文档](https://tokio.rs/)

## 相关 crate README

- [crates/mqtt_codec/README.md](../../crates/mqtt_codec/README.md) — Builder、Codec 与校验接口、示例
- [crates/mqtt_client/README.md](../../crates/mqtt_client/README.md) — 客户端能力、操作语义与完整示例
