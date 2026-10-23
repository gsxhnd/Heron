# Heron 开发文档

Heron 当前已经包含单节点 MQTT Broker、认证与 ACL、管理 REST API，以及 `mqtt_codec` 与 `mqtt_client`。规则引擎、Web GUI 和集群仍在设计中。

## 文档索引

### 概述与规划

- [项目概述](01-project-overview.md) — 目标、能力矩阵与当前状态
- [需求分析](02-requirements.md) — 功能与非功能需求
- [系统架构](03-architecture.md) — 模块划分、数据流与部署架构
- [路线图](04-roadmap.md) — 版本规划与里程碑

### 开发指南

- [快速开始](05-quick-start.md) — 获取代码、构建与测试
- [编解码实现](06-codec-implementation.md) — mqtt_codec 模块说明
- [Broker 实现](07-broker-implementation.md) — Broker 模块设计
- [插件开发](08-plugin-development.md) — 认证、授权、钩子插件

### 接口与数据

- [API 设计](09-api-design.md) — REST API 与 gRPC 接口
- [数据库设计](10-database-design.md) — PostgreSQL 表结构
- [UI 设计](11-ui-design.md) — Web GUI 设计规范

## 外部资源

- [MQTT 3.1.1 规范](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/mqtt-v3.1.1.html)
- [MQTT 5.0 规范](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html)
- [Rust 官方文档](https://doc.rust-lang.org/)
- [Tokio 文档](https://tokio.rs/)
