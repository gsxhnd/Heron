# 项目概述

> 状态: 单节点 Broker、认证授权、管理 API 和 Web GUI 已可运行。规则引擎与集群仍在规划中。

## 项目简介

Heron 是一个使用 Rust 编写的 MQTT Broker 与管理平台，覆盖协议处理、Broker 服务、API、插件系统、可视化界面和集群能力。

## 目标能力

- 完整的 MQTT v3.1.1 和 v5.0 协议支持
- 高性能、低延迟的消息代理服务
- API、插件和规则引擎扩展点
- 可视化管理界面
- 集群部署和高可用

## 能力状态矩阵

| 能力 | 说明 | 状态 |
|------|------|------|
| MQTT v3.1.1 编解码 | 协议包定义、编码、解码、校验 | 已实现 |
| MQTT v5.0 编解码 | 属性系统、编码、解码、校验 | 已实现 |
| Broker 核心 | TCP 接入、会话、主题、QoS 0/1/2 路由 | 已实现 |
| WebSocket | MQTT over WebSocket（子协议 `mqtt`） | 已实现 |
| 持久化 | 内存存储，以及 SeaORM + PostgreSQL | 已实现 |
| REST API | 客户端、主题、订阅、系统状态、健康检查、Prometheus | 已实现 |
| 插件系统 | 用户名/密码、Token、证书认证与 ACL。钩子仍未实现 | 部分实现 |
| Web GUI | React + shadcn/ui 管理界面 | 已实现 |
| Raft 集群 | 节点协同与高可用 | 规划中 |

## 当前仓库结构

```text
Heron/
├── crates/
│   ├── mqtt_codec/            # 已实现
│   ├── mqtt_client/           # 已实现
│   ├── heron_core/      # 会话、主题树、路由
│   ├── heron_auth/      # 认证与 ACL
│   ├── heron_mqtt/      # TCP / WebSocket / TLS
│   ├── heron_api/       # REST、健康检查、Prometheus
│   ├── heron_storage/   # SeaORM + PostgreSQL
│   └── heron/           # 可执行入口
├── conf/broker.yaml           # 示例配置
├── docs/                      # 项目文档
└── Cargo.toml
```

## 目标仓库结构

```text
Heron/
├── crates/
│   ├── mqtt_codec/
│   ├── mqtt_client/
│   ├── heron_core/
│   ├── heron_auth/
│   ├── heron_rules/
│   ├── heron_api/
│   ├── heron_cluster/
│   └── heron_web/
└── docs/
```

## 当前可以做什么

- 运行 `cargo test -p mqtt_codec` 验证编解码实现
- 启动单节点 Broker：`cargo run -p heron -- --config conf/broker.yaml`
- 在配置中启用认证、TLS 和管理 API 后调用 `/health`、`/api/v1` 与 `/metrics`
- 阅读 `mqtt_codec/README.md` 了解 Builder、Codec 和校验接口

## 当前还不能做什么

- 使用 Docker、集群或 Web 管理界面
- 使用规则引擎或钩子插件
