# 项目概述

> 状态: 单节点 Broker、认证授权、管理 API、Web GUI 与规则引擎已可运行。集群仍在规划中。

## 项目简介

Heron 是一个使用 Rust 编写的 MQTT Broker 与管理平台，覆盖协议处理、Broker 服务、认证授权、规则引擎、管理 API 与可视化界面。

## 目标能力

- 完整的 MQTT v3.1.1 和 v5.0 协议支持
- 高性能、低延迟的消息代理服务
- API、插件和规则引擎扩展点
- 可视化管理界面
- 集群部署和高可用

## 能力状态矩阵

| 能力 | 说明 | 状态 |
|------|------|------|
| MQTT v3.1.1 / v5.0 编解码 | 协议包定义、编码、解码、校验、属性系统 | 已实现 |
| Broker 核心 | TCP/WebSocket/MQTTS 接入、会话、主题、QoS 0/1/2 路由 | 已实现 |
| 持久化 | 内存存储，以及 SeaORM + PostgreSQL | 已实现 |
| 认证授权 | 用户名/密码、Token、客户端证书与 ACL（YAML 配置） | 已实现 |
| 规则引擎 | SQL 规则、动作、生命周期钩子、持久化 | 已实现 |
| REST API | 客户端、主题、订阅、规则、系统状态、健康检查、Prometheus | 已实现 |
| Web GUI | React + shadcn/ui 管理界面 | 已实现 |
| `mqtt_client` | 异步 TCP/TLS 客户端（v3.1.1 / v5.0） | 已实现 |
| 插件系统 | Hook trait 已实现；动态加载（进程外/热加载）未实现 | 部分实现 |
| Raft 集群 | 节点协同与高可用 | 规划中 |

## 仓库结构

```text
Heron/
├── crates/
│   ├── mqtt_codec/      # MQTT v3.1.1 / v5.0 编解码（已实现）
│   ├── mqtt_client/     # 异步 MQTT 客户端库 + CLI（已实现）
│   ├── heron_core/      # 会话、主题树、路由、Storage 接口
│   ├── heron_mqtt/      # TCP / WebSocket / TLS 监听
│   ├── heron_auth/      # 认证与 ACL
│   ├── heron_rules/     # 规则引擎与 Hook 插件
│   ├── heron_api/       # REST、健康检查、Prometheus、管理 WebSocket
│   ├── heron_storage/   # SeaORM + PostgreSQL
│   └── heron/           # 可执行入口
├── conf/broker.yaml     # 示例配置
├── web/                 # React 管理界面
├── docs/dev/            # 开发计划与设计文档
├── docs/wiki/           # 使用与实现文档
└── Cargo.toml
```

## 当前可以做什么

- 运行 `cargo test --workspace` 验证全部模块
- 启动单节点 Broker：`cargo run -p heron -- --config conf/broker.yaml`
- 在配置中启用认证、TLS 和管理 API 后调用 `/health`、`/api/v1` 与 `/metrics`
- 启动 `web/` 管理界面并在浏览器中管理 Broker

## 当前还不能做什么

- 使用 Docker 或集群

## 文档导航

- 计划与设计: [需求分析](02-requirements.md)、[系统架构](03-architecture.md)、[路线图](04-roadmap.md)、[数据库设计](05-database-design.md)、[UI 设计](06-ui-design.md)
- 使用与实现: [docs/wiki](../wiki/README.md)
