# Broker 实现

> 状态: 已实现。入口是 `heron`，监听在 `heron_mqtt`，会话与路由在 `heron_core`，PostgreSQL 在 `heron_storage`，规则引擎在 `heron_rules`。

## 模块划分

| Crate | 职责 | 关键源码 |
|-------|------|----------|
| `heron` | 进程入口，YAML 配置，装配各监听与 API | `src/main.rs` |
| `heron_mqtt` | TCP / WebSocket / TLS(MQTTS) 监听，按 CONNECT 选择 v4/v5 编解码 | `server.rs`、`connection.rs`、`tls.rs` |
| `heron_core` | 会话、主题树与通配符匹配、QoS 0/1/2 路由、保留消息、`Storage` trait 及内存实现 | `broker.rs`、`topic.rs`、`qos.rs`、`memory.rs` |
| `heron_storage` | SeaORM + PostgreSQL 实现 `Storage`，连接时建表 | `postgres.rs`、`schema.rs` |
| `heron_rules` | 规则解析/执行与 Hook 插件 | `parse.rs`、`engine.rs`、`hook.rs` |
| `heron_api` | 管理 REST、健康检查、Prometheus、`/api/v1/events` WebSocket | `lib.rs`、`events.rs` |
| `heron_auth` | 用户名/密码、Token、客户端证书与 ACL | `lib.rs`、`topic.rs` |

## 连接生命周期

```text
accept -> decode CONNECT -> auth/authz -> create session -> subscribe/publish loop
        -> keep-alive PINGREQ/PINGRESP -> DISCONNECT 或异常断开 -> 会话回收/遗嘱
```

- 编解码错误、协议错误、认证错误与网络错误分别处理
- `BrokerHooks` 在连接、订阅与发布流程中调用认证、ACL 与规则钩子
- 每个连接是独立的 Tokio task

## 存储边界

`Storage` trait 定义在 `heron_core`，默认内存实现；`heron_storage::PostgresStorage` 通过 SeaORM 写入 PostgreSQL，进程启动时按 [schema.rs](../../crates/heron_storage/src/schema.rs) 建 `clients` / `sessions` / `subscriptions` / `retained_messages` / `messages` / `rules` 表。表结构设计见 dev 的[数据库设计](../dev/05-database-design.md)。

## 测试

- 单元测试: 主题匹配与 QoS 状态流转（`heron_core`）
- 集成测试: CONNECT / SUBSCRIBE / PUBLISH 基本路径（`heron_mqtt`、`heron_core`）
- 存储 round-trip 需要 PostgreSQL，测 `heron_storage` 时传入可用的数据库 URL
