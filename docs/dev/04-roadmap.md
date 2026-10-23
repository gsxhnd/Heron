# 路线图

> 状态: v0.4.0 管理 API 已落地。10,000+ 连接、P99 < 50ms、覆盖率 > 80% 仍是质量目标，尚未压测。

## 版本规划

### v0.1.0 - 协议基础 ✅

已完成: 项目初始化、MQTT v3.1.1 与 v5.0 协议编解码 (含属性系统)

---

### v0.2.0 - 单节点 Broker ✅

已落地: TCP 与 WebSocket 接入、会话（Clean Session / 持久会话、Keep Alive、遗嘱、会话接管）、主题树与 `+`/`#` 匹配、QoS 0/1/2 路由、保留消息，以及 SeaORM + PostgreSQL 持久化（默认也可使用内存存储）。

质量目标: 10,000+ 并发连接，P99 < 50ms，测试覆盖率 > 80%。功能测试已覆盖连接、订阅、QoS 与会话恢复；上述性能和覆盖率指标尚未测量。

---

### v0.3.0 - 安全与认证 ✅

已落地: TLS 1.2/1.3（MQTTS 与 WSS）、用户名/密码、CONNECT 密码形式的 Token、已校验客户端证书身份，以及按优先级匹配的 ACL。默认不启用认证，行为与 v0.2.0 相同。用户、Token 和 ACL 来自 YAML，而不是数据库表。

---

### v0.4.0 - API 接口 ✅

已落地: Axum REST API。`GET /health` 与 `GET /api/v1/system/health` 不鉴权。其余 `/api/v1` 路由和 `GET /metrics` 在配置了 bearer token 时要求 `Authorization: Bearer`。

- 客户端: 列表、详情、断开连接
- 主题: 列表，以及 `POST /api/v1/topics/{topic}/publish`
- 订阅: `GET /api/v1/subscriptions`
- 系统: `GET /api/v1/system/status`
- Prometheus: `GET /metrics`

规则管理仍属于 v0.5.0。

---

### v0.5.0 - 规则引擎

**预计**: 3 周

核心: 规则解析器、消息/内容路由、数据转换、条件触发、钩子插件、HTTP Webhook

---

### v0.6.0 - Web GUI

**预计**: 3-4 周

技术栈: Vue 3 + Reka UI + Pinia + Vite

核心: 仪表盘、客户端管理、主题管理、规则配置、系统设置、WebSocket 实时推送

---

### v0.7.0 - Raft 集群

**预计**: 4-5 周

核心: Raft 共识 (Leader 选举、日志复制)、gRPC 节点通信、状态同步、故障转移

质量目标: 3/5/7 节点集群，故障恢复 < 30s

---

### v1.0.0 - 生产就绪

**预计**: 2-3 周

核心: 集成/压力/稳定性测试、Docker/Kubernetes/Helm 部署、完整文档

质量目标: 100,000+ 并发连接，100,000+ msg/s，99.99% 可用性

## 后续规划

### v1.x

- 外部集成: Kafka、RabbitMQ
- 边缘计算: 边缘节点支持
- 多租户: 租户隔离和资源配额
- 高级规则引擎: 复杂事件处理 (CEP)

### v2.x

- MQTT 5.0 完整特性: 共享订阅、流控
- 全球分布式: 多区域部署
- 协议插件: CoAP、LwM2M

## 里程碑时间线

```
2026 Q2          2026 Q3          2026 Q4          2027 Q1
   │                │                │                │
   ▼                ▼                ▼                ▼
v0.2.0          v0.3.0          v0.5.0          v0.7.0
单节点Broker     安全认证         规则引擎         Raft集群
   │                │                │                │
   └────────────────┴────────────────┴────────────────┘
                            │
                       v0.4.0 API
                       v0.6.0 Web GUI
                            │
                            ▼
                       v1.0.0 生产就绪
```
