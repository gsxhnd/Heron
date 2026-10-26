# API 设计

> 状态: 已实现下列 REST 路由、健康检查、Prometheus 文本指标，以及 `GET /api/v1/events` WebSocket。gRPC 仍未实现。管理 API 与 MQTT 监听分开绑定。配置了 `api.token`（或环境变量 `HERON_API_TOKEN`）时，`/api/v1` 管理路由和 `/metrics` 要求 Bearer Token。`/health` 与 `/api/v1/system/health` 保持开放。WebSocket 额外接受查询参数 `token`，因为浏览器不能在升级请求上设置 `Authorization`。

## 设计目标

- 为 Broker 提供管理、监控和调试入口
- 保持资源命名、分页和错误响应风格一致
- 为 Web GUI 和自动化运维提供稳定接口

## REST API

### Base URL

```
/api/v1
```

### 路由结构

```text
/api/v1
├── /clients
├── /topics
├── /subscriptions
├── /rules
└── /system

/metrics    (Prometheus, 根路径)
```

### 认证

- 方案: Bearer Token
- 预留扩展: API Key 或管理用户会话

### 通用响应格式

成功:

```json
{
  "data": {},
  "error": null,
  "request_id": "optional"
}
```

错误:

```json
{
  "data": null,
  "error": {
    "code": "not_found",
    "message": "resource not found"
  }
}
```

### 错误码

| 错误码 | 描述 |
|-------|------|
| 400 | 请求参数错误 |
| 401 | 未授权 |
| 404 | 资源不存在 |
| 500 | 服务器内部错误 |

## 接口详情

### 客户端管理

```http
GET    /clients              # 列表 (支持 page, page_size, connected, client_id, protocol 参数)
GET    /clients/{client_id}  # 详情
DELETE /clients/{client_id}  # 断开连接
```

详情额外包含 `keep_alive`、`clean_start` 和 `session_expiry`。

响应示例:

```json
{
  "total": 100,
  "page": 1,
  "page_size": 20,
  "data": [
    {
      "client_id": "client001",
      "protocol_version": 4,
      "ip_address": "192.168.1.100",
      "connected": true,
      "connected_at": "2026-03-23T10:00:00Z"
    }
  ]
}
```

### 主题管理

```http
GET  /topics                    # 主题列表
GET  /topics/{topic}            # 主题详情，含保留载荷
POST /topics/{topic}/publish    # 发布消息
```

空载荷且 `retain: true` 会删除保留消息。详情在载荷是 UTF-8 时返回 `payload_text`，并总是在有保留消息时返回 `payload_hex`。

发布请求体:

```json
{
  "payload": "Hello World",
  "qos": 1,
  "retain": false
}
```

### 系统监控

```http
GET /subscriptions       # 订阅列表
GET /system/status       # 系统状态
GET /system/health       # 健康检查（不鉴权）
GET /health              # 健康检查（根路径，不鉴权）
GET /metrics             # Prometheus 指标 (根路径)
```

状态响应:

```json
{
  "uptime": 3600,
  "connections": 1000,
  "messages_received": 50000,
  "messages_sent": 48000
}
```

### 规则配置

```http
GET    /rules
POST   /rules
PUT    /rules/{rule_id}
DELETE /rules/{rule_id}
```

规则由 Broker 持有并执行，经 `Storage` 持久化，重启后恢复。管理 API 对同一份规则做增删改查；操作成功时规则立即生效并在后台写通存储（写失败仅记录日志，不影响内存中的规则状态）。

### 实时推送

```http
GET /api/v1/events
```

WebSocket 文本帧。连接后每秒一条 `type: snapshot`，字段包括连接数、会话数、收发计数、每秒速率、主题数和订阅数。第一条快照建立在线客户端基线，之后在线集合变化时发送 `type: client`，`action` 为 `connected` 或 `disconnected`。

## gRPC (集群节点间通信)

```protobuf
service ClusterService {
  rpc SyncState(StateRequest) returns (StateResponse);
  rpc ForwardMessage(MessageRequest) returns (MessageResponse);
}
```

## 未决问题

- 管理 API 与数据平面目前分端口监听，尚未做网络隔离
- 用户与 ACL 由 YAML 配置，没有独立的管理用户表
- 健康检查同时提供 `GET /health` 和 `GET /api/v1/system/health`
