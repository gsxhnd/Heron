# Broker 实现指南

> 状态: 已实现。入口是 `heron`，监听在 `heron_mqtt`，会话与路由在 `heron_core`，PostgreSQL 在 `heron_storage`。

## 目标模块

- 网络接入: TCP 与 WebSocket
- 会话管理: 连接状态、会话恢复、Keep Alive
- 主题管理: 主题树、通配符匹配、保留消息
- 消息路由: 发布到订阅者、QoS 流程、离线队列
- 存储接口: 持久化层预留边界

## 连接生命周期

```text
accept -> decode CONNECT -> auth/authz -> create session -> subscribe/publish loop -> cleanup
```

实现优先级:

1. TCP 接入与协议检测
2. CONNECT/CONNACK 基本流程
3. SUBSCRIBE/PUBLISH/UNSUBSCRIBE
4. QoS 1/2 状态流转
5. 会话恢复与持久化扩展点

## 核心数据结构

```rust
pub struct Session {
    client_id: String,
    connected: bool,
    protocol_version: ProtocolVersion,
}

pub struct TopicNode {
    name: String,
    children: HashMap<String, TopicNode>,
}

pub struct MessageRouter {
    sessions: HashMap<ClientId, Arc<Session>>,
}
```

## 并发与错误处理

- 基于 Tokio task 管理连接生命周期
- 区分编解码错误、协议错误、认证错误与网络错误
- 使用清晰的状态机，避免在连接循环中堆叠分支

## 测试建议

- 单元测试: 主题匹配与 QoS 状态流转
- 集成测试: CONNECT/SUBSCRIBE/PUBLISH 基本路径
- 回归测试: 异常断连、重复 packet id、非法 topic
