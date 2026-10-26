# 插件开发指南

> 状态: `heron_auth` 已实现认证与 ACL trait，以及内置的密码、Token、证书和 ACL。钩子插件 (`Hook` trait 与 `heron_rules` 规则引擎) 已实现并在 Broker 生命周期上运行；动态插件加载（进程外/共享库热加载）仍未实现。

## 插件分类

- 认证插件: 处理用户名密码、Token 或外部身份验证
- 授权插件: 判断客户端是否可发布或订阅某主题
- 钩子插件: 在连接、断开、发布、订阅等事件上执行附加逻辑

## Trait 定义

### 认证插件

```rust
#[async_trait]
pub trait Authenticator: Send + Sync {
    async fn authenticate(
        &self,
        username: &str,
        password: &str,
    ) -> Result<AuthResult, AuthError>;
}
```

### 授权插件

```rust
#[async_trait]
pub trait Authorizer: Send + Sync {
    async fn authorize(
        &self,
        client_id: &str,
        username: Option<&str>,
        topic: &str,
        action: Action,
    ) -> Result<bool, AuthError>;
}
```

### 钩子插件

已实现的现实接口与设计草案略有出入，`heron_rules/src/hook.rs` 中的 `Hook` trait 现在是:

```rust
#[async_trait]
pub trait Hook: Send + Sync {
    async fn on_connect(&self, ctx: &HookContext) -> Result<(), HookError>;
    async fn on_disconnect(&self, ctx: &HookContext) -> Result<(), HookError>;
    async fn on_publish(&self, ctx: &HookContext) -> Result<(), HookError>;
    async fn on_subscribe(&self, ctx: &HookContext) -> Result<(), HookError>;
    async fn on_unsubscribe(&self, ctx: &HookContext) -> Result<(), HookError>;
}
```

- 每个回调都有默认的 `Ok(())` 实现，插件按需覆盖
- `HookContext` 提供 event 类型、`client_id`、`username`、`topic`、`payload`、`qos`、`retain`、`peer` 与 `filter`
- `Err(HookError::Reject)` 会拒绝 CONNECT/SUBSCRIBE 或当前 PUBLISH；`Retry` 和 `Internal` 只记录日志，不中断 broker
- 通过 `RuleEngine::add_hook(Arc<dyn Hook>)` 注册，钩子在内置规则之前执行

## 设计约束

- 插件接口独立于网络层和存储层
- 错误需区分拒绝、重试和内部失败
- Hook 不应阻塞 Broker 主数据路径
- 配置支持静态文件与环境变量映射

## 待定项

- 在 Hook trait 语义之上的进程外插件加载机制
- 生命周期管理
- 配置注入格式
- 安全隔离策略
