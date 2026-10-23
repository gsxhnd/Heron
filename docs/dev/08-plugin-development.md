# 插件开发指南

> 状态: 设计草案。`heron_auth` 已实现认证与 ACL trait，以及内置的密码、Token、证书和 ACL。钩子与动态插件加载仍未实现。

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

```rust
#[async_trait]
pub trait Hook: Send + Sync {
    async fn on_publish(
        &self,
        client_id: &str,
        topic: &str,
        payload: &[u8],
    ) -> Result<HookAction, HookError>;
}
```

## 设计约束

- 插件接口独立于网络层和存储层
- 错误需区分拒绝、重试和内部失败
- Hook 不应阻塞 Broker 主数据路径
- 配置支持静态文件与环境变量映射

## 待定项

- 插件加载机制
- 生命周期管理
- 配置注入格式
- 安全隔离策略
