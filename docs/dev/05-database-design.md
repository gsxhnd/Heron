# 数据库设计

> 状态: `heron_storage`（SeaORM + PostgreSQL）已在连接时创建并读写 `clients`、`sessions`、`subscriptions`、`retained_messages`、`messages`、`rules` 六张表（见 `crates/heron_storage/src/schema.rs`）。认证用户与 ACL 目前由 YAML 配置，`users`、`acl_rules` 及统计/分组类表尚未建库，属于规划。

> 下文与实现不一致之处以 `schema.rs` 为准：`ip_address` 为 `VARCHAR(128)` 而非 `INET`（保持 SeaORM `String` 模型）；`messages.direction` 区分出站 inflight 与入站 QoS 2 握手；`rules.conditions` 为 `TEXT`（JSON 字符串）。

## 核心表

### clients (客户端表)

```sql
CREATE TABLE clients (
    id BIGSERIAL PRIMARY KEY,
    client_id VARCHAR(255) NOT NULL UNIQUE,
    protocol_version SMALLINT NOT NULL,
    ip_address INET,
    port INTEGER,
    connected BOOLEAN DEFAULT false,
    connected_at TIMESTAMP,
    disconnected_at TIMESTAMP,
    keep_alive INTEGER,
    clean_session BOOLEAN,
    will_topic VARCHAR(512),
    will_qos SMALLINT,
    will_retain BOOLEAN,
    will_payload BYTEA,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_clients_client_id ON clients(client_id);
CREATE INDEX idx_clients_connected ON clients(connected);
```

### sessions (会话表)

```sql
CREATE TABLE sessions (
    id BIGSERIAL PRIMARY KEY,
    client_id VARCHAR(255) NOT NULL UNIQUE,
    clean_session BOOLEAN NOT NULL,
    session_expiry_interval INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE
);
```

### subscriptions (订阅表)

```sql
CREATE TABLE subscriptions (
    id BIGSERIAL PRIMARY KEY,
    client_id VARCHAR(255) NOT NULL,
    topic VARCHAR(512) NOT NULL,
    qos SMALLINT NOT NULL,
    no_local BOOLEAN NOT NULL DEFAULT FALSE,
    retain_as_published BOOLEAN NOT NULL DEFAULT FALSE,
    retain_handling SMALLINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE,
    UNIQUE(client_id, topic)
);

CREATE INDEX idx_subscriptions_client_id ON subscriptions(client_id);
CREATE INDEX idx_subscriptions_topic ON subscriptions(topic);
```

### retained_messages (保留消息表)

```sql
CREATE TABLE retained_messages (
    id BIGSERIAL PRIMARY KEY,
    topic VARCHAR(512) NOT NULL UNIQUE,
    qos SMALLINT NOT NULL,
    payload BYTEA,
    payload_size INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_retained_messages_topic ON retained_messages(topic);
```

### messages (离线/inflight 消息表)

```sql
CREATE TABLE messages (
    id BIGSERIAL PRIMARY KEY,
    client_id VARCHAR(255) NOT NULL,
    packet_id INTEGER NOT NULL,
    topic VARCHAR(512) NOT NULL,
    qos SMALLINT NOT NULL,
    payload BYTEA,
    payload_size INTEGER,
    retain BOOLEAN DEFAULT false,
    dup BOOLEAN DEFAULT false,
    direction VARCHAR(8) NOT NULL DEFAULT 'out',  -- out = 出站 inflight, in = 入站 QoS 2 握手
    state VARCHAR(20) NOT NULL,                   -- pending, puback, pubrec, pubrel, pubcomp
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE,
    UNIQUE(client_id, packet_id, direction)
);

CREATE INDEX idx_messages_client_id ON messages(client_id);
CREATE INDEX idx_messages_state ON messages(state);
CREATE INDEX idx_messages_created_at ON messages(created_at);
```

## 认证授权表（规划，v0.3.0 起由 YAML 配置）

### users (用户表)

```sql
CREATE TABLE users (
    id BIGSERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    salt VARCHAR(255),
    enabled BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### acl_rules (ACL 规则表)

```sql
CREATE TABLE acl_rules (
    id BIGSERIAL PRIMARY KEY,
    username VARCHAR(255),
    client_id VARCHAR(255),
    topic VARCHAR(512) NOT NULL,
    action VARCHAR(20) NOT NULL,  -- publish, subscribe, pubsub
    allow BOOLEAN NOT NULL,
    priority INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_acl_rules_username ON acl_rules(username);
CREATE INDEX idx_acl_rules_client_id ON acl_rules(client_id);
CREATE INDEX idx_acl_rules_topic ON acl_rules(topic);
```

## 规则引擎表（已实现）

### rules (规则表)

```sql
CREATE TABLE rules (
    id BIGINT PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    enabled BOOLEAN DEFAULT true,
    priority INTEGER DEFAULT 0,
    topic_filter VARCHAR(512) NOT NULL,
    conditions TEXT,          -- JSON 条件字符串
    actions TEXT NOT NULL,    -- JSON 动作数组
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_rules_enabled ON rules(enabled);
CREATE INDEX idx_rules_topic_filter ON rules(topic_filter);
```

## 统计表（规划，未建）

### client_stats (客户端统计)

```sql
CREATE TABLE client_stats (
    id BIGSERIAL PRIMARY KEY,
    client_id VARCHAR(255) NOT NULL,
    messages_received BIGINT DEFAULT 0,
    messages_sent BIGINT DEFAULT 0,
    bytes_received BIGINT DEFAULT 0,
    bytes_sent BIGINT DEFAULT 0,
    date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE,
    UNIQUE(client_id, date)
);

CREATE INDEX idx_client_stats_client_id ON client_stats(client_id);
CREATE INDEX idx_client_stats_date ON client_stats(date);
```

### connection_history (连接历史)

```sql
CREATE TABLE connection_history (
    id BIGSERIAL PRIMARY KEY,
    client_id VARCHAR(255) NOT NULL,
    ip_address INET,
    connected_at TIMESTAMP NOT NULL,
    disconnected_at TIMESTAMP,
    disconnect_reason VARCHAR(100),
    FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE
);

CREATE INDEX idx_connection_history_client_id ON connection_history(client_id);
CREATE INDEX idx_connection_history_connected_at ON connection_history(connected_at);
```

## 客户端管理表（规划，未建）

### client_groups (分组表)

```sql
CREATE TABLE client_groups (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### client_group_members (分组成员)

```sql
CREATE TABLE client_group_members (
    id BIGSERIAL PRIMARY KEY,
    group_id BIGINT NOT NULL,
    client_id VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (group_id) REFERENCES client_groups(id) ON DELETE CASCADE,
    FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE,
    UNIQUE(group_id, client_id)
);
```

### client_tags (标签表)

```sql
CREATE TABLE client_tags (
    id BIGSERIAL PRIMARY KEY,
    client_id VARCHAR(255) NOT NULL,
    tag VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (client_id) REFERENCES clients(client_id) ON DELETE CASCADE,
    UNIQUE(client_id, tag)
);

CREATE INDEX idx_client_tags_tag ON client_tags(tag);
```

### blacklist (黑名单表)

```sql
CREATE TABLE blacklist (
    id BIGSERIAL PRIMARY KEY,
    client_id VARCHAR(255),
    ip_address INET,
    reason TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP
);

CREATE INDEX idx_blacklist_client_id ON blacklist(client_id);
CREATE INDEX idx_blacklist_ip_address ON blacklist(ip_address);
```

## 维护策略

- 为高频查询字段和外键创建索引
- 定期清理过期离线消息
- 定期归档历史连接记录
- 定期清理过期黑名单记录
