# MQTT 编解码库 (mqtt_codec)

高性能、异步友好的 MQTT 控制报文编解码库，支持 **MQTT v3.1.1**（协议级别 4）与 **MQTT v5.0**（协议级别 5）。适用于 `tokio-util` 的 `Framed` 等流式 I/O 场景。

## 核心特性

- **双版本支持**：v3.1.1 全部 14 种、v5.0 全部 15 种控制报文的编解码（含 v5 的 AUTH）。
- **异步友好**：基于 [`bytes`](https://docs.rs/bytes) 构建，实现 crate 级 `Encoder` / `Decoder` trait，可与 `Framed` 直接配合。
- **类型安全**：用 Rust 枚举与强类型字段表达报文结构，减少协议层面的误用。
- **严格验证**：编码与解码路径均调用 `validate_packet()`，覆盖主题名/过滤器、QoS 与 Packet ID、CONNECT 标志位、v5 属性作用域与 Reason Code 等 MUST 规则。
- **构建器模式**：为 CONNECT、PUBLISH、SUBSCRIBE 等复杂报文提供 Builder API（见 `examples/v4_builder.rs`、`examples/v5_builder.rs`）。
- **结构化错误**：基于 `thiserror` 的 `MqttError`，区分协议违规、格式错误、主题/客户端 ID 等问题类型。

## 目录结构

```
mqtt_codec/
├── src/
│   ├── lib.rs          # Encoder / Decoder trait、错误类型重导出
│   ├── error.rs        # MqttError
│   ├── v4/             # MQTT v3.1.1
│   │   ├── packet.rs   # 报文类型
│   │   ├── codec.rs    # MqttCodec（编解码入口）
│   │   ├── encoder.rs / decoder.rs
│   │   ├── validation.rs
│   │   ├── builder.rs
│   │   └── return_codes.rs
│   └── v5/             # MQTT v5.0
│       ├── packet.rs
│       ├── properties_codec.rs / property_id.rs
│       └── …（与 v4 平行的 codec / validation / builder）
├── tests/              # 集成测试（按版本与主题拆分）
│   ├── v4.rs           # v4 测试入口
│   ├── v4/             # roundtrip、validation、wire_and_limits、builder、return_codes
│   ├── v5.rs           # v5 测试入口
│   └── v5/             # codec、validation、builder
└── examples/           # v4_encode、v4_builder、v5_encode、v5_builder
```

实现代码仅在 `src/`；`tests/` 通过集成测试调用公开 API，避免在库源码中堆积大段 `#[cfg(test)]` 模块。

## 依赖配置

在 Heron 工作区内使用 path 依赖：

```toml
[dependencies]
mqtt_codec = { path = "../mqtt_codec" }
bytes = "1"
```

独立引用 crate 时（当前版本见 `Cargo.toml`）：

```toml
[dependencies]
mqtt_codec = "0.1"
bytes = "1"
```

## 快速开始

### MQTT v3.1.1

```rust
use bytes::BytesMut;
use mqtt_codec::v4::{ConnectPacket, MqttCodec, Packet};
use mqtt_codec::{Decoder, Encoder};

fn main() -> Result<(), mqtt_codec::MqttError> {
    let mut codec = MqttCodec::new();
    let mut buffer = BytesMut::new();

    let connect = ConnectPacket {
        protocol_name: "MQTT".to_string(),
        protocol_level: 4,
        client_id: "example-client".to_string(),
        keep_alive: 60,
        clean_session: true,
        ..Default::default()
    };
    codec.encode(Packet::Connect(connect), &mut buffer)?;

    if let Some(packet) = codec.decode(&mut buffer)? {
        if let Packet::Connect(p) = packet {
            println!("收到 CONNECT，Client ID: {}", p.client_id);
        }
    }
    Ok(())
}
```

### MQTT v5.0

v5 在 v4 能力基础上增加 **Properties**、**Reason Code** 与 **AUTH** 报文。`MqttCodec` 支持可选的最大报文长度限制（在 CONNECT/CONNACK 协商后设置）：

```rust
use bytes::BytesMut;
use mqtt_codec::v5::{ConnectPacket, MqttCodec, Packet, Properties};
use mqtt_codec::{Decoder, Encoder};

fn main() -> Result<(), mqtt_codec::MqttError> {
    let mut codec = MqttCodec::with_max_packet_size(256 * 1024);
    let mut buffer = BytesMut::new();

    let connect = ConnectPacket {
        protocol_name: "MQTT".to_string(),
        protocol_level: 5,
        client_id: "v5-client".to_string(),
        keep_alive: 60,
        clean_start: true,
        properties: Properties {
            session_expiry_interval: Some(3600),
            ..Default::default()
        },
        ..Default::default()
    };
    codec.encode(Packet::Connect(connect), &mut buffer)?;

    if let Some(_packet) = codec.decode(&mut buffer)? {
        // 处理解码后的报文
    }
    Ok(())
}
```

更多示例：

```bash
cargo run -p mqtt_codec --example v4_encode
cargo run -p mqtt_codec --example v4_builder
cargo run -p mqtt_codec --example v5_encode
cargo run -p mqtt_codec --example v5_builder
```

## 与 `Framed` 集成

`mqtt_codec` 在 crate 根定义了与 `tokio-util::codec` 风格一致的 trait：

| Trait | 说明 |
|-------|------|
| `Encoder<T>` | 将 `Packet` 写入 `BytesMut` |
| `Decoder` | 从 `BytesMut` 增量解析完整报文；数据不足时返回 `Ok(None)` |

各版本的 `v4::MqttCodec` / `v5::MqttCodec` 均实现上述 trait，可作为 `Framed<TcpStream, MqttCodec>` 的 Codec 类型使用。

## 验证范围（摘要）

| 类别 | v3.1.1 | v5.0 |
|------|--------|------|
| 协议名 / 级别 | `MQTT` + level 4 | `MQTT` + level 5 |
| 主题名 | 非空、无通配符、无 NUL | 同上；支持 Topic Alias 场景 |
| 主题过滤器 | `+` / `#` 位置规则 | 同上 + 共享订阅解析 |
| QoS / Packet ID | QoS 0 无 ID；ID 非 0 | 同上 |
| CONNECT 标志 | Will、用户名/密码一致性 | 含 v5 属性与 Will Properties |
| 属性 | — | 按报文类型校验属性作用域与取值 |

完整规则见各版本的 `validation.rs`。

## 测试

集成测试位于 `tests/`，按协议版本与主题组织：

| 目录 / 文件 | 覆盖内容 |
|-------------|----------|
| `tests/v4/roundtrip.rs` | 各控制报文编解码往返 |
| `tests/v4/validation.rs` | 协议校验函数 |
| `tests/v4/wire_and_limits.rs` | 线格式、大 payload、边界 Packet ID |
| `tests/v4/builder.rs` | Builder API |
| `tests/v4/return_codes.rs` | 返回码映射 |
| `tests/v5/codec.rs` | v5 编解码与 Properties |
| `tests/v5/validation.rs` | v5 校验与共享订阅 |
| `tests/v5/builder.rs` | v5 Builder |

常用命令：

```bash
# 运行本 crate 全部测试
cargo test -p mqtt_codec

# 仅 v3.1.1 / v5.0 集成测试
cargo test -p mqtt_codec --test v4
cargo test -p mqtt_codec --test v5

# 静态检查（CI 建议）
cargo clippy -p mqtt_codec --all-targets -- -D warnings
cargo fmt --check
```

## 性能与使用建议

1. **重用缓冲区**：对同一连接复用 `BytesMut`，减少分配。
2. **先构建再编码**：在 Builder 或业务层保证字段合法，避免编码阶段才触发 `MqttError`。
3. **v5 报文大小**：在收到 CONNACK 后调用 `MqttCodec::set_max_packet_size()`，与对端协商值对齐。
4. **职责边界**：本库只负责控制报文的编解码与协议校验，不包含会话状态、QoS 重传或 Broker 路由逻辑。

## 许可证

MIT License
