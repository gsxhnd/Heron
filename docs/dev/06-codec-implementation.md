# MQTT 协议编解码实现

> 状态: 已实现，基于当前 `main` 分支。

## 模块架构

```
mqtt_codec/
├── src/
│   ├── lib.rs           # 公共 trait 定义
│   ├── error.rs         # 错误类型定义
│   ├── v4/              # MQTT v3.1.1 实现
│   │   ├── mod.rs
│   │   ├── packet.rs
│   │   ├── codec.rs
│   │   ├── decoder.rs
│   │   └── encoder.rs
│   └── v5/              # MQTT v5.0 实现
│       ├── mod.rs
│       ├── packet.rs
│       ├── codec.rs
│       ├── decoder.rs
│       └── encoder.rs
```

## 核心 Trait

```rust
/// 解码器 trait
pub trait Decoder {
    type Item;
    type Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error>;
}

/// 编码器 trait
pub trait Encoder<T> {
    type Error;

    fn encode(&mut self, item: T, dst: &mut BytesMut) -> Result<(), Self::Error>;
}
```

## 控制包类型

| 类型 | 值 | 方向 | 描述 |
|------|----|----|------|
| CONNECT | 1 | C → S | 连接请求 |
| CONNACK | 2 | S → C | 连接确认 |
| PUBLISH | 3 | 双向 | 发布消息 |
| PUBACK | 4 | 双向 | 发布确认 (QoS 1) |
| PUBREC | 5 | 双向 | 发布接收 (QoS 2) |
| PUBREL | 6 | 双向 | 发布释放 (QoS 2) |
| PUBCOMP | 7 | 双向 | 发布完成 (QoS 2) |
| SUBSCRIBE | 8 | C → S | 订阅请求 |
| SUBACK | 9 | S → C | 订阅确认 |
| UNSUBSCRIBE | 10 | C → S | 取消订阅 |
| UNSUBACK | 11 | S → C | 取消订阅确认 |
| PINGREQ | 12 | C → S | 心跳请求 |
| PINGRESP | 13 | S → C | 心跳响应 |
| DISCONNECT | 14 | C → S | 断开连接 |

## QoS 级别

```rust
pub enum QoS {
    AtMostOnce = 0,   // 最多一次
    AtLeastOnce = 1,  // 至少一次
    ExactlyOnce = 2,  // 恰好一次
}
```

## 固定头部结构

```
┌─────────────────────────────────────────────────────────┐
│ Byte 1: Control Header                                  │
├───────┬───────┬───────┬───────┬───────┬───────┬───────┬───────┤
│ Bit 7 │ Bit 6 │ Bit 5 │ Bit 4 │ Bit 3 │ Bit 2 │ Bit 1 │ Bit 0 │
├───────┴───────┴───────┴───────┼───────┴───────┴───────┴───────┤
│      Packet Type (4 bits)     │      Flags (4 bits)           │
└───────────────────────────────┴───────────────────────────────┘
│ Byte 2+: Remaining Length (Variable Byte Integer)       │
└─────────────────────────────────────────────────────────┘
```

## 可变长度整数编码

```rust
fn encode_variable_length(mut value: u32, dst: &mut BytesMut) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value > 0 {
            byte |= 0x80;
        }
        dst.put_u8(byte);
        if value == 0 {
            break;
        }
    }
}

fn decode_variable_length(src: &mut BytesMut) -> Result<u32, Error> {
    let mut value: u32 = 0;
    let mut shift: u32 = 0;
    loop {
        let byte = src.get_u8();
        value |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift > 28 {
            return Err(Error::MalformedVariableLength);
        }
    }
    Ok(value)
}
```

## MQTT v5.0 属性

| 属性 ID | 名称 | 数据类型 |
|---------|------|---------|
| 0x01 | Payload Format Indicator | Byte |
| 0x02 | Message Expiry Interval | Four Byte Integer |
| 0x03 | Content Type | UTF-8 String |
| 0x11 | Session Expiry Interval | Four Byte Integer |
| 0x21 | Receive Maximum | Two Byte Integer |
| 0x22 | Topic Alias Maximum | Two Byte Integer |

## 测试

```bash
cargo test -p mqtt_codec
```

具体使用示例参考 `mqtt_codec/README.md` 和 `mqtt_codec/examples/`。
