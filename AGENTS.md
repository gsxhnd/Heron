# AGENTS.md

Focused guidance for AI coding agents working in this repository.

**Goal:** understand crate boundaries, run/build/test, and follow project-specific patterns.

## Project

Heron — Rust MQTT broker. The workspace contains `heron_codec`, `heron_client`, and a single-node broker: `heron_core` (sessions, topic tree, QoS routing), `heron_mqtt` (TCP and WebSocket), `heron_storage` (SeaORM + PostgreSQL), and the `heron` binary. Authentication, REST API, rules, Web GUI, and Raft are still planned (see [docs/dev/](docs/dev/)).

## Current workspace layout

```
Cargo.toml          # workspace root (resolver = "2"); members = ["crates/*"]
crates/
  heron_codec/       # canonical MQTT codec library
    src/lib.rs      # Encoder/Decoder traits, MqttError, v4/v5 re-exports
    src/v4/         # MQTT 3.1.1 (protocol level 4)
    src/v5/         # MQTT 5.0 (protocol level 5)
    src/error.rs    # MqttError (thiserror)
    tests/          # integration tests (v4/, v5/ subdirectories)
    examples/       # v4_encode, v4_builder, v5_encode, v5_builder
  heron_client/      # async MQTT client library + `mqtt` CLI
    src/lib.rs      # public API re-exports
    src/client.rs   # MqttClient handle (request/response commands)
    src/config.rs   # ClientConfig, Credentials, LastWill, Tls/Reconnect options
    src/error.rs    # ClientError
    src/event.rs    # Event enum
    src/transport.rs # TCP + TLS transport
    src/inflight.rs # QoS 1/2 inflight window + pending queue
    src/session/    # per-version drivers (mod.rs, state.rs, v4.rs, v5.rs)
    src/main.rs     # CLI (clap)
    examples/pubsub.rs
    tests/v4.rs, tests/v5.rs, tests/support/  # mock broker + integration tests
  heron_core/ # sessions, topic tree, QoS 0/1/2 router, Storage trait
  heron_mqtt/ # TCP + WebSocket listeners and connection handler
  heron_storage/ # SeaORM PostgreSQL implementation of Storage
  heron/      # binary: `--config`, YAML, tokio runtime
conf/broker.yaml    # example listener and storage config
docs/dev/           # design docs
```

## Commands

| Task | Command |
|------|---------|
| Build | `cargo build --workspace` |
| Test all | `cargo test --workspace` |
| Test codec | `cargo test -p heron_codec` |
| Test client | `cargo test -p heron_client` |
| Test broker | `cargo test -p heron_core -p heron_mqtt` |
| Run broker | `cargo run -p heron -- --config conf/broker.yaml` |
| Lint | `cargo clippy --all-targets --all-features --locked -- -D warnings` |
| Format | `cargo fmt` |
| Run example | `cargo run -p heron_codec --example v4_encode` |

Use `-p heron_codec` or `-p heron_client` to scope work to a specific crate.

## Conventions

- Rust edition 2024; dependencies: `bytes` (with serde), `thiserror`, `tokio`
- Integration tests live under each crate's `tests/` directory (not inline `#[cfg(test)]`)
- **`heron_codec` is the canonical codec crate** — do not add logic to any legacy `mqtt-codec` name
- Never use `unwrap()`/`expect()` outside tests
- Use `#[expect(clippy::lint)]` over `#[allow(...)]` with justification
- Every `TODO` needs a linked issue: `// TODO(#42): ...`
- Prefer `&T` over `.clone()` unless ownership transfer is required
- Name tests descriptively: `process_should_return_error_when_input_empty()`
- Documentation and README are written in Chinese (Simplified)

## Key files (`heron_codec`)

| Area | Path |
|------|------|
| Traits | [crates/heron_codec/src/lib.rs](crates/heron_codec/src/lib.rs) |
| v3.1.1 codec | [crates/heron_codec/src/v4/codec.rs](crates/heron_codec/src/v4/codec.rs), `encoder.rs`, `decoder.rs`, `validation.rs` |
| v5.0 codec | [crates/heron_codec/src/v5/codec.rs](crates/heron_codec/src/v5/codec.rs), `properties_codec.rs`, `validation.rs` |
| Packet types | [crates/heron_codec/src/v4/packet.rs](crates/heron_codec/src/v4/packet.rs), [crates/heron_codec/src/v5/packet.rs](crates/heron_codec/src/v5/packet.rs) |
| Builders | [crates/heron_codec/src/v4/builder.rs](crates/heron_codec/src/v4/builder.rs), [crates/heron_codec/src/v5/builder.rs](crates/heron_codec/src/v5/builder.rs) |

## Client design

- `MqttClient` is a cloneable handle; every operation is a `Command` carrying a
  `oneshot` reply channel, so `publish`/`subscribe`/`unsubscribe` resolve with the
  broker's acknowledgement (bounded by `ClientConfig::request_timeout`).
- `session::run_event_loop` owns reconnect/backoff and a `SessionState` that
  survives reconnects: subscriptions, inflight publishes, QoS 2 inbound, and
  commands queued while offline.
- `session/v4.rs` and `session/v5.rs` are independent drivers but share
  `session/mod.rs` helpers (`ConnAckInfo`, deadlines, `client_id`).
- When a connection ends, `SessionState::fail_pending()` answers every
  outstanding operation with `ClientError::Disconnected` — callers must never hang.
- Integration tests drive the client against `tests/support`'s in-process mock
  broker (real TCP + `heron_codec`); add new scenarios there rather than mocking
  the socket layer.

## Codec design

- `Encoder<T>` and `Decoder` in `heron_codec/src/lib.rs` follow `tokio_util::codec` patterns (`Framed<S, C>`).
- `v4` and `v5` are independent module trees: packet types, builders, codecs, validation.
- **Encode** and **decode** both call `validate_packet()` — protocol MUST rules are enforced on the wire path.
- v5 adds Properties, Reason Codes, and AUTH; property parsing lives in `v5/properties_codec.rs`.
- Builder pattern for constructing packets (see `examples/`).

## When editing code

**Protocol codec changes**

- Implement v3.1.1 vs v5.0 in `heron_codec/src/v4/` and `heron_codec/src/v5/` separately; keep APIs parallel where sensible.
- After changing validation or wire format, run `cargo test -p heron_codec`.
- Errors use `MqttError` at the crate root (not per-module codec error aliases).

**Testing**

- Integration tests live under `crates/heron_codec/tests/` and `crates/heron_mqtt/tests/`.
- Broker session and routing tests live in `heron_core`. PostgreSQL round-trips need a reachable database URL passed to `PostgresStorage::connect`.

**Common pitfalls**

- The broker crates are `heron`, `heron_core`, `heron_mqtt`, and `heron_storage`.
- Codec validates packets; sessions, QoS state, and routing live in `heron_core`.
- `Maximum packet size` is enforced only on the v5 `MqttCodec` wrapper when configured.

## Broker crates

| Component | Role |
|-----------|------|
| `heron` | Binary: `--config`, YAML, tokio runtime |
| `heron_mqtt` | TCP and WebSocket listeners. CONNECT selects the v3.1.1 or v5.0 codec |
| `heron_core` | Sessions, topic tree, QoS 0/1/2 routing, `Storage` trait, in-memory store |
| `heron_storage` | SeaORM + PostgreSQL. Creates tables on connect |
| `heron_codec` | Packet encode/decode |

Run: `cargo run -p heron -- --config conf/broker.yaml`. Default listeners are TCP `:1883` and WebSocket `:8083` with in-memory storage. TLS, authentication, and Raft are not in this version.

## Later architecture

| Component | Role |
|-----------|------|
| `heron_raft` | Raft node/server; protobufs via `tonic-build` |

**Later workflow**

- Install `protoc` for `tonic-build` (`brew install protobuf` on macOS) only after the Raft crate exists.
- Example config: `conf/broker.yaml`.

**Planned networking patterns**

- `tokio` for all I/O; one multi-thread runtime in app, services as spawned tasks.
- Per-connection handler after protocol version is detected from CONNECT.

## CI

- Workflow: [.github/workflows/test.yml](.github/workflows/test.yml) — checkout on push to `master`.
- Extend with `cargo test --workspace` and `cargo clippy` when CI hardening is desired.
- Future `tonic-build` / `protoc` steps apply only after gRPC/Raft crates are added.

## See also

- [.agents/skills/rust-best-practices/SKILL.md](.agents/skills/rust-best-practices/SKILL.md) — Rust coding conventions
- [docs/dev/README.md](docs/dev/README.md) — full product/design documentation
