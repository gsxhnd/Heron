//! In-memory rules and the broker hook that executes them.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use chrono::{SecondsFormat, Utc};
use heron_core::{
    BrokerHooks, ConnectEvent, DisconnectEvent, FilterClass, HookPublish, HookReject,
    PublishDecision, PublishEvent, SubscribeEvent, UnsubscribeEvent, classify_filter,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::error::{HookError, RuleError};
use crate::eval::{self, Action, Condition};
use crate::hook::{Hook, HookContext, HookEvent};

/// One rule returned by the management API.
#[derive(Debug, Clone, Serialize)]
pub struct RuleRecord {
    /// Identifier assigned by the engine.
    pub id: u64,
    /// Unique name.
    pub name: String,
    /// Optional description.
    pub description: Option<String>,
    /// Disabled rules stay stored and do not run.
    pub enabled: bool,
    /// Higher values run first.
    pub priority: i32,
    /// MQTT topic filter, including `$events/...` lifecycle topics.
    pub topic_filter: String,
    /// `null`, `{"sql":"..."}`, `{"all":[...]}`, or `{"any":[...]}`.
    pub conditions: Value,
    /// Action objects.
    pub actions: Value,
    /// RFC 3339 timestamp.
    pub created_at: String,
    /// RFC 3339 timestamp.
    pub updated_at: String,
}

/// Fields accepted when creating or replacing a rule.
#[derive(Debug, Clone, Deserialize)]
pub struct RuleInput {
    /// Unique name, 1 to 255 characters.
    pub name: String,
    /// Optional description, at most 2000 characters.
    #[serde(default)]
    pub description: Option<String>,
    /// Defaults to enabled.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Higher values run first.
    #[serde(default)]
    pub priority: i32,
    /// MQTT topic filter.
    pub topic_filter: String,
    /// Condition object. Null matches every message on the filter.
    #[serde(default)]
    pub conditions: Value,
    /// Action array.
    pub actions: Value,
}

fn default_enabled() -> bool {
    true
}

#[derive(Clone)]
struct Stored {
    record: RuleRecord,
    condition: Condition,
    actions: Vec<Action>,
}

struct Inner {
    next_id: u64,
    rules: Vec<Stored>,
    hooks: Vec<Arc<dyn Hook>>,
    http: reqwest::Client,
}

/// Cloneable rule store. Successful management responses report the engine as active.
#[derive(Clone)]
pub struct RuleEngine {
    inner: Arc<Mutex<Inner>>,
}

impl RuleEngine {
    /// Empty engine with an HTTP client for webhook actions.
    pub fn new() -> Self {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(3))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            inner: Arc::new(Mutex::new(Inner {
                next_id: 1,
                rules: Vec::new(),
                hooks: Vec::new(),
                http,
            })),
        }
    }

    /// Broker callback that evaluates plugins and rules.
    pub fn hooks(&self) -> Arc<dyn BrokerHooks> {
        Arc::new(EngineHooks {
            engine: self.clone(),
        })
    }

    /// Register a hook. Hooks run before built-in rules.
    pub fn add_hook(&self, hook: Arc<dyn Hook>) {
        lock(&self.inner).hooks.push(hook);
    }

    /// Rules ordered by priority, then name, then id.
    pub fn list(&self) -> Vec<RuleRecord> {
        let mut records: Vec<RuleRecord> = lock(&self.inner)
            .rules
            .iter()
            .map(|stored| stored.record.clone())
            .collect();
        records.sort_by(|left, right| {
            right
                .priority
                .cmp(&left.priority)
                .then_with(|| left.name.cmp(&right.name))
                .then_with(|| left.id.cmp(&right.id))
        });
        records
    }

    /// Validate and store a rule.
    pub fn insert(&self, input: RuleInput) -> Result<RuleRecord, RuleError> {
        let mut inner = lock(&self.inner);
        let draft = validate(&input, &inner.rules, None)?;
        let id = inner.next_id;
        inner.next_id = inner.next_id.saturating_add(1);
        let now = timestamp();
        let record = RuleRecord {
            id,
            name: draft.name,
            description: draft.description,
            enabled: draft.enabled,
            priority: draft.priority,
            topic_filter: draft.topic_filter,
            conditions: draft.conditions,
            actions: draft.actions,
            created_at: now.clone(),
            updated_at: now,
        };
        inner.rules.push(Stored {
            record: record.clone(),
            condition: draft.condition,
            actions: draft.compiled,
        });
        Ok(record)
    }

    /// Replace an existing rule, keeping its id and creation time.
    pub fn update(&self, id: u64, input: RuleInput) -> Result<RuleRecord, RuleError> {
        let mut inner = lock(&self.inner);
        let Some(index) = inner.rules.iter().position(|stored| stored.record.id == id) else {
            return Err(RuleError::NotFound);
        };
        let draft = validate(&input, &inner.rules, Some(id))?;
        let created_at = inner.rules[index].record.created_at.clone();
        let record = RuleRecord {
            id,
            name: draft.name,
            description: draft.description,
            enabled: draft.enabled,
            priority: draft.priority,
            topic_filter: draft.topic_filter,
            conditions: draft.conditions,
            actions: draft.actions,
            created_at,
            updated_at: timestamp(),
        };
        inner.rules[index] = Stored {
            record: record.clone(),
            condition: draft.condition,
            actions: draft.compiled,
        };
        Ok(record)
    }

    /// Remove a rule.
    pub fn delete(&self, id: u64) -> Result<(), RuleError> {
        let mut inner = lock(&self.inner);
        let Some(index) = inner.rules.iter().position(|stored| stored.record.id == id) else {
            return Err(RuleError::NotFound);
        };
        inner.rules.remove(index);
        Ok(())
    }

    fn evaluate(&self, ctx: &HookContext) -> Effect {
        let (mut rules, http) = {
            let inner = lock(&self.inner);
            let rules: Vec<Stored> = inner
                .rules
                .iter()
                .filter(|stored| stored.record.enabled)
                .cloned()
                .collect();
            (rules, inner.http.clone())
        };
        rules.sort_by(|left, right| {
            right
                .record
                .priority
                .cmp(&left.record.priority)
                .then_with(|| left.record.name.cmp(&right.record.name))
                .then_with(|| left.record.id.cmp(&right.record.id))
        });
        let context = context_value(ctx);
        let mut effect = Effect::forward(ctx.payload.clone());
        for stored in rules {
            if !heron_core::topic_matches(&stored.record.topic_filter, &ctx.topic) {
                continue;
            }
            if !eval::condition_matches(&stored.condition, &context) {
                continue;
            }
            apply_actions(
                &stored.record,
                &stored.actions,
                ctx,
                &context,
                &mut effect,
                &http,
            );
        }
        effect
    }

    async fn run_hooks(&self, ctx: &HookContext) -> Result<(), HookReject> {
        let hooks = lock(&self.inner).hooks.clone();
        for hook in hooks {
            let result = match ctx.event {
                HookEvent::Connect => hook.on_connect(ctx).await,
                HookEvent::Disconnect => hook.on_disconnect(ctx).await,
                HookEvent::Publish => hook.on_publish(ctx).await,
                HookEvent::Subscribe => hook.on_subscribe(ctx).await,
                HookEvent::Unsubscribe => hook.on_unsubscribe(ctx).await,
            };
            match result {
                Ok(()) => {}
                Err(HookError::Reject) => {
                    return Err(HookReject {
                        reason: "hook rejected the event".to_owned(),
                    });
                }
                Err(HookError::Retry) => {
                    tracing::warn!(client_id = %ctx.client_id, "钩子请求重试，已继续处理");
                }
                Err(HookError::Internal(message)) => {
                    tracing::warn!(client_id = %ctx.client_id, error = %message, "钩子执行失败");
                }
            }
        }
        Ok(())
    }
}

impl Default for RuleEngine {
    fn default() -> Self {
        Self::new()
    }
}

struct Draft {
    name: String,
    description: Option<String>,
    enabled: bool,
    priority: i32,
    topic_filter: String,
    conditions: Value,
    actions: Value,
    condition: Condition,
    compiled: Vec<Action>,
}

fn validate(
    input: &RuleInput,
    rules: &[Stored],
    ignore_id: Option<u64>,
) -> Result<Draft, RuleError> {
    let name = input.name.trim();
    if name.is_empty() || name.len() > 255 {
        return Err(RuleError::Invalid(
            "name must be 1 to 255 characters".to_owned(),
        ));
    }
    if rules
        .iter()
        .any(|stored| stored.record.name == name && Some(stored.record.id) != ignore_id)
    {
        return Err(RuleError::Invalid("rule name already exists".to_owned()));
    }
    let topic_filter = input.topic_filter.trim();
    match classify_filter(topic_filter) {
        Ok(()) => {}
        Err(FilterClass::Invalid) => {
            return Err(RuleError::Invalid("invalid topic filter".to_owned()));
        }
        Err(FilterClass::Shared) => {
            return Err(RuleError::Invalid(
                "shared subscriptions are not supported".to_owned(),
            ));
        }
    }
    let description = input
        .description
        .as_deref()
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_owned);
    if description.as_ref().is_some_and(|text| text.len() > 2_000) {
        return Err(RuleError::Invalid(
            "description must be at most 2000 characters".to_owned(),
        ));
    }
    let condition = eval::compile_condition(&input.conditions)?;
    let compiled = eval::compile_actions(&input.actions)?;
    Ok(Draft {
        name: name.to_owned(),
        description,
        enabled: input.enabled,
        priority: input.priority,
        topic_filter: topic_filter.to_owned(),
        conditions: input.conditions.clone(),
        actions: input.actions.clone(),
        condition,
        compiled,
    })
}

struct Effect {
    forward: bool,
    payload: Option<Bytes>,
    transformed: bool,
    republish: Vec<HookPublish>,
    reject: bool,
}

impl Effect {
    fn forward(original: Bytes) -> Self {
        Self {
            forward: true,
            payload: Some(original),
            transformed: false,
            republish: Vec::new(),
            reject: false,
        }
    }

    fn decision(self) -> PublishDecision {
        PublishDecision {
            forward: self.forward && !self.reject,
            payload: if self.transformed { self.payload } else { None },
            republish: self.republish,
        }
    }
}

fn apply_actions(
    record: &RuleRecord,
    actions: &[Action],
    ctx: &HookContext,
    context: &Value,
    effect: &mut Effect,
    http: &reqwest::Client,
) {
    for action in actions {
        match action {
            Action::Log => {
                tracing::info!(
                    rule = %record.name,
                    topic = %ctx.topic,
                    client_id = %ctx.client_id,
                    "规则匹配"
                );
            }
            Action::Drop | Action::Reject => {
                effect.forward = false;
                effect.reject = true;
            }
            Action::Transform { payload } => {
                if effect.transformed {
                    continue;
                }
                effect.payload = Some(value_to_payload(&eval::render_json(payload, context)));
                effect.transformed = true;
            }
            Action::Republish {
                topic,
                payload,
                qos,
                retain,
            } => {
                let rendered = eval::render_text(topic, context);
                if !heron_core::valid_topic_name(&rendered) {
                    tracing::warn!(rule = %record.name, topic = %rendered, "规则转发主题无效");
                    continue;
                }
                let body = match payload {
                    Some(template) => value_to_payload(&eval::render_json(template, context)),
                    None => ctx.payload.clone(),
                };
                effect.republish.push(HookPublish {
                    topic: rendered,
                    payload: body,
                    qos: *qos,
                    retain: *retain,
                });
            }
            Action::Webhook { url, body } => {
                let rendered = eval::render_text(url, context);
                if !is_runtime_http(&rendered) {
                    tracing::warn!(rule = %record.name, "规则 webhook 地址无效");
                    continue;
                }
                let payload = match body {
                    Some(template) => eval::render_json(template, context),
                    None => context.clone(),
                };
                let client = http.clone();
                let rule = record.name.clone();
                tokio::spawn(async move {
                    if let Err(err) = client.post(&rendered).json(&payload).send().await {
                        tracing::warn!(rule = %rule, error = %err, "规则 webhook 失败");
                    }
                });
            }
        }
    }
}

fn is_runtime_http(text: &str) -> bool {
    reqwest::Url::parse(text).is_ok_and(|url| matches!(url.scheme(), "http" | "https"))
}

fn value_to_payload(value: &Value) -> Bytes {
    match value {
        Value::String(text) => Bytes::from(text.clone()),
        other => match serde_json::to_vec(other) {
            Ok(bytes) => Bytes::from(bytes),
            Err(err) => {
                tracing::warn!(error = %err, "规则转换序列化失败");
                Bytes::new()
            }
        },
    }
}

fn context_value(ctx: &HookContext) -> Value {
    json!({
        "client_id": ctx.client_id,
        "username": ctx.username,
        "topic": ctx.topic,
        "qos": ctx.qos,
        "retain": ctx.retain,
        "payload": parse_payload(&ctx.payload),
        "event": ctx.event.as_str(),
        "peer": ctx.peer,
        "filter": ctx.filter,
    })
}

fn parse_payload(bytes: &[u8]) -> Value {
    if bytes.is_empty() {
        return Value::String(String::new());
    }
    serde_json::from_slice(bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(bytes).into_owned()))
}

fn control_context(
    event: HookEvent,
    client_id: String,
    username: Option<String>,
    peer: Option<String>,
    filter: Option<String>,
) -> HookContext {
    let topic = event.topic().unwrap_or_default().to_owned();
    let mut body = json!({
        "client_id": client_id,
        "username": username,
    });
    if let Some(peer) = &peer {
        body["peer"] = json!(peer);
    }
    if let Some(filter) = &filter {
        body["filter"] = json!(filter);
    }
    let payload = match serde_json::to_vec(&body) {
        Ok(bytes) => Bytes::from(bytes),
        Err(_) => Bytes::new(),
    };
    HookContext {
        event,
        client_id,
        username,
        topic,
        payload,
        qos: 0,
        retain: false,
        peer,
        filter,
    }
}

struct EngineHooks {
    engine: RuleEngine,
}

#[async_trait::async_trait]
impl BrokerHooks for EngineHooks {
    async fn on_connect(&self, event: ConnectEvent) -> Result<Vec<HookPublish>, HookReject> {
        let ctx = control_context(
            HookEvent::Connect,
            event.client_id,
            event.username,
            event.peer,
            None,
        );
        self.engine.run_hooks(&ctx).await?;
        let effect = self.engine.evaluate(&ctx);
        if effect.reject {
            return Err(HookReject {
                reason: "rule rejected the event".to_owned(),
            });
        }
        Ok(effect.republish)
    }

    async fn on_disconnect(&self, event: DisconnectEvent) -> Vec<HookPublish> {
        let ctx = control_context(
            HookEvent::Disconnect,
            event.client_id,
            event.username,
            event.peer,
            None,
        );
        if self.engine.run_hooks(&ctx).await.is_err() {
            return Vec::new();
        }
        self.engine.evaluate(&ctx).republish
    }

    async fn on_publish(&self, event: PublishEvent) -> PublishDecision {
        let ctx = HookContext {
            event: HookEvent::Publish,
            client_id: event.client_id,
            username: event.username,
            topic: event.topic,
            payload: event.payload,
            qos: event.qos,
            retain: event.retain,
            peer: None,
            filter: None,
        };
        if self.engine.run_hooks(&ctx).await.is_err() {
            return PublishDecision {
                forward: false,
                payload: None,
                republish: Vec::new(),
            };
        }
        self.engine.evaluate(&ctx).decision()
    }

    async fn on_subscribe(&self, event: SubscribeEvent) -> Result<Vec<HookPublish>, HookReject> {
        let ctx = control_context(
            HookEvent::Subscribe,
            event.client_id,
            event.username,
            None,
            Some(event.filter),
        );
        self.engine.run_hooks(&ctx).await?;
        let effect = self.engine.evaluate(&ctx);
        if effect.reject {
            return Err(HookReject {
                reason: "rule rejected the event".to_owned(),
            });
        }
        Ok(effect.republish)
    }

    async fn on_unsubscribe(&self, event: UnsubscribeEvent) -> Vec<HookPublish> {
        let ctx = control_context(
            HookEvent::Unsubscribe,
            event.client_id,
            event.username,
            None,
            Some(event.filter),
        );
        if self.engine.run_hooks(&ctx).await.is_err() {
            return Vec::new();
        }
        self.engine.evaluate(&ctx).republish
    }
}

fn lock(inner: &Mutex<Inner>) -> std::sync::MutexGuard<'_, Inner> {
    match inner.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}
