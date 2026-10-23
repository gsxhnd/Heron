import assert from "node:assert/strict"
import test from "node:test"

import { apiUrl, eventsUrl, parseClientNotice, parseSnapshot } from "./events.ts"
import { formatRate, formatUptime, protocolLabel } from "./format.ts"
import { catalogKeys, translate } from "./i18n.ts"
import { buildTopicTree } from "./topics.ts"
import { rulePayload } from "./api.ts"

test("formatters should render uptime, rate, and protocol labels", () => {
  assert.equal(formatUptime(45), "0m 45s")
  assert.equal(formatUptime(3700), "1h 1m")
  assert.equal(formatUptime(90_000), "1d 1h")
  assert.equal(formatRate(1.26), "1.3")
  assert.equal(formatRate(120.4), "120")
  assert.equal(protocolLabel(4), "3.1.1")
  assert.equal(protocolLabel(5), "5.0")
})

test("locales should expose the same keys", () => {
  assert.deepEqual(catalogKeys("zh-CN"), catalogKeys("en-US"))
  assert.equal(translate("zh-CN", "notices.connected", { id: "a" }), "a 已连接")
  assert.equal(translate("en-US", "notices.disconnected", { id: "a" }), "a disconnected")
})

test("topic tree should nest filters and keep retained flags on the leaf", () => {
  const tree = buildTopicTree([
    { topic: "sensors/temp", subscribers: 2, retained: true },
    { topic: "sensors", subscribers: 1, retained: false },
    { topic: "alerts", subscribers: 0, retained: false },
  ])
  assert.equal(tree.length, 2)
  const sensors = tree.find((node) => node.name === "sensors")
  assert.ok(sensors)
  assert.equal(sensors.subscribers, 1)
  assert.equal(sensors.children[0]?.path, "sensors/temp")
  assert.equal(sensors.children[0]?.retained, true)
  assert.equal(sensors.children[0]?.subscribers, 2)
})

test("event parsers should accept snapshots and client notices", () => {
  const snapshot = parseSnapshot({
    type: "snapshot",
    uptime: 1,
    connections: 2,
    sessions: 3,
    messages_received: 4,
    messages_sent: 5,
    receive_per_sec: 1.5,
    send_per_sec: 0,
    topics: 6,
    subscriptions: 7,
  })
  assert.equal(snapshot?.connections, 2)
  assert.equal(parseSnapshot({ type: "client" }), null)
  const notice = parseClientNotice({
    type: "client",
    action: "connected",
    client_id: "live-1",
    protocol_version: 4,
  })
  assert.equal(notice?.client_id, "live-1")
  assert.equal(parseClientNotice({ type: "client", action: "other", client_id: "x" }), null)
})

test("urls should follow the page origin unless an API base is set", () => {
  assert.equal(apiUrl("/api/v1/clients", ""), "/api/v1/clients")
  assert.equal(apiUrl("/health", "http://127.0.0.1:8080/"), "http://127.0.0.1:8080/health")
  assert.equal(
    eventsUrl("http://localhost:5173/clients", "", "secret"),
    "ws://localhost:5173/api/v1/events?token=secret",
  )
  assert.equal(
    eventsUrl("http://localhost:5173/", "https://broker.example", ""),
    "wss://broker.example/api/v1/events",
  )
})

test("rule payload should reject actions that are not an array", () => {
  assert.throws(() =>
    rulePayload({
      name: "forward",
      description: "",
      enabled: true,
      priority: 0,
      topic_filter: "a/#",
      conditionsText: "{}",
      actionsText: "{}",
    }),
  )
  const body = rulePayload({
    name: "forward",
    description: "",
    enabled: true,
    priority: 1,
    topic_filter: "a/#",
    conditionsText: "",
    actionsText: '[{"type":"log"}]',
  })
  assert.equal(body.conditions, null)
  assert.deepEqual(body.actions, [{ type: "log" }])
})
