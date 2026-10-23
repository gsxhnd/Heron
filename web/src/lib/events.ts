import type { ClientNotice, Snapshot } from "./types.ts"

function numberField(value: unknown, key: string): number | null {
  if (typeof value !== "object" || value === null || !(key in value)) {
    return null
  }
  const field = (value as Record<string, unknown>)[key]
  return typeof field === "number" && Number.isFinite(field) ? field : null
}

function stringField(value: unknown, key: string): string | null {
  if (typeof value !== "object" || value === null) {
    return null
  }
  const field = (value as Record<string, unknown>)[key]
  return typeof field === "string" ? field : null
}

export function parseSnapshot(value: unknown): Snapshot | null {
  if (stringField(value, "type") !== "snapshot") {
    return null
  }
  const uptime = numberField(value, "uptime")
  const connections = numberField(value, "connections")
  const sessions = numberField(value, "sessions")
  const messagesReceived = numberField(value, "messages_received")
  const messagesSent = numberField(value, "messages_sent")
  const receivePerSec = numberField(value, "receive_per_sec")
  const sendPerSec = numberField(value, "send_per_sec")
  const topics = numberField(value, "topics")
  const subscriptions = numberField(value, "subscriptions")
  if (
    uptime === null ||
    connections === null ||
    sessions === null ||
    messagesReceived === null ||
    messagesSent === null ||
    receivePerSec === null ||
    sendPerSec === null ||
    topics === null ||
    subscriptions === null
  ) {
    return null
  }
  return {
    type: "snapshot",
    uptime,
    connections,
    sessions,
    messages_received: messagesReceived,
    messages_sent: messagesSent,
    receive_per_sec: receivePerSec,
    send_per_sec: sendPerSec,
    topics,
    subscriptions,
  }
}

export function parseClientNotice(value: unknown): ClientNotice | null {
  if (stringField(value, "type") !== "client") {
    return null
  }
  const action = stringField(value, "action")
  const clientId = stringField(value, "client_id")
  if ((action !== "connected" && action !== "disconnected") || clientId === null) {
    return null
  }
  const protocol = numberField(value, "protocol_version")
  return {
    type: "client",
    action,
    client_id: clientId,
    ...(protocol === null ? {} : { protocol_version: protocol }),
  }
}

export function eventsUrl(pageUrl: string, apiBase: string, token: string): string {
  const trimmed = apiBase.trim()
  const origin = trimmed.length > 0 ? new URL(trimmed) : new URL(pageUrl)
  const protocol = origin.protocol === "https:" ? "wss:" : "ws:"
  const url = new URL(`${protocol}//${origin.host}/api/v1/events`)
  if (token.length > 0) {
    url.searchParams.set("token", token)
  }
  return url.toString()
}

export function apiUrl(path: string, apiBase: string): string {
  const base = apiBase.trim().replace(/\/$/, "")
  return `${base}${path}`
}
