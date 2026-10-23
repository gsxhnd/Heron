export interface ClientRecord {
  client_id: string
  protocol_version: number
  ip_address: string | null
  connected: boolean
  connected_at: string | null
  username: string | null
  keep_alive: number
  clean_start: boolean
  session_expiry: number | null
}

export interface ClientPage {
  total: number
  page: number
  page_size: number
  data: ClientRecord[]
}

export interface TopicRecord {
  topic: string
  subscribers: number
  retained: boolean
}

export interface TopicDetail {
  topic: string
  subscribers: number
  retained: boolean
  payload_text: string | null
  payload_hex: string | null
  qos: number | null
}

export interface SubscriptionRecord {
  client_id: string
  topic: string
  qos: number
}

export interface StatusRecord {
  uptime: number
  connections: number
  messages_received: number
  messages_sent: number
  sessions?: number
}

export interface RuleRecord {
  id: number
  name: string
  description: string | null
  enabled: boolean
  priority: number
  topic_filter: string
  conditions: unknown
  actions: unknown
  created_at: string
  updated_at: string
}

export interface RuleInput {
  name: string
  description: string
  enabled: boolean
  priority: number
  topic_filter: string
  conditionsText: string
  actionsText: string
}

export interface RuleList {
  rules: RuleRecord[]
  engine: string | null
}

export interface Snapshot {
  type: "snapshot"
  uptime: number
  connections: number
  sessions: number
  messages_received: number
  messages_sent: number
  receive_per_sec: number
  send_per_sec: number
  topics: number
  subscriptions: number
}

export interface ClientNotice {
  type: "client"
  action: "connected" | "disconnected"
  client_id: string
  protocol_version?: number
}

export interface Sample {
  at: number
  connections: number
  receivePerSec: number
  sendPerSec: number
}
