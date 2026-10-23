import { apiUrl } from "./events.ts"
import type {
  ClientPage,
  ClientRecord,
  RuleInput,
  RuleList,
  RuleRecord,
  StatusRecord,
  SubscriptionRecord,
  TopicDetail,
  TopicRecord,
} from "./types.ts"

export class ApiError extends Error {
  readonly status: number
  readonly code: string

  constructor(status: number, code: string, message: string) {
    super(message)
    this.name = "ApiError"
    this.status = status
    this.code = code
  }
}

interface RequestOptions {
  apiBase: string
  token: string
  method?: string
  body?: unknown
}

export async function apiRequest<T>(path: string, options: RequestOptions): Promise<T> {
  const headers = new Headers()
  if (options.token.length > 0) {
    headers.set("Authorization", `Bearer ${options.token}`)
  }
  let body: string | undefined
  if (options.body !== undefined) {
    headers.set("Content-Type", "application/json")
    body = JSON.stringify(options.body)
  }
  const response = await fetch(apiUrl(path, options.apiBase), {
    method: options.method ?? "GET",
    headers,
    body,
  })
  if (response.status === 204) {
    return undefined as T
  }
  const text = await response.text()
  const payload: unknown = text.length > 0 ? JSON.parse(text) : null
  if (!response.ok) {
    const error = readError(payload)
    throw new ApiError(response.status, error.code, error.message)
  }
  return payload as T
}

function readError(payload: unknown): { code: string; message: string } {
  if (typeof payload === "object" && payload !== null && "error" in payload) {
    const error = (payload as { error?: { code?: unknown; message?: unknown } }).error
    if (error && typeof error.message === "string") {
      return {
        code: typeof error.code === "string" ? error.code : "error",
        message: error.message,
      }
    }
  }
  return { code: "error", message: "request failed" }
}

export function encodeTopic(topic: string): string {
  return topic
    .split("/")
    .map((level) => encodeURIComponent(level))
    .join("/")
}

export async function listClients(
  options: RequestOptions & {
    page: number
    pageSize: number
    connected?: boolean
    clientId?: string
    protocol?: number
  },
): Promise<ClientPage> {
  const params = new URLSearchParams({
    page: String(options.page),
    page_size: String(options.pageSize),
  })
  if (options.connected !== undefined) {
    params.set("connected", String(options.connected))
  }
  if (options.clientId && options.clientId.length > 0) {
    params.set("client_id", options.clientId)
  }
  if (options.protocol !== undefined) {
    params.set("protocol", String(options.protocol))
  }
  return apiRequest<ClientPage>(`/api/v1/clients?${params.toString()}`, options)
}

export async function getClient(clientId: string, options: RequestOptions): Promise<ClientRecord> {
  const payload = await apiRequest<{ data: ClientRecord }>(
    `/api/v1/clients/${encodeURIComponent(clientId)}`,
    options,
  )
  return payload.data
}

export async function disconnectClient(clientId: string, options: RequestOptions): Promise<void> {
  await apiRequest(`/api/v1/clients/${encodeURIComponent(clientId)}`, {
    ...options,
    method: "DELETE",
  })
}

export async function listTopics(options: RequestOptions): Promise<TopicRecord[]> {
  const payload = await apiRequest<{ data: TopicRecord[] }>("/api/v1/topics", options)
  return payload.data
}

export async function getTopic(topic: string, options: RequestOptions): Promise<TopicDetail> {
  const payload = await apiRequest<{ data: TopicDetail }>(
    `/api/v1/topics/${encodeTopic(topic)}`,
    options,
  )
  return payload.data
}

export async function publishTopic(
  topic: string,
  body: { payload: string; qos: number; retain: boolean },
  options: RequestOptions,
): Promise<void> {
  await apiRequest(`/api/v1/topics/${encodeTopic(topic)}/publish`, {
    ...options,
    method: "POST",
    body,
  })
}

export async function listSubscriptions(options: RequestOptions): Promise<SubscriptionRecord[]> {
  const payload = await apiRequest<{ data: SubscriptionRecord[] }>(
    "/api/v1/subscriptions",
    options,
  )
  return payload.data
}

export async function systemStatus(options: RequestOptions): Promise<StatusRecord> {
  return apiRequest<StatusRecord>("/api/v1/system/status", options)
}

export async function listRules(options: RequestOptions): Promise<RuleList> {
  const payload = await apiRequest<{ data: RuleRecord[]; engine?: string | null }>(
    "/api/v1/rules",
    options,
  )
  return { rules: payload.data, engine: payload.engine ?? null }
}

export function rulePayload(input: RuleInput): {
  name: string
  description: string
  enabled: boolean
  priority: number
  topic_filter: string
  conditions: unknown
  actions: unknown
} {
  let conditions: unknown
  let actions: unknown
  try {
    conditions = input.conditionsText.trim().length === 0 ? null : JSON.parse(input.conditionsText)
    actions = JSON.parse(input.actionsText)
  } catch {
    throw new ApiError(400, "bad_request", "invalid JSON")
  }
  if (!Array.isArray(actions) || (conditions !== null && typeof conditions !== "object")) {
    throw new ApiError(400, "bad_request", "invalid JSON")
  }
  if (Array.isArray(conditions)) {
    throw new ApiError(400, "bad_request", "invalid JSON")
  }
  return {
    name: input.name,
    description: input.description,
    enabled: input.enabled,
    priority: input.priority,
    topic_filter: input.topic_filter,
    conditions,
    actions,
  }
}

export async function createRule(input: RuleInput, options: RequestOptions): Promise<RuleRecord> {
  const payload = await apiRequest<{ data: RuleRecord }>("/api/v1/rules", {
    ...options,
    method: "POST",
    body: rulePayload(input),
  })
  return payload.data
}

export async function updateRule(
  id: number,
  input: RuleInput,
  options: RequestOptions,
): Promise<RuleRecord> {
  const payload = await apiRequest<{ data: RuleRecord }>(`/api/v1/rules/${id}`, {
    ...options,
    method: "PUT",
    body: rulePayload(input),
  })
  return payload.data
}

export async function deleteRule(id: number, options: RequestOptions): Promise<void> {
  await apiRequest(`/api/v1/rules/${id}`, { ...options, method: "DELETE" })
}
