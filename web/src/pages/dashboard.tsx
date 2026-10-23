import { useCallback } from "react"
import { Area, AreaChart, CartesianGrid, Line, LineChart, XAxis } from "recharts"

import { PageHeader, QueryState } from "@/components/page"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { ChartContainer, ChartTooltip, ChartTooltipContent, type ChartConfig } from "@/components/ui/chart"
import { listSubscriptions, listTopics, systemStatus } from "@/lib/api"
import { formatRate, formatUptime } from "@/lib/format"
import { useI18n } from "@/lib/use-i18n"
import { useLoad } from "@/lib/use-load"
import { useLive } from "@/stores/live"
import { usePreferences } from "@/stores/preferences"

export function DashboardPage() {
  const { locale, t } = useI18n()
  const apiBase = usePreferences((state) => state.apiBase)
  const token = usePreferences((state) => state.token)
  const snapshot = useLive((state) => state.snapshot)
  const samples = useLive((state) => state.samples)
  const notices = useLive((state) => state.notices)
  const load = useCallback(async () => {
    const options = { apiBase, token }
    const [status, topics, subscriptions] = await Promise.all([
      systemStatus(options),
      listTopics(options),
      listSubscriptions(options),
    ])
    return { status, topics, subscriptions }
  }, [apiBase, token])
  const query = useLoad(load)
  const topics = query.data?.topics ?? []
  const top = [...topics].sort((left, right) => right.subscribers - left.subscribers).slice(0, 10)
  const connections = snapshot?.connections ?? query.data?.status.connections ?? 0
  const topicCount = snapshot?.topics ?? topics.length
  const subscriptionCount = snapshot?.subscriptions ?? query.data?.subscriptions.length ?? 0
  const receive = snapshot?.receive_per_sec ?? 0
  const sent = snapshot?.send_per_sec ?? 0
  const throughputConfig = {
    received: { label: t("dashboard.received"), color: "var(--chart-1)" },
    sent: { label: t("dashboard.sent"), color: "var(--chart-2)" },
  } satisfies ChartConfig
  const connectionConfig = {
    connections: { label: t("dashboard.connections"), color: "var(--chart-3)" },
  } satisfies ChartConfig

  return (
    <div className="flex flex-col gap-4">
      <PageHeader title={t("dashboard.title")} description={t("dashboard.description")} />
      <QueryState loading={query.loading && !query.data} error={query.error}>
        <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
          <Metric label={t("dashboard.connections")} value={String(connections)} />
          <Metric label={t("dashboard.topics")} value={String(topicCount)} />
          <Metric label={t("dashboard.rate")} value={`${formatRate(receive)} / ${formatRate(sent)}`} />
          <Metric label={t("dashboard.subscriptions")} value={String(subscriptionCount)} />
        </div>
      </QueryState>
      <Alert>
        <AlertDescription>{t("dashboard.resources")}</AlertDescription>
      </Alert>
      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>{t("dashboard.throughput")}</CardTitle>
            <CardDescription>
              {t("dashboard.received")} / {t("dashboard.sent")}
            </CardDescription>
          </CardHeader>
          <CardContent>
            <ChartContainer config={throughputConfig} className="aspect-auto h-56 w-full">
              <LineChart data={samples}>
                <CartesianGrid vertical={false} />
                <XAxis dataKey="at" tickFormatter={(value: number) => formatClock(value, locale)} minTickGap={24} />
                <ChartTooltip content={<ChartTooltipContent />} />
                <Line dataKey="receivePerSec" name="received" stroke="var(--color-received)" dot={false} />
                <Line dataKey="sendPerSec" name="sent" stroke="var(--color-sent)" dot={false} />
              </LineChart>
            </ChartContainer>
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>{t("dashboard.connectionTrend")}</CardTitle>
            <CardDescription>
              {t("dashboard.uptime")} {formatUptime(snapshot?.uptime ?? query.data?.status.uptime ?? 0)} · {t("dashboard.sessions")} {snapshot?.sessions ?? "—"}
            </CardDescription>
          </CardHeader>
          <CardContent>
            <ChartContainer config={connectionConfig} className="aspect-auto h-56 w-full">
              <AreaChart data={samples}>
                <CartesianGrid vertical={false} />
                <XAxis dataKey="at" tickFormatter={(value: number) => formatClock(value, locale)} minTickGap={24} />
                <ChartTooltip content={<ChartTooltipContent />} />
                <Area dataKey="connections" stroke="var(--color-connections)" fill="var(--color-connections)" fillOpacity={0.2} />
              </AreaChart>
            </ChartContainer>
          </CardContent>
        </Card>
      </div>
      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>{t("dashboard.topTopics")}</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-2">
            {top.length === 0 ? (
              <p className="text-sm text-muted-foreground">{t("topics.empty")}</p>
            ) : (
              top.map((topic) => (
                <div key={topic.topic} className="flex items-center justify-between gap-3">
                  <span className="truncate font-mono text-xs">{topic.topic}</span>
                  <span className="text-sm text-muted-foreground">{topic.subscribers}</span>
                </div>
              ))
            )}
          </CardContent>
        </Card>
        <Card>
          <CardHeader>
            <CardTitle>{t("notices.title")}</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col gap-2">
            {notices.length === 0 ? (
              <p className="text-sm text-muted-foreground">{t("notices.empty")}</p>
            ) : (
              notices.map((notice, index) => (
                <p key={`${notice.client_id}-${index}`} className="text-sm">
                  {t(notice.action === "connected" ? "notices.connected" : "notices.disconnected", {
                    id: notice.client_id,
                  })}
                </p>
              ))
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

function Metric({ label, value }: { label: string; value: string }) {
  return (
    <Card>
      <CardHeader>
        <CardDescription>{label}</CardDescription>
        <CardTitle>{value}</CardTitle>
      </CardHeader>
    </Card>
  )
}

function formatClock(value: number, locale: string) {
  return new Date(value).toLocaleTimeString(locale, { hour: "2-digit", minute: "2-digit", second: "2-digit" })
}
