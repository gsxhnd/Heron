import { useCallback } from "react"

import { PageHeader, QueryState } from "@/components/page"
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group"
import { formatUptime } from "@/lib/format"
import type { Locale } from "@/lib/i18n"
import { useI18n } from "@/lib/use-i18n"
import { useLoad } from "@/lib/use-load"
import { systemStatus } from "@/lib/api"
import { useTheme } from "@/components/theme-provider"
import { usePreferences } from "@/stores/preferences"

export function SettingsPage() {
  const { t } = useI18n()
  const { theme, setTheme } = useTheme()
  const locale = usePreferences((state) => state.locale)
  const apiBase = usePreferences((state) => state.apiBase)
  const token = usePreferences((state) => state.token)
  const setLocale = usePreferences((state) => state.setLocale)
  const setApiBase = usePreferences((state) => state.setApiBase)
  const setToken = usePreferences((state) => state.setToken)
  const load = useCallback(() => systemStatus({ apiBase, token }), [apiBase, token])
  const query = useLoad(load)

  return (
    <div className="flex flex-col gap-4">
      <PageHeader title={t("settings.title")} description={t("settings.description")} />
      <Card>
        <CardHeader>
          <CardTitle>{t("settings.connection")}</CardTitle>
          <CardDescription>{t("settings.connectionHelp")}</CardDescription>
        </CardHeader>
        <CardContent>
          <FieldGroup>
            <Field>
              <FieldLabel htmlFor="api-base">{t("settings.baseUrl")}</FieldLabel>
              <Input id="api-base" value={apiBase} placeholder="http://127.0.0.1:8080" onChange={(event) => setApiBase(event.target.value)} />
            </Field>
            <Field>
              <FieldLabel htmlFor="api-token">{t("settings.token")}</FieldLabel>
              <Input id="api-token" type="password" value={token} autoComplete="off" onChange={(event) => setToken(event.target.value)} />
            </Field>
          </FieldGroup>
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <CardTitle>{t("settings.appearance")}</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-col gap-4">
          <Field>
            <FieldLabel>{t("settings.theme")}</FieldLabel>
            <ToggleGroup variant="outline" value={[theme]} onValueChange={(values) => {
              const next = values[0]
              if (next === "light" || next === "dark" || next === "system") {
                setTheme(next)
              }
            }}>
              <ToggleGroupItem value="light">{t("settings.themeLight")}</ToggleGroupItem>
              <ToggleGroupItem value="dark">{t("settings.themeDark")}</ToggleGroupItem>
              <ToggleGroupItem value="system">{t("settings.themeSystem")}</ToggleGroupItem>
            </ToggleGroup>
          </Field>
          <Field>
            <FieldLabel>{t("settings.language")}</FieldLabel>
            <ToggleGroup variant="outline" value={[locale]} onValueChange={(values) => {
              const next = values[0]
              if (next === "zh-CN" || next === "en-US") {
                setLocale(next satisfies Locale)
              }
            }}>
              <ToggleGroupItem value="zh-CN">简体中文</ToggleGroupItem>
              <ToggleGroupItem value="en-US">English</ToggleGroupItem>
            </ToggleGroup>
          </Field>
        </CardContent>
      </Card>
      <Alert>
        <AlertTitle>{t("settings.security")}</AlertTitle>
        <AlertDescription>{t("settings.securityBody")}</AlertDescription>
      </Alert>
      <Alert>
        <AlertTitle>{t("settings.logs")}</AlertTitle>
        <AlertDescription>{t("settings.logsBody")}</AlertDescription>
      </Alert>
      <Card>
        <CardHeader>
          <CardTitle>{t("settings.runtime")}</CardTitle>
        </CardHeader>
        <CardContent>
          <QueryState loading={query.loading && !query.data} error={query.error}>
            {query.data ? (
              <div className="flex flex-col gap-2 text-sm">
                <div className="flex justify-between gap-3">
                  <span className="text-muted-foreground">{t("dashboard.uptime")}</span>
                  <span>{formatUptime(query.data.uptime)}</span>
                </div>
                <div className="flex justify-between gap-3">
                  <span className="text-muted-foreground">{t("dashboard.connections")}</span>
                  <span>{query.data.connections}</span>
                </div>
                <div className="flex justify-between gap-3">
                  <span className="text-muted-foreground">{t("dashboard.received")}</span>
                  <span>{query.data.messages_received}</span>
                </div>
                <div className="flex justify-between gap-3">
                  <span className="text-muted-foreground">{t("dashboard.sent")}</span>
                  <span>{query.data.messages_sent}</span>
                </div>
              </div>
            ) : null}
          </QueryState>
        </CardContent>
      </Card>
    </div>
  )
}
