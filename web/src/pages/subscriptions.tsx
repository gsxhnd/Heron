import { useCallback } from "react"

import { PageHeader, QueryState } from "@/components/page"
import { Badge } from "@/components/ui/badge"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { listSubscriptions } from "@/lib/api"
import { useI18n } from "@/lib/use-i18n"
import { useLoad } from "@/lib/use-load"
import { usePreferences } from "@/stores/preferences"

export function SubscriptionsPage() {
  const { t } = useI18n()
  const apiBase = usePreferences((state) => state.apiBase)
  const token = usePreferences((state) => state.token)
  const load = useCallback(() => listSubscriptions({ apiBase, token }), [apiBase, token])
  const query = useLoad(load)
  const rows = query.data ?? []

  return (
    <div className="flex flex-col gap-4">
      <PageHeader title={t("subscriptions.title")} description={t("subscriptions.description")} />
      <QueryState loading={query.loading && !query.data} error={query.error} empty={rows.length === 0} emptyTitle={t("subscriptions.empty")}>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>{t("subscriptions.client")}</TableHead>
              <TableHead>{t("subscriptions.topic")}</TableHead>
              <TableHead>{t("subscriptions.qos")}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((item) => (
              <TableRow key={`${item.client_id}:${item.topic}`}>
                <TableCell>{item.client_id}</TableCell>
                <TableCell className="font-mono text-xs">{item.topic}</TableCell>
                <TableCell>
                  <Badge variant="outline">{item.qos}</Badge>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </QueryState>
    </div>
  )
}
