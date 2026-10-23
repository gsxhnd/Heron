import { useCallback, useState } from "react"
import { Link, useParams } from "@tanstack/react-router"

import { PageHeader, QueryState } from "@/components/page"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { toast } from "@/components/ui/toast"
import { disconnectClient, getClient, listSubscriptions } from "@/lib/api"
import { protocolLabel } from "@/lib/format"
import { useI18n } from "@/lib/use-i18n"
import { useLoad } from "@/lib/use-load"
import { preferenceRequest, usePreferences } from "@/stores/preferences"

export function ClientPage() {
  const { clientId } = useParams({ from: "/clients/$clientId" })
  const { t } = useI18n()
  const apiBase = usePreferences((state) => state.apiBase)
  const token = usePreferences((state) => state.token)
  const [confirm, setConfirm] = useState(false)
  const load = useCallback(async () => {
    const options = { apiBase, token }
    const [client, subscriptions] = await Promise.all([
      getClient(clientId, options),
      listSubscriptions(options),
    ])
    return {
      client,
      subscriptions: subscriptions.filter((item) => item.client_id === clientId),
    }
  }, [apiBase, token, clientId])
  const query = useLoad(load)
  const client = query.data?.client

  return (
    <div className="flex flex-col gap-4">
      <PageHeader
        title={clientId}
        description={t("clients.detail")}
        actions={
          <div className="flex gap-2">
            <Button variant="outline" render={<Link to="/clients" />} nativeButton={false}>
              {t("nav.clients")}
            </Button>
            <Button variant="destructive" disabled={!client?.connected} onClick={() => setConfirm(true)}>
              {t("action.disconnect")}
            </Button>
          </div>
        }
      />
      <QueryState loading={query.loading && !query.data} error={query.error}>
        {client ? (
          <div className="grid gap-4 lg:grid-cols-2">
            <Card>
              <CardHeader>
                <CardTitle>{t("clients.detail")}</CardTitle>
              </CardHeader>
              <CardContent className="flex flex-col gap-2 text-sm">
                <Row label={t("clients.protocol")} value={protocolLabel(client.protocol_version)} />
                <Row label={t("clients.address")} value={client.ip_address ?? "—"} />
                <Row label={t("clients.username")} value={client.username ?? t("clients.anonymous")} />
                <Row label={t("clients.status")} value={client.connected ? t("clients.online") : t("clients.offline")} />
                <Row label={t("clients.connectedAt")} value={client.connected_at ?? "—"} />
                <Row label={t("clients.keepAlive")} value={t("common.seconds", { value: client.keep_alive })} />
                <Row label={t("clients.cleanStart")} value={client.clean_start ? t("common.yes") : t("common.no")} />
                <Row
                  label={t("clients.sessionExpiry")}
                  value={
                    client.session_expiry === null
                      ? t("clients.expiryNever")
                      : client.session_expiry === 0
                        ? t("clients.expiryZero")
                        : t("common.seconds", { value: client.session_expiry })
                  }
                />
              </CardContent>
            </Card>
            <Card>
              <CardHeader>
                <CardTitle>{t("clients.subscriptions")}</CardTitle>
              </CardHeader>
              <CardContent>
                {(query.data?.subscriptions.length ?? 0) === 0 ? (
                  <p className="text-sm text-muted-foreground">{t("subscriptions.empty")}</p>
                ) : (
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>{t("subscriptions.topic")}</TableHead>
                        <TableHead>{t("subscriptions.qos")}</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {query.data?.subscriptions.map((item) => (
                        <TableRow key={item.topic}>
                          <TableCell className="font-mono text-xs">{item.topic}</TableCell>
                          <TableCell>
                            <Badge variant="outline">{item.qos}</Badge>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                )}
              </CardContent>
            </Card>
          </div>
        ) : null}
      </QueryState>
      <AlertDialog open={confirm} onOpenChange={setConfirm}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("clients.confirmDisconnect")}</AlertDialogTitle>
            <AlertDialogDescription>{clientId}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("action.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              variant="destructive"
              onClick={() => {
                void disconnectClient(clientId, preferenceRequest()).then(() => {
                  toast.add({ title: t("action.disconnect"), type: "success" })
                  setConfirm(false)
                  query.reload()
                })
              }}
            >
              {t("action.disconnect")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}

function Row({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-center justify-between gap-3">
      <span className="text-muted-foreground">{label}</span>
      <span>{value}</span>
    </div>
  )
}
