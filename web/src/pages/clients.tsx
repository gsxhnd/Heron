import { useCallback, useState } from "react"
import { Link } from "@tanstack/react-router"

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
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group"
import { Badge } from "@/components/ui/badge"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { disconnectClient, listClients } from "@/lib/api"
import { protocolLabel } from "@/lib/format"
import { useI18n } from "@/lib/use-i18n"
import { useLoad } from "@/lib/use-load"
import { preferenceRequest, usePreferences } from "@/stores/preferences"
import { toast } from "@/components/ui/toast"

export function ClientsPage() {
  const { t } = useI18n()
  const apiBase = usePreferences((state) => state.apiBase)
  const token = usePreferences((state) => state.token)
  const [page, setPage] = useState(1)
  const [clientId, setClientId] = useState("")
  const [connected, setConnected] = useState("all")
  const [protocol, setProtocol] = useState("all")
  const [selected, setSelected] = useState<string[]>([])
  const [confirm, setConfirm] = useState<string[] | null>(null)
  const load = useCallback(
    () =>
      listClients({
        apiBase,
        token,
        page,
        pageSize: 20,
        clientId: clientId.trim(),
        connected: connected === "all" ? undefined : connected === "true",
        protocol: protocol === "all" ? undefined : Number(protocol),
      }),
    [apiBase, token, page, clientId, connected, protocol],
  )
  const query = useLoad(load)
  const rows = query.data?.data ?? []

  async function disconnect(ids: string[]) {
    await Promise.all(ids.map((id) => disconnectClient(id, preferenceRequest())))
    setSelected([])
    setConfirm(null)
    query.reload()
    toast.add({ title: t("action.disconnect"), type: "success" })
  }

  return (
    <div className="flex flex-col gap-4">
      <PageHeader
        title={t("clients.title")}
        description={t("clients.description")}
        actions={
          <Button variant="destructive" disabled={selected.length === 0} onClick={() => setConfirm(selected)}>
            {t("action.disconnectSelected")}
          </Button>
        }
      />
      <div className="flex flex-col gap-3 lg:flex-row lg:items-center">
        <Input
          value={clientId}
          placeholder={t("clients.search")}
          aria-label={t("clients.search")}
          onChange={(event) => {
            setPage(1)
            setClientId(event.target.value)
          }}
        />
        <ToggleGroup
          variant="outline"
          spacing={2}
          value={[connected]}
          onValueChange={(values) => {
            const next = values[0]
            if (next) {
              setPage(1)
              setConnected(next)
            }
          }}
        >
          <ToggleGroupItem value="all">{t("clients.all")}</ToggleGroupItem>
          <ToggleGroupItem value="true">{t("clients.online")}</ToggleGroupItem>
          <ToggleGroupItem value="false">{t("clients.offline")}</ToggleGroupItem>
        </ToggleGroup>
        <ToggleGroup
          variant="outline"
          spacing={2}
          value={[protocol]}
          onValueChange={(values) => {
            const next = values[0]
            if (next) {
              setPage(1)
              setProtocol(next)
            }
          }}
        >
          <ToggleGroupItem value="all">{t("clients.all")}</ToggleGroupItem>
          <ToggleGroupItem value="4">3.1.1</ToggleGroupItem>
          <ToggleGroupItem value="5">5.0</ToggleGroupItem>
        </ToggleGroup>
      </div>
      <QueryState loading={query.loading && !query.data} error={query.error} empty={rows.length === 0} emptyTitle={t("clients.empty")}>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-10" />
              <TableHead>{t("clients.id")}</TableHead>
              <TableHead>{t("clients.protocol")}</TableHead>
              <TableHead>{t("clients.address")}</TableHead>
              <TableHead>{t("clients.status")}</TableHead>
              <TableHead>{t("clients.connectedAt")}</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {rows.map((client) => (
              <TableRow key={client.client_id}>
                <TableCell>
                  <Checkbox
                    aria-label={client.client_id}
                    disabled={!client.connected}
                    checked={selected.includes(client.client_id)}
                    onCheckedChange={(checked) => {
                      setSelected((current) =>
                        checked === true
                          ? [...current, client.client_id]
                          : current.filter((id) => id !== client.client_id),
                      )
                    }}
                  />
                </TableCell>
                <TableCell>
                  <Link to="/clients/$clientId" params={{ clientId: client.client_id }} className="underline-offset-4 hover:underline">
                    {client.client_id}
                  </Link>
                </TableCell>
                <TableCell>{protocolLabel(client.protocol_version)}</TableCell>
                <TableCell>{client.ip_address ?? "—"}</TableCell>
                <TableCell>
                  <Badge variant={client.connected ? "secondary" : "outline"}>
                    {client.connected ? t("clients.online") : t("clients.offline")}
                  </Badge>
                </TableCell>
                <TableCell>{client.connected_at ?? "—"}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </QueryState>
      <div className="flex items-center justify-between gap-3">
        <p className="text-sm text-muted-foreground">
          {t("clients.page", { page, total: query.data?.total ?? 0 })}
        </p>
        <div className="flex gap-2">
          <Button variant="outline" size="sm" disabled={page <= 1} onClick={() => setPage((value) => value - 1)}>
            {t("common.previous")}
          </Button>
          <Button
            variant="outline"
            size="sm"
            disabled={!query.data || page * query.data.page_size >= query.data.total}
            onClick={() => setPage((value) => value + 1)}
          >
            {t("common.next")}
          </Button>
        </div>
      </div>
      <AlertDialog open={confirm !== null} onOpenChange={(open) => !open && setConfirm(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {confirm && confirm.length > 1
                ? t("clients.confirmBatch", { count: confirm.length })
                : t("clients.confirmDisconnect")}
            </AlertDialogTitle>
            <AlertDialogDescription>{confirm?.join(", ")}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("action.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              variant="destructive"
              onClick={() => {
                if (confirm) {
                  void disconnect(confirm)
                }
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
