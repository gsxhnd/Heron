import { useCallback, useState } from "react"

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
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field"
import { Switch } from "@/components/ui/switch"
import { Textarea } from "@/components/ui/textarea"
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group"
import { toast } from "@/components/ui/toast"
import { getTopic, listTopics, publishTopic } from "@/lib/api"
import { useI18n } from "@/lib/use-i18n"
import { useLoad } from "@/lib/use-load"
import { buildTopicTree, type TopicNode } from "@/lib/topics"
import { preferenceRequest, usePreferences } from "@/stores/preferences"

export function TopicsPage() {
  const { t } = useI18n()
  const apiBase = usePreferences((state) => state.apiBase)
  const token = usePreferences((state) => state.token)
  const [selected, setSelected] = useState<string | null>(null)
  const [publishOpen, setPublishOpen] = useState(false)
  const [clearOpen, setClearOpen] = useState(false)
  const load = useCallback(() => listTopics({ apiBase, token }), [apiBase, token])
  const query = useLoad(load)
  const tree = buildTopicTree(query.data ?? [])
  const detail = useLoad(
    useCallback(async () => {
      if (!selected) {
        return null
      }
      return getTopic(selected, { apiBase, token })
    }, [apiBase, token, selected]),
  )

  return (
    <div className="flex flex-col gap-4">
      <PageHeader title={t("topics.title")} description={t("topics.description")} />
      <QueryState loading={query.loading && !query.data} error={query.error} empty={(query.data?.length ?? 0) === 0} emptyTitle={t("topics.empty")}>
        <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
          <Card>
            <CardHeader>
              <CardTitle>{t("topics.title")}</CardTitle>
            </CardHeader>
            <CardContent>
              <ul className="flex flex-col gap-1">
                {tree.map((node) => (
                  <TopicBranch key={node.path} node={node} selected={selected} onSelect={setSelected} />
                ))}
              </ul>
            </CardContent>
          </Card>
          <Card>
            <CardHeader>
              <CardTitle>{selected ?? t("topics.payload")}</CardTitle>
            </CardHeader>
            <CardContent className="flex flex-col gap-3">
              {detail.error ? (
                <p className="text-sm text-destructive">{detail.error.message}</p>
              ) : selected && detail.data ? (
                <>
                  <div className="flex flex-wrap gap-2">
                    <Badge variant="outline">
                      {t("topics.subscribers")} {detail.data.subscribers}
                    </Badge>
                    {detail.data.retained ? <Badge>{t("topics.retained")}</Badge> : null}
                  </div>
                  <p className="font-mono text-xs break-all">
                    {detail.data.retained
                      ? (detail.data.payload_text ?? `${t("topics.binary")} ${detail.data.payload_hex ?? ""}`)
                      : t("topics.noPayload")}
                  </p>
                  <div className="flex gap-2">
                    <Button onClick={() => setPublishOpen(true)}>{t("action.publish")}</Button>
                    <Button variant="outline" disabled={!detail.data.retained} onClick={() => setClearOpen(true)}>
                      {t("action.clearRetained")}
                    </Button>
                  </div>
                </>
              ) : (
                <p className="text-sm text-muted-foreground">{t("topics.empty")}</p>
              )}
            </CardContent>
          </Card>
        </div>
      </QueryState>
      <PublishDialog
        open={publishOpen}
        topic={selected}
        onOpenChange={setPublishOpen}
        onDone={() => {
          query.reload()
          detail.reload()
        }}
      />
      <AlertDialog open={clearOpen} onOpenChange={setClearOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("topics.clearTitle")}</AlertDialogTitle>
            <AlertDialogDescription>{t("topics.clearBody", { topic: selected ?? "" })}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("action.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => {
                if (!selected) {
                  return
                }
                void publishTopic(selected, { payload: "", qos: 0, retain: true }, preferenceRequest()).then(() => {
                  toast.add({ title: t("action.clearRetained"), type: "success" })
                  setClearOpen(false)
                  query.reload()
                  detail.reload()
                })
              }}
            >
              {t("action.clearRetained")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}

function TopicBranch({
  node,
  selected,
  onSelect,
}: {
  node: TopicNode
  selected: string | null
  onSelect: (path: string) => void
}) {
  return (
    <li className="flex flex-col gap-1">
      <button
        type="button"
        className="flex items-center justify-between gap-2 rounded-md px-2 py-1 text-left text-sm hover:bg-muted aria-current:bg-muted"
        aria-current={selected === node.path ? "true" : undefined}
        onClick={() => {
          if (node.subscribers !== null || node.retained) {
            onSelect(node.path)
          }
        }}
      >
        <span className="truncate font-mono text-xs">{node.name}</span>
        <span className="text-xs text-muted-foreground">
          {node.subscribers ?? "—"}
          {node.retained ? " · R" : ""}
        </span>
      </button>
      {node.children.length > 0 ? (
        <ul className="flex flex-col gap-1 pl-4">
          {node.children.map((child) => (
            <TopicBranch key={child.path} node={child} selected={selected} onSelect={onSelect} />
          ))}
        </ul>
      ) : null}
    </li>
  )
}

function PublishDialog({
  open,
  topic,
  onOpenChange,
  onDone,
}: {
  open: boolean
  topic: string | null
  onOpenChange: (open: boolean) => void
  onDone: () => void
}) {
  const { t } = useI18n()
  const [payload, setPayload] = useState("")
  const [qos, setQos] = useState("0")
  const [retain, setRetain] = useState(false)
  const [invalid, setInvalid] = useState(false)

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{t("topics.publishTitle")}</DialogTitle>
          <DialogDescription>{topic}</DialogDescription>
        </DialogHeader>
        <FieldGroup>
          <Field data-invalid={invalid || undefined}>
            <FieldLabel htmlFor="payload">{t("topics.payload")}</FieldLabel>
            <Textarea id="payload" value={payload} aria-invalid={invalid || undefined} onChange={(event) => setPayload(event.target.value)} />
          </Field>
          <Field>
            <FieldLabel>{t("subscriptions.qos")}</FieldLabel>
            <ToggleGroup variant="outline" value={[qos]} onValueChange={(values) => values[0] && setQos(values[0])}>
              <ToggleGroupItem value="0">0</ToggleGroupItem>
              <ToggleGroupItem value="1">1</ToggleGroupItem>
              <ToggleGroupItem value="2">2</ToggleGroupItem>
            </ToggleGroup>
          </Field>
          <Field orientation="horizontal">
            <Switch id="retain" checked={retain} onCheckedChange={(checked) => setRetain(checked === true)} />
            <FieldLabel htmlFor="retain">{t("topics.retained")}</FieldLabel>
          </Field>
        </FieldGroup>
        <DialogFooter>
          <Button
            onClick={() => {
              if (!topic) {
                setInvalid(true)
                return
              }
              void publishTopic(topic, { payload, qos: Number(qos), retain }, preferenceRequest()).then(() => {
                toast.add({ title: t("action.publish"), type: "success" })
                setPayload("")
                onOpenChange(false)
                onDone()
              })
            }}
          >
            {t("action.publish")}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
