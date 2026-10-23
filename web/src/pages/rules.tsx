import { useCallback, useState } from "react"

import { PageHeader, QueryState } from "@/components/page"
import { Alert, AlertDescription } from "@/components/ui/alert"
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
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Field, FieldDescription, FieldError, FieldGroup, FieldLabel } from "@/components/ui/field"
import { Input } from "@/components/ui/input"
import { Switch } from "@/components/ui/switch"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Textarea } from "@/components/ui/textarea"
import { toast } from "@/components/ui/toast"
import { ApiError, createRule, deleteRule, listRules, updateRule } from "@/lib/api"
import type { RuleInput, RuleRecord } from "@/lib/types"
import { useI18n } from "@/lib/use-i18n"
import { useLoad } from "@/lib/use-load"
import { preferenceRequest, usePreferences } from "@/stores/preferences"

const emptyInput = (): RuleInput => ({
  name: "",
  description: "",
  enabled: true,
  priority: 0,
  topic_filter: "",
  conditionsText: "",
  actionsText: "[]",
})

export function RulesPage() {
  const { t } = useI18n()
  const apiBase = usePreferences((state) => state.apiBase)
  const token = usePreferences((state) => state.token)
  const [editing, setEditing] = useState<RuleRecord | null | "new">(null)
  const [removeId, setRemoveId] = useState<number | null>(null)
  const load = useCallback(() => listRules({ apiBase, token }), [apiBase, token])
  const query = useLoad(load)
  const rules = query.data?.rules ?? []

  return (
    <div className="flex flex-col gap-4">
      <PageHeader
        title={t("rules.title")}
        description={t("rules.description")}
        actions={<Button onClick={() => setEditing("new")}>{t("action.create")}</Button>}
      />
      <Alert>
        <AlertDescription>{t("rules.engine")}</AlertDescription>
      </Alert>
      <QueryState loading={query.loading && !query.data} error={query.error} empty={rules.length === 0} emptyTitle={t("rules.empty")}>
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead>{t("rules.name")}</TableHead>
              <TableHead>{t("rules.topicFilter")}</TableHead>
              <TableHead>{t("rules.priority")}</TableHead>
              <TableHead>{t("rules.enabled")}</TableHead>
              <TableHead />
            </TableRow>
          </TableHeader>
          <TableBody>
            {rules.map((rule) => (
              <TableRow key={rule.id}>
                <TableCell>{rule.name}</TableCell>
                <TableCell className="font-mono text-xs">{rule.topic_filter}</TableCell>
                <TableCell>{rule.priority}</TableCell>
                <TableCell>
                  <Badge variant={rule.enabled ? "secondary" : "outline"}>
                    {rule.enabled ? t("rules.enabled") : t("rules.disabled")}
                  </Badge>
                </TableCell>
                <TableCell>
                  <div className="flex justify-end gap-2">
                    <Button variant="outline" size="sm" onClick={() => setEditing(rule)}>
                      {t("action.edit")}
                    </Button>
                    <Button variant="destructive" size="sm" onClick={() => setRemoveId(rule.id)}>
                      {t("action.delete")}
                    </Button>
                  </div>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </QueryState>
      <RuleDialog
        editing={editing}
        onOpenChange={(open) => {
          if (!open) {
            setEditing(null)
          }
        }}
        onDone={() => {
          setEditing(null)
          query.reload()
        }}
      />
      <AlertDialog open={removeId !== null} onOpenChange={(open) => !open && setRemoveId(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>{t("rules.deleteTitle")}</AlertDialogTitle>
            <AlertDialogDescription>{t("rules.description")}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>{t("action.cancel")}</AlertDialogCancel>
            <AlertDialogAction
              variant="destructive"
              onClick={() => {
                if (removeId === null) {
                  return
                }
                void deleteRule(removeId, preferenceRequest()).then(() => {
                  toast.add({ title: t("action.delete"), type: "success" })
                  setRemoveId(null)
                  query.reload()
                })
              }}
            >
              {t("action.delete")}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}

function ruleInput(editing: RuleRecord | "new"): RuleInput {
  if (editing === "new") {
    return emptyInput()
  }
  return {
    name: editing.name,
    description: editing.description ?? "",
    enabled: editing.enabled,
    priority: editing.priority,
    topic_filter: editing.topic_filter,
    conditionsText: editing.conditions === null ? "" : JSON.stringify(editing.conditions, null, 2),
    actionsText: JSON.stringify(editing.actions, null, 2),
  }
}

function RuleDialog({
  editing,
  onOpenChange,
  onDone,
}: {
  editing: RuleRecord | "new" | null
  onOpenChange: (open: boolean) => void
  onDone: () => void
}) {
  if (!editing) {
    return null
  }
  const formKey = editing === "new" ? "new" : String(editing.id)
  return <RuleForm key={formKey} editing={editing} onOpenChange={onOpenChange} onDone={onDone} />
}

function RuleForm({
  editing,
  onOpenChange,
  onDone,
}: {
  editing: RuleRecord | "new"
  onOpenChange: (open: boolean) => void
  onDone: () => void
}) {
  const { t } = useI18n()
  const [form, setForm] = useState<RuleInput>(() => ruleInput(editing))
  const [invalid, setInvalid] = useState(false)
  const sourceId = editing === "new" ? 0 : editing.id

  return (
    <Dialog open onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{editing === "new" ? t("action.create") : t("action.edit")}</DialogTitle>
          <DialogDescription>{t("rules.engine")}</DialogDescription>
        </DialogHeader>
        <FieldGroup>
          <Field data-invalid={invalid || undefined}>
            <FieldLabel htmlFor="rule-name">{t("rules.name")}</FieldLabel>
            <Input
              id="rule-name"
              value={form.name}
              aria-invalid={invalid || undefined}
              onChange={(event) => setForm({ ...form, name: event.target.value })}
            />
          </Field>
          <Field>
            <FieldLabel htmlFor="rule-filter">{t("rules.topicFilter")}</FieldLabel>
            <Input
              id="rule-filter"
              value={form.topic_filter}
              onChange={(event) => setForm({ ...form, topic_filter: event.target.value })}
            />
          </Field>
          <Field>
            <FieldLabel htmlFor="rule-priority">{t("rules.priority")}</FieldLabel>
            <Input
              id="rule-priority"
              type="number"
              value={form.priority}
              onChange={(event) => setForm({ ...form, priority: Number(event.target.value) })}
            />
          </Field>
          <Field>
            <FieldLabel htmlFor="rule-description">{t("rules.descriptionField")}</FieldLabel>
            <Input
              id="rule-description"
              value={form.description}
              onChange={(event) => setForm({ ...form, description: event.target.value })}
            />
          </Field>
          <Field data-invalid={invalid || undefined}>
            <FieldLabel htmlFor="rule-conditions">{t("rules.conditions")}</FieldLabel>
            <Textarea
              id="rule-conditions"
              value={form.conditionsText}
              aria-invalid={invalid || undefined}
              onChange={(event) => setForm({ ...form, conditionsText: event.target.value })}
            />
          </Field>
          <Field data-invalid={invalid || undefined}>
            <FieldLabel htmlFor="rule-actions">{t("rules.actions")}</FieldLabel>
            <Textarea
              id="rule-actions"
              value={form.actionsText}
              aria-invalid={invalid || undefined}
              onChange={(event) => setForm({ ...form, actionsText: event.target.value })}
            />
            {invalid ? <FieldError>{t("rules.invalidJson")}</FieldError> : <FieldDescription>{t("rules.engine")}</FieldDescription>}
          </Field>
          <Field orientation="horizontal">
            <Switch id="rule-enabled" checked={form.enabled} onCheckedChange={(checked) => setForm({ ...form, enabled: checked === true })} />
            <FieldLabel htmlFor="rule-enabled">{t("rules.enabled")}</FieldLabel>
          </Field>
        </FieldGroup>
        <DialogFooter>
          <Button
            onClick={() => {
              const save = editing === "new" ? createRule(form, preferenceRequest()) : updateRule(sourceId, form, preferenceRequest())
              void save
                .then(() => {
                  toast.add({ title: t("action.save"), type: "success" })
                  setInvalid(false)
                  onDone()
                })
                .catch((error: unknown) => {
                  setInvalid(true)
                  if (error instanceof ApiError) {
                    toast.add({ title: error.message, type: "error" })
                  }
                })
            }}
          >
            {t("action.save")}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
