import type { ReactNode } from "react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "@/components/ui/empty"
import { Skeleton } from "@/components/ui/skeleton"
import { errorMessage } from "@/lib/use-load"
import { useI18n } from "@/lib/use-i18n"

export function PageHeader({ title, description, actions }: { title: string; description: string; actions?: ReactNode }) {
  return (
    <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
      <div className="flex flex-col gap-1">
        <h1 className="text-lg font-medium">{title}</h1>
        <p className="text-sm text-muted-foreground">{description}</p>
      </div>
      {actions}
    </div>
  )
}

export function QueryState({
  loading,
  error,
  empty,
  emptyTitle,
  children,
}: {
  loading: boolean
  error: Error | null
  empty?: boolean
  emptyTitle?: string
  children: ReactNode
}) {
  const { t } = useI18n()
  if (loading && !children) {
    return (
      <div className="flex flex-col gap-2" aria-busy="true">
        <Skeleton className="h-8 w-full" />
        <Skeleton className="h-8 w-full" />
        <Skeleton className="h-8 w-2/3" />
        <span className="sr-only">{t("status.loading")}</span>
      </div>
    )
  }
  if (error) {
    return (
      <Alert variant="destructive">
        <AlertTitle>{t("status.error")}</AlertTitle>
        <AlertDescription>{errorMessage(error, t("status.unauthorized"), t("status.error"))}</AlertDescription>
      </Alert>
    )
  }
  if (empty) {
    return (
      <Empty>
        <EmptyHeader>
          <EmptyTitle>{emptyTitle}</EmptyTitle>
          <EmptyDescription>{emptyTitle}</EmptyDescription>
        </EmptyHeader>
      </Empty>
    )
  }
  return children
}
