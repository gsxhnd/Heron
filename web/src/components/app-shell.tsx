import { useEffect } from "react"
import { Outlet } from "@tanstack/react-router"
import { RefreshCwIcon, SearchIcon } from "lucide-react"

import { AppSidebar } from "@/components/app-sidebar"
import { CommandMenu } from "@/components/command-menu"
import { LiveSocket } from "@/components/live-socket"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Separator } from "@/components/ui/separator"
import { SidebarInset, SidebarProvider, SidebarTrigger } from "@/components/ui/sidebar"
import { useI18n } from "@/lib/use-i18n"
import { useLive } from "@/stores/live"

export function AppShell() {
  const { locale, t } = useI18n()
  const connected = useLive((state) => state.connected)

  useEffect(() => {
    document.documentElement.lang = locale
  }, [locale])

  return (
    <SidebarProvider>
      <LiveSocket />
      <CommandMenu />
      <AppSidebar />
      <SidebarInset>
        <header className="flex h-12 items-center gap-2 border-b px-4">
          <SidebarTrigger />
          <Separator orientation="vertical" className="h-4" />
          <div className="ml-auto flex items-center gap-2">
            <Badge variant={connected ? "secondary" : "outline"}>
              {connected ? t("dashboard.live") : t("dashboard.offline")}
            </Badge>
            <Button
              variant="outline"
              size="sm"
              onClick={() => window.dispatchEvent(new Event("heron:refresh"))}
            >
              <RefreshCwIcon data-icon="inline-start" />
              {t("action.refresh")}
            </Button>
            <Button variant="outline" size="sm" onClick={() => window.dispatchEvent(new KeyboardEvent("keydown", { key: "k", ctrlKey: true }))}>
              <SearchIcon data-icon="inline-start" />
              {t("action.search")}
            </Button>
          </div>
        </header>
        <div className="flex flex-1 flex-col gap-4 p-4">
          <Outlet />
        </div>
      </SidebarInset>
    </SidebarProvider>
  )
}
