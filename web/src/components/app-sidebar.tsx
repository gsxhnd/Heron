import { Link, useRouterState } from "@tanstack/react-router"
import {
  LayoutDashboardIcon,
  RadioIcon,
  ScrollTextIcon,
  SettingsIcon,
  UsersIcon,
  WaypointsIcon,
} from "lucide-react"

import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarRail,
} from "@/components/ui/sidebar"
import type { MessageKey } from "@/lib/i18n"
import { useI18n } from "@/lib/use-i18n"

const items: { to: "/" | "/clients" | "/topics" | "/subscriptions" | "/rules" | "/settings"; key: MessageKey; icon: typeof UsersIcon }[] = [
  { to: "/", key: "nav.dashboard", icon: LayoutDashboardIcon },
  { to: "/clients", key: "nav.clients", icon: UsersIcon },
  { to: "/topics", key: "nav.topics", icon: WaypointsIcon },
  { to: "/subscriptions", key: "nav.subscriptions", icon: RadioIcon },
  { to: "/rules", key: "nav.rules", icon: ScrollTextIcon },
  { to: "/settings", key: "nav.settings", icon: SettingsIcon },
]

export function AppSidebar() {
  const { t } = useI18n()
  const pathname = useRouterState({ select: (state) => state.location.pathname })

  return (
    <Sidebar collapsible="offcanvas">
      <SidebarHeader>
        <div className="px-2 py-1 text-sm font-medium">{t("app.name")}</div>
      </SidebarHeader>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupLabel>{t("app.name")}</SidebarGroupLabel>
          <SidebarGroupContent>
            <SidebarMenu>
              {items.map((item) => {
                const Icon = item.icon
                const active = item.to === "/" ? pathname === "/" : pathname.startsWith(item.to)
                return (
                  <SidebarMenuItem key={item.to}>
                    <SidebarMenuButton render={<Link to={item.to} />} isActive={active}>
                      <Icon />
                      <span>{t(item.key)}</span>
                    </SidebarMenuButton>
                  </SidebarMenuItem>
                )
              })}
            </SidebarMenu>
          </SidebarGroupContent>
        </SidebarGroup>
      </SidebarContent>
      <SidebarFooter />
      <SidebarRail />
    </Sidebar>
  )
}
