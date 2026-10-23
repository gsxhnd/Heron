import { createRootRoute, createRoute, createRouter, RouterProvider } from "@tanstack/react-router"

import { AppShell } from "@/components/app-shell"
import { ClientPage } from "@/pages/client"
import { ClientsPage } from "@/pages/clients"
import { DashboardPage } from "@/pages/dashboard"
import { RulesPage } from "@/pages/rules"
import { SettingsPage } from "@/pages/settings"
import { SubscriptionsPage } from "@/pages/subscriptions"
import { TopicsPage } from "@/pages/topics"

const rootRoute = createRootRoute({ component: AppShell })

const dashboardRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  component: DashboardPage,
})

const clientsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/clients",
  component: ClientsPage,
})

const clientRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/clients/$clientId",
  component: ClientPage,
})

const topicsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/topics",
  component: TopicsPage,
})

const subscriptionsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/subscriptions",
  component: SubscriptionsPage,
})

const rulesRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/rules",
  component: RulesPage,
})

const settingsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/settings",
  component: SettingsPage,
})

const routeTree = rootRoute.addChildren([
  dashboardRoute,
  clientsRoute,
  clientRoute,
  topicsRoute,
  subscriptionsRoute,
  rulesRoute,
  settingsRoute,
])

export const router = createRouter({ routeTree })

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router
  }
}

export function AppRouter() {
  return <RouterProvider router={router} />
}
