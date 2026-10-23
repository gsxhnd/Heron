import { StrictMode } from "react"
import { createRoot } from "react-dom/client"

import { ThemeProvider } from "@/components/theme-provider.tsx"
import { Toaster } from "@/components/ui/toast"
import { TooltipProvider } from "@/components/ui/tooltip"
import i18n, { type Locale } from "@/lib/i18n"
import { AppRouter } from "@/router.tsx"
import { usePreferences } from "@/stores/preferences"

import "./index.css"

function applyLocale(locale: Locale) {
  if (i18n.resolvedLanguage !== locale) {
    void i18n.changeLanguage(locale)
  }
}

usePreferences.subscribe((state) => applyLocale(state.locale))
usePreferences.persist.onFinishHydration((state) => applyLocale(state.locale))

const root = document.getElementById("root")
if (!root) {
  throw new Error("root element is missing")
}

createRoot(root).render(
  <StrictMode>
    <ThemeProvider>
      <TooltipProvider>
        <Toaster>
          <AppRouter />
        </Toaster>
      </TooltipProvider>
    </ThemeProvider>
  </StrictMode>,
)
