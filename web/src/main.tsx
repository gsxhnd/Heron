import { StrictMode } from "react"
import { createRoot } from "react-dom/client"

import { ThemeProvider } from "@/components/theme-provider.tsx"
import { Toaster } from "@/components/ui/toast"
import { TooltipProvider } from "@/components/ui/tooltip"
import { AppRouter } from "@/router.tsx"

import "./index.css"

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
