import { create } from "zustand"
import { persist } from "zustand/middleware"

import type { Locale } from "../lib/i18n.ts"

interface PreferencesState {
  locale: Locale
  apiBase: string
  token: string
  setLocale: (locale: Locale) => void
  setApiBase: (apiBase: string) => void
  setToken: (token: string) => void
}

export const usePreferences = create<PreferencesState>()(
  persist(
    (set) => ({
      locale: "zh-CN",
      apiBase: "",
      token: "",
      setLocale: (locale) => set({ locale }),
      setApiBase: (apiBase) => set({ apiBase }),
      setToken: (token) => set({ token }),
    }),
    { name: "heron-preferences" },
  ),
)

export function preferenceRequest() {
  const { apiBase, token } = usePreferences.getState()
  return { apiBase, token }
}
