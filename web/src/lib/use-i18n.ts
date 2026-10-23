import { useTranslation } from "react-i18next"

import type { Locale, MessageKey } from "@/lib/i18n"

export function useI18n() {
  const { t, i18n } = useTranslation()
  const locale: Locale = i18n.resolvedLanguage === "en-US" ? "en-US" : "zh-CN"
  return {
    locale,
    t: (key: MessageKey, values?: Record<string, string | number>) => t(key, values),
  }
}
