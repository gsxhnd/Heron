import { usePreferences } from "@/stores/preferences"
import { translate, type MessageKey } from "@/lib/i18n"

export function useI18n() {
  const locale = usePreferences((state) => state.locale)
  const t = (key: MessageKey, values?: Record<string, string | number>) =>
    translate(locale, key, values)
  return { locale, t }
}
