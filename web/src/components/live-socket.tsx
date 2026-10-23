import { useEffect } from "react"

import { parseClientNotice, parseSnapshot, eventsUrl } from "@/lib/events"
import { translate } from "@/lib/i18n"
import { toast } from "@/components/ui/toast"
import { useLive } from "@/stores/live"
import { usePreferences } from "@/stores/preferences"

export function LiveSocket() {
  const apiBase = usePreferences((state) => state.apiBase)
  const token = usePreferences((state) => state.token)

  useEffect(() => {
    let socket: WebSocket | null = null
    let stopped = false
    let timer = 0

    const connect = () => {
      const preferences = usePreferences.getState()
      socket = new WebSocket(eventsUrl(window.location.href, preferences.apiBase, preferences.token))
      socket.addEventListener("open", () => useLive.getState().setConnected(true))
      socket.addEventListener("close", () => {
        useLive.getState().setConnected(false)
        if (!stopped) {
          timer = window.setTimeout(connect, 2000)
        }
      })
      socket.addEventListener("message", (event) => {
        if (typeof event.data !== "string") {
          return
        }
        let payload: unknown
        try {
          payload = JSON.parse(event.data)
        } catch {
          return
        }
        const snapshot = parseSnapshot(payload)
        if (snapshot) {
          useLive.getState().applySnapshot(snapshot)
          return
        }
        const notice = parseClientNotice(payload)
        if (!notice) {
          return
        }
        useLive.getState().pushNotice(notice)
        const locale = usePreferences.getState().locale
        const key = notice.action === "connected" ? "notices.connected" : "notices.disconnected"
        toast.add({
          title: translate(locale, key, { id: notice.client_id }),
          type: notice.action === "connected" ? "success" : "warning",
        })
      })
    }

    connect()
    return () => {
      stopped = true
      window.clearTimeout(timer)
      socket?.close()
    }
  }, [apiBase, token])

  return null
}
