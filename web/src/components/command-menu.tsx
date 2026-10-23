import { useEffect, useState } from "react"
import { useNavigate } from "@tanstack/react-router"

import {
  Command,
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command"
import type { MessageKey } from "@/lib/i18n"
import { useI18n } from "@/lib/use-i18n"

const destinations: { to: "/" | "/clients" | "/topics" | "/subscriptions" | "/rules" | "/settings"; key: MessageKey }[] = [
  { to: "/", key: "nav.dashboard" },
  { to: "/clients", key: "nav.clients" },
  { to: "/topics", key: "nav.topics" },
  { to: "/subscriptions", key: "nav.subscriptions" },
  { to: "/rules", key: "nav.rules" },
  { to: "/settings", key: "nav.settings" },
]

export function CommandMenu() {
  const { t } = useI18n()
  const navigate = useNavigate()
  const [open, setOpen] = useState(false)

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault()
        setOpen((value) => !value)
      }
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "r") {
        event.preventDefault()
        window.dispatchEvent(new Event("heron:refresh"))
      }
    }
    window.addEventListener("keydown", onKey)
    return () => window.removeEventListener("keydown", onKey)
  }, [])

  return (
    <CommandDialog
      open={open}
      onOpenChange={setOpen}
      title={t("action.search")}
      description={t("command.placeholder")}
    >
      <Command>
        <CommandInput placeholder={t("command.placeholder")} />
        <CommandList>
          <CommandEmpty>{t("command.empty")}</CommandEmpty>
          <CommandGroup>
            {destinations.map((item) => (
              <CommandItem
                key={item.to}
                value={t(item.key)}
                onSelect={() => {
                  setOpen(false)
                  void navigate({ to: item.to })
                }}
              >
                {t(item.key)}
              </CommandItem>
            ))}
          </CommandGroup>
        </CommandList>
      </Command>
    </CommandDialog>
  )
}
