import { useCallback, useEffect, useState } from "react"

import { ApiError } from "@/lib/api"

interface LoadState<T> {
  data: T | null
  error: Error | null
  loading: boolean
  reload: () => void
}

export function useLoad<T>(load: () => Promise<T>): LoadState<T> {
  const [data, setData] = useState<T | null>(null)
  const [error, setError] = useState<Error | null>(null)
  const [loading, setLoading] = useState(true)
  const [nonce, setNonce] = useState(0)

  useEffect(() => {
    let active = true
    load()
      .then((value) => {
        if (!active) {
          return
        }
        setData(value)
        setError(null)
      })
      .catch((caught: unknown) => {
        if (!active) {
          return
        }
        setError(caught instanceof Error ? caught : new Error("request failed"))
      })
      .finally(() => {
        if (active) {
          setLoading(false)
        }
      })
    return () => {
      active = false
    }
  }, [load, nonce])

  useEffect(() => {
    const onRefresh = () => setNonce((value) => value + 1)
    window.addEventListener("heron:refresh", onRefresh)
    return () => window.removeEventListener("heron:refresh", onRefresh)
  }, [])

  const reload = useCallback(() => {
    setLoading(true)
    setNonce((value) => value + 1)
  }, [])
  return { data, error, loading, reload }
}

export function errorMessage(error: Error | null, unauthorized: string, fallback: string): string {
  if (error instanceof ApiError && error.status === 401) {
    return unauthorized
  }
  if (error) {
    return error.message || fallback
  }
  return fallback
}
