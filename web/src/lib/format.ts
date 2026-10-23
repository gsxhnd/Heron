export function formatUptime(seconds: number): string {
  const whole = Math.max(0, Math.floor(seconds))
  const days = Math.floor(whole / 86_400)
  const hours = Math.floor((whole % 86_400) / 3_600)
  const minutes = Math.floor((whole % 3_600) / 60)
  const rest = whole % 60
  if (days > 0) {
    return `${days}d ${hours}h`
  }
  if (hours > 0) {
    return `${hours}h ${minutes}m`
  }
  return `${minutes}m ${rest}s`
}

export function formatRate(value: number): string {
  if (!Number.isFinite(value)) {
    return "0"
  }
  if (value >= 100) {
    return Math.round(value).toString()
  }
  return value.toFixed(1)
}

export function protocolLabel(level: number): string {
  if (level === 4) {
    return "3.1.1"
  }
  if (level === 5) {
    return "5.0"
  }
  return String(level)
}
