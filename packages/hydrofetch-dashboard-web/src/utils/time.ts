/** Display ingest timestamps in China standard time (no DST). */

const TZ = 'Asia/Shanghai'

function hasExplicitTimeZone(iso: string): boolean {
  return /[zZ]$/.test(iso) || /[+-]\d{2}(?::\d{2}){1,2}$/.test(iso)
}

function normalizePostgresOffset(iso: string): string {
  if (/[zZ]$/.test(iso)) return iso
  if (/[+-]\d{2}$/.test(iso)) return `${iso}:00`
  if (/[+-]\d{4}$/.test(iso)) return `${iso.slice(0, -2)}:${iso.slice(-2)}`
  return iso
}

function parsePostgresInstant(raw: string): Date | null {
  const t = raw.trim()
  if (!t) return null
  let iso = t.includes('T') ? t : t.replace(/^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})/, '$1T$2')
  iso = normalizePostgresOffset(iso)
  if (!hasExplicitTimeZone(iso)) iso += 'Z'
  const d = new Date(iso)
  return Number.isNaN(d.getTime()) ? null : d
}

/** Format a DB/API datetime string for UI in Asia/Shanghai. */
export function formatDateTimeShanghai(raw: string | null | undefined): string {
  if (raw == null || String(raw).trim() === '') return '—'
  const d = parsePostgresInstant(String(raw))
  if (!d) return String(raw).slice(0, 19)
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: TZ,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
    hourCycle: 'h23',
  }).formatToParts(d)
  const m: Record<string, string> = {}
  for (const p of parts) {
    if (p.type !== 'literal') m[p.type] = p.value
  }
  return `${m.year}-${m.month}-${m.day} ${m.hour}:${m.minute}:${m.second}`
}
