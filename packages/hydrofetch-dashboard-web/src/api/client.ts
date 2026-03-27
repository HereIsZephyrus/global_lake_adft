const BASE = '/api'

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`)
  if (!res.ok) throw new Error(`API ${path} → ${res.status}`)
  return res.json() as Promise<T>
}

export interface KPIs {
  total_jobs: number
  active_jobs: number
  completed_jobs: number
  failed_jobs: number
  stalled_jobs: number
  completion_rate: number
  failure_rate: number
  job_dir: string
  log_dir: string
  db_table: string
}

export interface StateCount { state: string; count: number }
export interface TimelinePoint { hour: string; state: string; count: number }
export interface TileProgress {
  tile_id: string | null
  total: number
  completed: number
  failed: number
  active: number
  pending: number
}
export interface JobRow {
  job_id: string | null
  state: string
  tile_id: string | null
  date_iso: string | null
  attempt: number
  task_id: string | null
  drive_file_id: string | null
  local_raw_path: string | null
  local_sample_path: string | null
  updated_at: string | null
  last_error: string | null
}
export interface JobsPage { items: JobRow[]; total: number; limit: number; offset: number }
export interface FailureRow {
  job_id: string | null
  tile_id: string | null
  date_iso: string | null
  attempt: number
  last_error: string | null
  error_group: string
  updated_at: string | null
  task_id: string | null
}
export interface FailureSummary { error_group: string; count: number }
export interface Failures { items: FailureRow[]; summary: FailureSummary[] }
export interface DailyCount { date: string; row_count: number }
export interface IngestStats {
  available: boolean
  message: string
  table_name: string
  total_rows: number
  min_date: string | null
  max_date: string | null
  latest_ingested_at: string | null
  daily_counts: DailyCount[]
  recent_rows: { hylak_id: string | null; date: string | null; ingested_at: string | null }[]
}
export interface AlertStalled { job_id: string | null; state: string; tile_id: string | null; date_iso: string | null; updated_at: string | null; updated_age_hours: number }
export interface AlertRetry { job_id: string | null; state: string; tile_id: string | null; date_iso: string | null; attempt: number; updated_at: string | null }
export interface Alerts { stalled: AlertStalled[]; high_retry: AlertRetry[]; write_waiting: object[] }
export interface LogEntry { timestamp: string; level: string; logger: string; message: string; log_file: string }
export interface LogsResult { errors: LogEntry[]; warnings: LogEntry[]; writes: LogEntry[] }

export const api = {
  overview: () => get<KPIs>('/overview'),
  states: () => get<StateCount[]>('/states'),
  timeline: (hours = 6) => get<TimelinePoint[]>(`/timeline?hours=${hours}`),
  tileProgress: () => get<TileProgress[]>('/tile-progress'),
  dateProgress: () => get<{ date_iso: string; total: number; completed: number; failed: number; active: number }[]>('/date-progress'),
  failures: () => get<Failures>('/failures'),
  jobs: (params?: { state?: string; tile_id?: string; min_attempt?: number; limit?: number; offset?: number }) => {
    const q = new URLSearchParams()
    if (params?.state) q.set('state', params.state)
    if (params?.tile_id) q.set('tile_id', params.tile_id)
    if (params?.min_attempt) q.set('min_attempt', String(params.min_attempt))
    if (params?.limit) q.set('limit', String(params.limit))
    if (params?.offset) q.set('offset', String(params.offset))
    return get<JobsPage>(`/jobs${q.toString() ? '?' + q : ''}`)
  },
  ingest: () => get<IngestStats>('/ingest'),
  alerts: () => get<Alerts>('/alerts'),
  logs: () => get<LogsResult>('/logs'),
}
