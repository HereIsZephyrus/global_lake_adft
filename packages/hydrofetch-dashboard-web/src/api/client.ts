const BASE = '/api'

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`)
  if (!res.ok) throw new Error(`API ${path} → ${res.status}`)
  return res.json() as Promise<T>
}

async function post<T = void>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    method: 'POST',
    headers: body !== undefined ? { 'Content-Type': 'application/json' } : {},
    body: body !== undefined ? JSON.stringify(body) : undefined,
  })
  if (!res.ok) {
    let detail = `API POST ${path} → ${res.status}`
    try {
      const json = await res.json()
      if (json?.detail) detail = String(json.detail)
    } catch { /* ignore */ }
    throw new Error(detail)
  }
  const text = await res.text()
  return (text ? JSON.parse(text) : undefined) as T
}

async function del<T = void>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`, { method: 'DELETE' })
  if (!res.ok) {
    let detail = `API DELETE ${path} → ${res.status}`
    try {
      const json = await res.json()
      if (json?.detail) detail = String(json.detail)
    } catch { /* ignore */ }
    throw new Error(detail)
  }
  const text = await res.text()
  return (text ? JSON.parse(text) : undefined) as T
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface ProjectConfig {
  project_id: string
  project_name: string
  gee_project: string
  credentials_file: string
  start_date: string
  end_date: string
  max_concurrent: number
  created_at: string
  status: 'running' | 'stopped' | 'finished'
  active_jobs: number
}

export type CreateProjectBody = Omit<ProjectConfig, 'project_id' | 'created_at' | 'status' | 'active_jobs'>

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
  project_name?: string
  process_status?: 'running' | 'stopped' | 'finished'
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
export interface TableSizeRow {
  table_name: string
  total_bytes: number
  total_pretty: string
  data_bytes: number
  data_pretty: string
  index_bytes: number
  index_pretty: string
}
export interface DBSizeStats {
  available: boolean
  message: string
  db_name: string
  db_size_bytes: number
  db_size_pretty: string
  tables: TableSizeRow[]
}

// ---------------------------------------------------------------------------
// API
// ---------------------------------------------------------------------------

export const api = {
  // Project management
  projects: () => get<ProjectConfig[]>('/projects'),
  createProject: (body: CreateProjectBody) => post<ProjectConfig>('/projects', body),
  deleteProject: (id: string) => del(`/projects/${id}`),
  startProject: (id: string) => post<{ status: string }>(`/projects/${id}/start`),
  stopProject: (id: string) => post<{ status: string }>(`/projects/${id}/stop`),

  // Per-project data endpoints
  overview: (pid: string) => get<KPIs>(`/projects/${pid}/overview`),
  states: (pid: string) => get<StateCount[]>(`/projects/${pid}/states`),
  timeline: (pid: string, hours = 6) => get<TimelinePoint[]>(`/projects/${pid}/timeline?hours=${hours}`),
  failures: (pid: string) => get<Failures>(`/projects/${pid}/failures`),
  jobs: (pid: string, params?: { state?: string; tile_id?: string; min_attempt?: number; limit?: number; offset?: number }) => {
    const q = new URLSearchParams()
    if (params?.state) q.set('state', params.state)
    if (params?.tile_id) q.set('tile_id', params.tile_id)
    if (params?.min_attempt) q.set('min_attempt', String(params.min_attempt))
    if (params?.limit) q.set('limit', String(params.limit))
    if (params?.offset) q.set('offset', String(params.offset))
    return get<JobsPage>(`/projects/${pid}/jobs${q.toString() ? '?' + q : ''}`)
  },
  alerts: (pid: string) => get<Alerts>(`/projects/${pid}/alerts`),
  logs: (pid: string) => get<LogsResult>(`/projects/${pid}/logs`),

  // Global endpoints (not per-project)
  ingest: () => get<IngestStats>('/ingest'),
  dbSize: () => get<DBSizeStats>('/db-size'),
  globalLogs: () => get<LogsResult>('/logs'),
  tileProgress: () => get<TileProgress[]>('/tile-progress'),
  dateProgress: () => get<{ date_iso: string; total: number; completed: number; failed: number; active: number }[]>('/date-progress'),
}
