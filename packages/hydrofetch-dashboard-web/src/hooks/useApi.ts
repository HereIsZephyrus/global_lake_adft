import useSWR, { type SWRConfiguration } from 'swr'
import { api, type ProjectConfig } from '../api/client'

// ---------------------------------------------------------------------------
// Global (non-project-scoped) hooks
// ---------------------------------------------------------------------------

export function useProjects() {
  return useSWR<ProjectConfig[]>('projects', api.projects, {
    refreshInterval: 60_000,
    revalidateOnFocus: false,
  })
}

export function useIngest() {
  return useSWR('ingest', api.ingest, {
    refreshInterval: 120_000,
    revalidateOnFocus: false,
  })
}

export function useDbSize() {
  return useSWR('dbSize', api.dbSize, {
    refreshInterval: 120_000,
    revalidateOnFocus: false,
  })
}

// ---------------------------------------------------------------------------
// Per-project hooks (projectId: null → suspended, no request sent)
// ---------------------------------------------------------------------------

function useProjectData<T>(
  key: string,
  projectId: string | null,
  fetcher: (pid: string) => Promise<T>,
  extra?: SWRConfiguration<T>,
) {
  return useSWR<T>(
    projectId ? [key, projectId] : null,
    () => fetcher(projectId!),
    { revalidateOnFocus: false, ...extra },
  )
}

export function useSummary(projectId: string | null) {
  return useProjectData('summary', projectId, api.summary, { refreshInterval: 120_000 })
}

export function useOverview(projectId: string | null) {
  return useProjectData('overview', projectId, api.overview, { refreshInterval: 60_000 })
}

export function useStates(projectId: string | null) {
  return useProjectData('states', projectId, api.states, { refreshInterval: 60_000 })
}

export function useTimeline(projectId: string | null, hours = 6) {
  return useProjectData(
    'timeline',
    projectId,
    (pid) => api.timeline(pid, hours),
    { refreshInterval: 120_000 },
  )
}

export function useFailures(projectId: string | null) {
  return useProjectData('failures', projectId, api.failures, { refreshInterval: 120_000 })
}

export function useAlerts(projectId: string | null) {
  return useProjectData('alerts', projectId, api.alerts, { refreshInterval: 120_000 })
}

export function useLogs(projectId: string | null) {
  return useProjectData('logs', projectId, api.logs, { refreshInterval: 120_000 })
}
