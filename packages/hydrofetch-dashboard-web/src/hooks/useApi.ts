import useSWR, { type SWRConfiguration } from 'swr'
import { api } from '../api/client'

type ApiKey = keyof typeof api

const INTERVALS: Record<string, number> = {
  overview: 10_000,
  states: 10_000,
  timeline: 20_000,
  tileProgress: 30_000,
  dateProgress: 30_000,
  failures: 15_000,
  jobs: 0,
  ingest: 60_000,
  alerts: 15_000,
  logs: 30_000,
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function useApiData<T>(key: ApiKey, fetcher: () => Promise<T>, extra?: SWRConfiguration<T>) {
  return useSWR<T>(key, fetcher, {
    refreshInterval: INTERVALS[key] ?? 15_000,
    revalidateOnFocus: false,
    ...extra,
  })
}

export function useOverview() {
  return useApiData('overview', api.overview)
}

export function useStates() {
  return useApiData('states', api.states)
}

export function useTimeline(hours = 6) {
  return useApiData('timeline', () => api.timeline(hours))
}

export function useFailures() {
  return useApiData('failures', api.failures)
}

export function useAlerts() {
  return useApiData('alerts', api.alerts)
}

export function useIngest() {
  return useApiData('ingest', api.ingest)
}

export function useLogs() {
  return useApiData('logs', api.logs)
}
