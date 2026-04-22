import type {
  AdminResult,
  JobAttemptEvent,
  JobRow,
  JobsFilter,
  QueueSnapshot,
  ServerConfigResponse,
  WorkerRow,
} from "../shared/types.ts"

export interface JobDetailResponse {
  job: JobRow
  attempts: JobAttemptEvent[]
}

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, init)
  if (!res.ok) {
    const body = (await res.json().catch(() => null)) as {
      message?: string
      error?: string
    } | null
    const msg =
      body?.message ?? body?.error ?? `${res.status} ${res.statusText}`
    throw new Error(msg)
  }
  return res.json() as Promise<T>
}

async function apiPost<T>(path: string): Promise<T> {
  return apiFetch<T>(path, { method: "POST" })
}

export function fetchSnapshot(): Promise<QueueSnapshot> {
  return apiFetch("/api/snapshot")
}

export function fetchWorkers(status?: string): Promise<WorkerRow[]> {
  const params = status ? `?status=${encodeURIComponent(status)}` : ""
  return apiFetch(`/api/workers${params}`)
}

export function fetchJobs(filter: JobsFilter = {}): Promise<JobRow[]> {
  const params = new URLSearchParams()
  if (filter.status) params.set("status", filter.status)
  if (filter.timedOut) params.set("timedOut", "true")
  if (filter.staleProgress) params.set("staleProgress", "true")
  if (filter.limit) params.set("limit", String(filter.limit))
  if (filter.q) params.set("q", filter.q)
  const qs = params.toString()
  return apiFetch(`/api/jobs${qs ? `?${qs}` : ""}`)
}

export function fetchJobDetail(jobId: string): Promise<JobDetailResponse> {
  return apiFetch(`/api/jobs/${encodeURIComponent(jobId)}`)
}

export function postRequeue(jobId: string): Promise<AdminResult> {
  return apiPost(`/api/jobs/${encodeURIComponent(jobId)}/requeue`)
}

export function postFail(jobId: string): Promise<AdminResult> {
  return apiPost(`/api/jobs/${encodeURIComponent(jobId)}/fail`)
}

export function fetchConfig(): Promise<ServerConfigResponse> {
  return apiFetch("/api/config")
}
