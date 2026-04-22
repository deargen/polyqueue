export interface QueueSnapshot {
  queued_jobs: number
  processing_jobs: number
  succeeded_jobs: number
  failed_jobs: number
  oldest_queued_age_seconds: number | null
  timed_out_processing_jobs: number
  stale_progress_jobs: number
  healthy_workers: number
  running_workers: number
}

export interface WorkerRow {
  worker_id: string
  hostname: string
  pid: number
  started_at: string
  last_heartbeat_at: string
  backend: string
  worker_name: string
  status: string
  current_job_id: string | null
  current_job_started_at: string | null
  version: string | null
  metadata: Record<string, unknown> | null
}

export interface JobRow {
  id: string
  job_type: string
  status: string
  attempt_count: number
  max_attempts: number
  payload: Record<string, unknown>
  result: unknown | null
  error_code: string | null
  last_error: string | null
  created_at: string
  updated_at: string
  started_at: string | null
  finished_at: string | null
  claimed_by_worker_id: string | null
  claimed_by_hostname: string | null
  claimed_by_pid: number | null
  claimed_at: string | null
  progress_heartbeat_at: string | null
  timeout_at: string | null
  max_run_seconds: number | null
  timeout_strategy: string | null
}

export interface JobAttemptEvent {
  event_id: string
  job_id: string
  attempt_number: number
  event_type: "claimed" | "succeeded" | "failed" | "abandoned"
  event_at: string
  worker_id: string | null
  worker_hostname: string | null
  worker_pid: number | null
  lease_token: string | null
  finalized_by: "worker" | "reaper" | "admin" | null
  duration_ms: number | null
  error_code: string | null
  error_message: string | null
  stack_trace: string | null
  job_type: string | null
  queue_name: string | null
}

export interface JobsFilter {
  status?: string
  timedOut?: boolean
  staleProgress?: boolean
  limit?: number
  q?: string
}

export interface AdminResult {
  ok: boolean
  message: string
}

export interface ServerConfigResponse {
  stale_progress_seconds: number
  worker_stale_seconds: number
}
