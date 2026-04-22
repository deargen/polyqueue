import { db } from "../db.ts"
import { getConfig } from "../config.ts"
import type { JobRow, JobsFilter } from "../../shared/types.ts"

export async function queryJobs(filter: JobsFilter = {}): Promise<JobRow[]> {
  const { jobsTable, staleProgressSeconds } = getConfig()
  const { status, timedOut, staleProgress, limit = 50, q } = filter

  const conditions: string[] = []
  const params: unknown[] = []

  if (status) {
    params.push(status)
    conditions.push(`status = $${params.length}`)
  }

  if (timedOut) {
    conditions.push("status = 'processing' AND timeout_at IS NOT NULL AND timeout_at < now()")
  }

  if (staleProgress) {
    params.push(staleProgressSeconds)
    conditions.push(
      `status = 'processing' AND progress_heartbeat_at < now() - make_interval(secs => $${params.length})`,
    )
  }

  if (q) {
    params.push(`%${q}%`)
    const i = params.length
    conditions.push(`(id ILIKE $${i} OR job_type ILIKE $${i})`)
  }

  const where = conditions.length > 0 ? `WHERE ${conditions.map((c) => `(${c})`).join(" AND ")}` : ""

  params.push(limit)
  const limitClause = `LIMIT $${params.length}`

  return db.unsafe<JobRow[]>(
    `SELECT id, job_type, status, attempt_count, max_attempts,
            payload, result, error_code, last_error,
            created_at, updated_at, started_at, finished_at,
            claimed_by_worker_id, claimed_by_hostname, claimed_by_pid,
            claimed_at, progress_heartbeat_at, timeout_at,
            max_run_seconds, timeout_strategy
     FROM ${jobsTable}
     ${where}
     ORDER BY created_at DESC
     ${limitClause}`,
    params,
  )
}
