import { db } from "../db.ts"
import { getConfig } from "../config.ts"
import type { JobRow } from "../../shared/types.ts"

export async function queryJobDetail(jobId: string): Promise<JobRow | null> {
  const { jobsTable } = getConfig()

  const rows = await db.unsafe<JobRow[]>(
    `SELECT id, job_type, status, attempt_count, max_attempts,
            payload, result, error_code, last_error,
            created_at, updated_at, started_at, finished_at,
            claimed_by_worker_id, claimed_by_hostname, claimed_by_pid,
            claimed_at, progress_heartbeat_at, timeout_at,
            max_run_seconds, timeout_strategy
     FROM ${jobsTable}
     WHERE id = $1`,
    [jobId],
  )

  return rows[0] ?? null
}
