import { db } from "../db.ts"
import { getConfig } from "../config.ts"
import type { JobAttemptEvent } from "../../shared/types.ts"

/** Return the full attempt event log for a job, oldest event first. */
export async function queryJobAttempts(jobId: string): Promise<JobAttemptEvent[]> {
  const { attemptsTable } = getConfig()

  return await db.unsafe<JobAttemptEvent[]>(
    `SELECT event_id::text, job_id, attempt_number, event_type, event_at,
            worker_id, worker_hostname, worker_pid, lease_token::text AS lease_token,
            finalized_by, duration_ms, error_code, error_message, stack_trace,
            job_type, queue_name
     FROM ${attemptsTable}
     WHERE job_id = $1
     ORDER BY attempt_number ASC, event_at ASC, event_id ASC`,
    [jobId],
  )
}
