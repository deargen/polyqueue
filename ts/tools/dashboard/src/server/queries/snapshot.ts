import { db } from "../db.ts"
import { getConfig } from "../config.ts"
import type { QueueSnapshot } from "../../shared/types.ts"

export async function querySnapshot(): Promise<QueueSnapshot> {
  const { jobsTable, workersTable, staleProgressSeconds, workerStaleSeconds } = getConfig()

  const rows = await db.unsafe<QueueSnapshot[]>(
    `SELECT
      (SELECT COALESCE(count(*), 0)::int FROM ${jobsTable} WHERE status = 'queued') AS queued_jobs,
      (SELECT COALESCE(count(*), 0)::int FROM ${jobsTable} WHERE status = 'processing') AS processing_jobs,
      (SELECT COALESCE(count(*), 0)::int FROM ${jobsTable} WHERE status = 'succeeded') AS succeeded_jobs,
      (SELECT COALESCE(count(*), 0)::int FROM ${jobsTable} WHERE status = 'failed') AS failed_jobs,
      (SELECT EXTRACT(epoch FROM now() - MIN(created_at)) FROM ${jobsTable} WHERE status = 'queued') AS oldest_queued_age_seconds,
      (SELECT COALESCE(count(*), 0)::int FROM ${jobsTable}
        WHERE status = 'processing' AND timeout_at IS NOT NULL AND timeout_at < now()) AS timed_out_processing_jobs,
      (SELECT COALESCE(count(*), 0)::int FROM ${jobsTable}
        WHERE status = 'processing' AND progress_heartbeat_at < now() - make_interval(secs => $1)) AS stale_progress_jobs,
      (SELECT COALESCE(count(*), 0)::int FROM ${workersTable}
        WHERE status = 'running' AND last_heartbeat_at >= now() - make_interval(secs => $2)) AS healthy_workers,
      (SELECT COALESCE(count(*), 0)::int FROM ${workersTable} WHERE status = 'running') AS running_workers`,
    [staleProgressSeconds, workerStaleSeconds],
  )

  if (!rows[0]) throw new Error("Snapshot query returned no rows")
  return rows[0]
}
