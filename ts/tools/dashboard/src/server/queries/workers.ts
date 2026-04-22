import { db } from "../db.ts"
import { getConfig } from "../config.ts"
import type { WorkerRow } from "../../shared/types.ts"

export async function queryWorkers(status?: string): Promise<WorkerRow[]> {
  const { workersTable } = getConfig()

  if (status) {
    return db.unsafe<WorkerRow[]>(
      `SELECT worker_id, hostname, pid, started_at, last_heartbeat_at,
              backend, worker_name, status, current_job_id, current_job_started_at,
              version, metadata
       FROM ${workersTable}
       WHERE status = $1
       ORDER BY last_heartbeat_at DESC`,
      [status],
    )
  }

  return db.unsafe<WorkerRow[]>(
    `SELECT worker_id, hostname, pid, started_at, last_heartbeat_at,
            backend, worker_name, status, current_job_id, current_job_started_at,
            version, metadata
     FROM ${workersTable}
     ORDER BY last_heartbeat_at DESC`,
  )
}
