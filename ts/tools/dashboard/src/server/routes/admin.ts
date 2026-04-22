import type { AdminResult } from "../../shared/types.ts"
import { getConfig } from "../config.ts"
import { db } from "../db.ts"

function adminJson(data: AdminResult, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
    },
  })
}

/**
 * Admin transitions match the race-safe pattern in `polyqueue.queue.db`:
 *
 * - Read the current `lease_token` + worker identity from the row.
 * - UPDATE predicated on `(status, lease_token)`; if another claim rotated
 *   the token between read and update, return 409.
 * - INSERT a matching attempt event with `finalized_by='admin'`.
 *
 * UPDATE + INSERT run in a single `db.begin(...)` transaction so an INSERT
 * failure rolls back the state transition. Worker identity fields from the
 * pre-update row are copied into the event for forensic parity with the
 * Python worker/reaper paths.
 */

interface ProcessingRow {
  lease_token: string | null
  attempt_count: number
  job_type: string
  claimed_by_worker_id: string | null
  claimed_by_hostname: string | null
  claimed_by_pid: number | null
  claimed_at: string | null
}

export async function handleAdminRequeue(
  _req: Request,
  jobId: string,
): Promise<Response> {
  const { jobsTable, attemptsTable } = getConfig()

  // Path 1: processing → queued (with audit event).
  let outcome: "requeued-processing" | "requeued-failed" | "conflict" | "none" =
    "none"

  await db.begin(async (sql) => {
    const processingRow = await sql.unsafe<ProcessingRow[]>(
      `SELECT lease_token::text AS lease_token,
              attempt_count, job_type,
              claimed_by_worker_id, claimed_by_hostname, claimed_by_pid,
              claimed_at
       FROM ${jobsTable}
       WHERE id = $1 AND status = 'processing'`,
      [jobId],
    )

    if (processingRow.length > 0 && processingRow[0].lease_token) {
      const row = processingRow[0]
      const token = row.lease_token as string
      const updated = await sql.unsafe<Array<{ id: string }>>(
        `UPDATE ${jobsTable}
         SET status           = 'queued',
             error_code       = NULL,
             last_error       = NULL,
             lease_expires_at = NULL,
             lease_token      = NULL,
             timeout_at       = NULL,
             updated_at       = now()
         WHERE id = $1
           AND status = 'processing'
           AND lease_token = $2::uuid
         RETURNING id`,
        [jobId, token],
      )
      if (updated.length === 1) {
        await sql.unsafe(
          `INSERT INTO ${attemptsTable}
             (job_id, attempt_number, event_type, finalized_by,
              worker_id, worker_hostname, worker_pid,
              lease_token, duration_ms, job_type,
              error_code, error_message)
           VALUES ($1, $2, 'abandoned', 'admin',
                   $3, $4, $5,
                   $6::uuid,
                   CAST(EXTRACT(EPOCH FROM (clock_timestamp() - $7::timestamptz)) * 1000 AS BIGINT),
                   $8, 'admin_requeue', 'Requeued via dashboard')`,
          [
            jobId,
            row.attempt_count,
            row.claimed_by_worker_id,
            row.claimed_by_hostname,
            row.claimed_by_pid,
            token,
            row.claimed_at,
            row.job_type,
          ],
        )
        outcome = "requeued-processing"
        return
      }
      outcome = "conflict"
      return
    }

    // Path 2: failed → queued (no audit event — no attempt was in flight).
    const fromFailed = await sql.unsafe<Array<{ id: string }>>(
      `UPDATE ${jobsTable}
       SET status     = 'queued',
           error_code = NULL,
           last_error = NULL,
           updated_at = now()
       WHERE id = $1 AND status = 'failed'
       RETURNING id`,
      [jobId],
    )
    outcome = fromFailed.length === 1 ? "requeued-failed" : "none"
  })

  if (outcome === "requeued-processing") {
    return adminJson({ ok: true, message: "Job requeued (was processing)" })
  }
  if (outcome === "requeued-failed") {
    return adminJson({ ok: true, message: "Job requeued (was failed)" })
  }
  if (outcome === "conflict") {
    return adminJson(
      { ok: false, message: "Lost race with worker/reaper; retry" },
      409,
    )
  }
  return adminJson(
    {
      ok: false,
      message:
        "Job not found or not in a requeueable state (must be failed or processing)",
    },
    409,
  )
}

export async function handleAdminFail(
  _req: Request,
  jobId: string,
): Promise<Response> {
  const { jobsTable, attemptsTable } = getConfig()

  let outcome: "failed" | "not-found" | "conflict" = "not-found"

  await db.begin(async (sql) => {
    const rows = await sql.unsafe<ProcessingRow[]>(
      `SELECT lease_token::text AS lease_token,
              attempt_count, job_type,
              claimed_by_worker_id, claimed_by_hostname, claimed_by_pid,
              claimed_at
       FROM ${jobsTable}
       WHERE id = $1 AND status = 'processing'`,
      [jobId],
    )
    if (rows.length === 0 || !rows[0].lease_token) {
      return
    }
    const row = rows[0]
    const token = row.lease_token as string

    const updated = await sql.unsafe<Array<{ id: string }>>(
      `UPDATE ${jobsTable}
       SET status           = 'failed',
           error_code       = 'admin_forced_fail',
           last_error       = 'Manually failed via dashboard',
           finished_at      = now(),
           updated_at       = now(),
           lease_expires_at = NULL,
           lease_token      = NULL,
           timeout_at       = NULL
       WHERE id = $1
         AND status = 'processing'
         AND lease_token = $2::uuid
       RETURNING id`,
      [jobId, token],
    )
    if (updated.length !== 1) {
      outcome = "conflict"
      return
    }

    await sql.unsafe(
      `INSERT INTO ${attemptsTable}
         (job_id, attempt_number, event_type, finalized_by,
          worker_id, worker_hostname, worker_pid,
          lease_token, duration_ms, job_type,
          error_code, error_message)
       VALUES ($1, $2, 'failed', 'admin',
               $3, $4, $5,
               $6::uuid,
               CAST(EXTRACT(EPOCH FROM (clock_timestamp() - $7::timestamptz)) * 1000 AS BIGINT),
               $8, 'admin_forced_fail', 'Manually failed via dashboard')`,
      [
        jobId,
        row.attempt_count,
        row.claimed_by_worker_id,
        row.claimed_by_hostname,
        row.claimed_by_pid,
        token,
        row.claimed_at,
        row.job_type,
      ],
    )
    outcome = "failed"
  })

  if (outcome === "failed") {
    return adminJson({ ok: true, message: "Job marked failed" })
  }
  if (outcome === "conflict") {
    return adminJson(
      { ok: false, message: "Lost race with worker/reaper; retry" },
      409,
    )
  }
  return adminJson(
    { ok: false, message: "Job not found or not processing" },
    409,
  )
}
