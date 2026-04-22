import { getConfig } from "./config.ts"
import { db } from "./db.ts"

/**
 * Creates the polyqueue_jobs, polyqueue_workers, and polyqueue_jobs_attempts
 * tables + indexes if they do not exist. Idempotent — safe to call on every
 * startup.
 *
 * Mirrors the Python `ensure_schema()` from polyqueue.schema.
 */
export async function ensureSchema(): Promise<void> {
  const { jobsTable, workersTable, attemptsTable } = getConfig()

  // Strip quotes for index names (e.g. "polyqueue"."polyqueue_jobs" → polyqueue_polyqueue_jobs)
  const jobsIndexBase = jobsTable.replace(/"/g, "").replace(/\./g, "_")
  const workersIndexBase = workersTable.replace(/"/g, "").replace(/\./g, "_")
  const attemptsIndexBase = attemptsTable.replace(/"/g, "").replace(/\./g, "_")

  // Ensure the schema itself exists (tables use a custom schema like "polyqueue").
  const schemaName = jobsTable.split(".")[0].replace(/"/g, "")
  await db.unsafe(`CREATE SCHEMA IF NOT EXISTS "${schemaName}"`)

  await db.unsafe(`
    CREATE TABLE IF NOT EXISTS ${jobsTable} (
        id            TEXT        PRIMARY KEY,
        job_type      TEXT        NOT NULL,
        status        TEXT        NOT NULL,
        payload       JSONB       NOT NULL,
        result        JSONB,
        error_code    TEXT,
        last_error    TEXT,
        attempt_count INT         NOT NULL DEFAULT 0,
        max_attempts  INT         NOT NULL DEFAULT 3,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
        started_at    TIMESTAMPTZ,
        finished_at   TIMESTAMPTZ,
        claimed_by_worker_id  TEXT,
        claimed_by_hostname   TEXT,
        claimed_by_pid        INT,
        claimed_at            TIMESTAMPTZ,
        lease_expires_at      TIMESTAMPTZ,
        lease_token           UUID,
        progress_heartbeat_at TIMESTAMPTZ,
        last_heartbeat_at     TIMESTAMPTZ,
        max_run_seconds       INT,
        timeout_strategy      TEXT,
        timeout_at            TIMESTAMPTZ
    )
  `)
  await db.unsafe(`ALTER TABLE ${jobsTable} ADD COLUMN IF NOT EXISTS lease_token UUID`)
  await db.unsafe(
    `ALTER TABLE ${jobsTable} ADD COLUMN IF NOT EXISTS last_heartbeat_at TIMESTAMPTZ`,
  )
  await db.unsafe(`
    CREATE INDEX IF NOT EXISTS idx_${jobsIndexBase}_status
        ON ${jobsTable} (status)
  `)
  await db.unsafe(`
    CREATE INDEX IF NOT EXISTS idx_${jobsIndexBase}_status_timeout
        ON ${jobsTable} (status, timeout_at)
        WHERE timeout_at IS NOT NULL
  `)
  await db.unsafe(`
    CREATE INDEX IF NOT EXISTS idx_${jobsIndexBase}_status_updated
        ON ${jobsTable} (status, updated_at)
  `)
  await db.unsafe(`
    CREATE INDEX IF NOT EXISTS idx_${jobsIndexBase}_processing_progress
        ON ${jobsTable} (progress_heartbeat_at)
        WHERE status = 'processing'
  `)
  await db.unsafe(`
    CREATE TABLE IF NOT EXISTS ${workersTable} (
        worker_id              TEXT        PRIMARY KEY,
        hostname               TEXT        NOT NULL,
        pid                    INT         NOT NULL,
        started_at             TIMESTAMPTZ NOT NULL,
        last_heartbeat_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
        backend                TEXT        NOT NULL DEFAULT '',
        worker_name            TEXT        NOT NULL DEFAULT '',
        status                 TEXT        NOT NULL DEFAULT 'running',
        current_job_id         TEXT,
        current_job_started_at TIMESTAMPTZ,
        version                TEXT,
        metadata               JSONB
    )
  `)
  await db.unsafe(`
    CREATE INDEX IF NOT EXISTS idx_${workersIndexBase}_status
        ON ${workersTable} (status)
  `)
  await db.unsafe(`
    CREATE INDEX IF NOT EXISTS idx_${workersIndexBase}_heartbeat
        ON ${workersTable} (last_heartbeat_at)
        WHERE status = 'running'
  `)

  // Append-only attempt event log.
  await db.unsafe(`
    CREATE TABLE IF NOT EXISTS ${attemptsTable} (
        event_id        BIGSERIAL   PRIMARY KEY,
        job_id          TEXT        NOT NULL,
        attempt_number  INT         NOT NULL CHECK (attempt_number > 0),
        event_type      TEXT        NOT NULL CHECK (event_type IN
                            ('claimed','succeeded','failed','abandoned')),
        event_at        TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        worker_id       TEXT,
        worker_hostname TEXT,
        worker_pid      INT,
        lease_token     UUID,
        finalized_by    TEXT CHECK (finalized_by IN ('worker','reaper','admin')),
        duration_ms     BIGINT,
        error_code      TEXT,
        error_message   TEXT,
        stack_trace     TEXT,
        job_type        TEXT,
        queue_name      TEXT,
        metadata        JSONB
    )
  `)
  await db.unsafe(`
    CREATE INDEX IF NOT EXISTS idx_${attemptsIndexBase}_terminal
        ON ${attemptsTable} (job_id, attempt_number, event_at DESC, event_id DESC)
        WHERE event_type IN ('succeeded','failed','abandoned')
  `)
  await db.unsafe(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_${attemptsIndexBase}_claimed
        ON ${attemptsTable} (job_id, attempt_number)
        WHERE event_type = 'claimed'
  `)
  await db.unsafe(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_${attemptsIndexBase}_dedupe
        ON ${attemptsTable} (job_id, attempt_number, event_type)
        WHERE event_type IN ('succeeded','failed')
  `)
  await db.unsafe(`
    CREATE INDEX IF NOT EXISTS idx_${attemptsIndexBase}_worker_failures
        ON ${attemptsTable} (worker_id, event_at DESC)
        WHERE event_type IN ('failed','abandoned')
  `)
  await db.unsafe(`
    CREATE INDEX IF NOT EXISTS idx_${attemptsIndexBase}_type_failures
        ON ${attemptsTable} (job_type, event_at DESC)
        WHERE event_type IN ('failed','abandoned')
  `)
}
