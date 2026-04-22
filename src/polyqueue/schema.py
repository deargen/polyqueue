"""Schema management — authoritative DDL for the polyqueue jobs table.

Usage::

    from polyqueue.schema import ensure_schema

    # With an engine + settings:
    await ensure_schema(engine, settings)

    # Or just get the SQL:
    ddl = jobs_table_ddl(settings.qualified_table())
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

    from polyqueue.config import PolyqueueSettings


def jobs_table_ddl(table: str) -> str:
    """Return the CREATE TABLE IF NOT EXISTS DDL for the jobs table.

    Args:
        table: Fully-qualified table reference, e.g. ``"public.polyqueue_jobs"``.
    """
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
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
);

ALTER TABLE {table} ADD COLUMN IF NOT EXISTS lease_token UUID;
ALTER TABLE {table} ADD COLUMN IF NOT EXISTS last_heartbeat_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_{table.replace(".", "_")}_status
    ON {table} (status);

CREATE INDEX IF NOT EXISTS idx_{table.replace(".", "_")}_status_timeout
    ON {table} (status, timeout_at)
    WHERE timeout_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_{table.replace(".", "_")}_status_updated
    ON {table} (status, updated_at);

CREATE INDEX IF NOT EXISTS idx_{table.replace(".", "_")}_processing_progress
    ON {table} (progress_heartbeat_at)
    WHERE status = 'processing';
"""


def workers_table_ddl(table: str) -> str:
    """Return the CREATE TABLE IF NOT EXISTS DDL for the workers registry table.

    Args:
        table: Fully-qualified table reference, e.g. ``"public.polyqueue_workers"``.
    """
    table_safe = table.replace(".", "_")
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
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
);

CREATE INDEX IF NOT EXISTS idx_{table_safe}_status ON {table} (status);
CREATE INDEX IF NOT EXISTS idx_{table_safe}_heartbeat ON {table} (last_heartbeat_at) WHERE status = 'running';
"""


def attempts_table_ddl(table: str) -> str:
    """Return the CREATE TABLE IF NOT EXISTS DDL for the attempts (audit log) table.

    Append-only per-attempt event log. Every state transition inserts one row;
    rows are never updated. No FK to the jobs table so audit history survives
    operator pruning.

    Args:
        table: Fully-qualified table reference, e.g. ``"public.polyqueue_jobs_attempts"``.
    """
    table_safe = table.replace(".", "_")
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
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
);

CREATE INDEX IF NOT EXISTS idx_{table_safe}_terminal
    ON {table} (job_id, attempt_number, event_at DESC, event_id DESC)
    WHERE event_type IN ('succeeded','failed','abandoned');

CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_safe}_claimed
    ON {table} (job_id, attempt_number)
    WHERE event_type = 'claimed';

CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_safe}_dedupe
    ON {table} (job_id, attempt_number, event_type)
    WHERE event_type IN ('succeeded','failed');

CREATE INDEX IF NOT EXISTS idx_{table_safe}_worker_failures
    ON {table} (worker_id, event_at DESC)
    WHERE event_type IN ('failed','abandoned');

CREATE INDEX IF NOT EXISTS idx_{table_safe}_type_failures
    ON {table} (job_type, event_at DESC)
    WHERE event_type IN ('failed','abandoned');
"""


async def ensure_schema(engine: AsyncEngine, settings: PolyqueueSettings) -> None:
    """Create the jobs, workers, and attempts tables and indexes if they do not exist.

    Idempotent — safe to call on every startup. Includes additive ALTER TABLE
    statements for columns added after initial release (lease_token,
    last_heartbeat_at) so existing deployments upgrade cleanly.

    Each DDL statement is executed individually because asyncpg rejects
    multi-statement strings passed to a prepared-statement cursor.
    """
    ddl = f"CREATE SCHEMA IF NOT EXISTS {settings.table_schema};\n"
    ddl += jobs_table_ddl(settings.qualified_table())
    ddl += workers_table_ddl(settings.qualified_workers_table())
    ddl += attempts_table_ddl(settings.qualified_attempts_table())
    statements = [s.strip() for s in ddl.split(";") if s.strip()]
    async with engine.begin() as conn:
        for stmt in statements:
            _ = await conn.execute(text(stmt))
