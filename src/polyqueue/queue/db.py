"""Shared Postgres state-transition helpers for all queue backend adapters.

All functions operate within a single session (passed in), so the caller controls
transaction boundaries. Every function is intentionally narrow — one concern each —
so backends can compose them without duplicating raw SQL.

Every function accepts ``table`` and ``attempts_table`` keyword arguments —
fully-qualified table references such as ``"public.polyqueue_jobs"`` — so the
table names and schema are configurable per queue adapter instance.

State-transition functions predicate their UPDATE on both ``status`` and
``lease_token``. A worker holds the ``lease_token`` it received from
``claim_job()``; if the reaper rotates the token (``reap_abandoned_attempt``),
the worker's belated terminal UPDATE will match zero rows and return False.
This is how the worker-vs-reaper race is resolved without cross-table locks.

Every terminal transition (succeeded/failed/abandoned) also INSERTs one row
into the attempts table inside the same transaction. The attempts table is
append-only; rows are never updated.

Settlement order invariant (enforced by callers):
    DB state must be committed *before* the broker message is settled (ack/delete/
    complete). This ensures Postgres is the authoritative record. If the broker
    settle subsequently fails, the claim() guard (``WHERE status = 'queued'``) will
    skip the redelivered message and mark it complete/deleted rather than
    double-processing.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import (
    datetime,  # noqa: TC003 — needed by Pydantic for ClaimRecord field resolution
)
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel
from sqlalchemy import func, select, text

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession

from polyqueue.tables import make_jobs_table, parse_qualified

logger = logging.getLogger(__name__)


class TimedOutJob(BaseModel):
    """Typed result returned by find_timed_out_jobs()."""

    id: str
    timeout_strategy: str | None
    attempt_count: int
    max_attempts: int
    lease_token: uuid.UUID | None = None


class ClaimRecord(BaseModel):
    """Typed result returned by claim_job()."""

    job_type: str
    payload: dict[str, Any]
    attempt_count: int
    max_attempts: int
    claimed_by_worker_id: str | None
    claimed_by_hostname: str | None
    claimed_by_pid: int | None
    claimed_at: datetime | None
    lease_token: uuid.UUID
    max_run_seconds: int | None
    timeout_strategy: str
    timeout_at: datetime | None
    created_at: datetime | None = None


_CLEAR_PROCESSING_META = """
    lease_expires_at      = NULL,
    lease_token           = NULL,
    timeout_at            = NULL
"""
# Note: claimed_by_*, claimed_at, progress_heartbeat_at, and last_heartbeat_at
# are deliberately NOT cleared. They preserve "last claim" forensic data for the
# dashboard and RETURNING clauses. "Currently claimed" is derived from
# status='processing', not from any claimed_* IS NOT NULL check.
# Only lease_token and lease_expires_at are cleared — they gate in-flight
# ownership, and clearing them blocks stale worker/reaper writes via the
# lease_token predicate on terminal UPDATEs.


async def _insert_attempt_event(
    session: AsyncSession,
    *,
    attempts_table: str | None = None,
    job_id: str,
    attempt_number: int,
    event_type: str,
    worker_id: str | None = None,
    worker_hostname: str | None = None,
    worker_pid: int | None = None,
    lease_token: UUID | None = None,
    finalized_by: str | None = None,
    claimed_at: datetime | None = None,
    error_code: str | None = None,
    error_message: str | None = None,
    stack_trace: str | None = None,
    job_type: str | None = None,
    queue_name: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Append one event row to the attempts log.

    Uses ``clock_timestamp()`` (not ``now()``) for ``event_at`` so multiple events
    inserted in one transaction get distinct timestamps. ``event_id`` gives a
    deterministic tiebreaker.

    ``duration_ms`` is computed inline from ``claimed_at`` when provided; NULL
    for ``claimed`` events.

    When ``attempts_table`` is None the insert is skipped entirely — useful for
    tests that only exercise the jobs-table state transition, and for callers
    that haven't yet been threaded to pass a qualified attempts table name.
    """
    if attempts_table is None:
        return
    duration_expr = (
        "CAST(EXTRACT(EPOCH FROM (clock_timestamp() - CAST(:claimed_at AS timestamptz))) * 1000 AS BIGINT)"
    )
    await session.execute(
        text(
            f"""
            INSERT INTO {attempts_table}
                (job_id, attempt_number, event_type, worker_id, worker_hostname,
                 worker_pid, lease_token, finalized_by, duration_ms,
                 error_code, error_message, stack_trace, job_type, queue_name, metadata)
            VALUES
                (:job_id, :attempt_number, :event_type, :worker_id, :worker_hostname,
                 :worker_pid, CAST(:lease_token AS UUID), :finalized_by, {duration_expr},
                 :error_code, :error_message, :stack_trace, :job_type, :queue_name,
                 CAST(:metadata AS jsonb))
            """
        ),
        {
            "job_id": job_id,
            "attempt_number": attempt_number,
            "event_type": event_type,
            "worker_id": worker_id,
            "worker_hostname": worker_hostname,
            "worker_pid": worker_pid,
            "lease_token": str(lease_token) if lease_token else None,
            "finalized_by": finalized_by,
            "claimed_at": claimed_at,
            "error_code": error_code,
            "error_message": error_message,
            "stack_trace": stack_trace,
            "job_type": job_type,
            "queue_name": queue_name,
            "metadata": json.dumps(metadata) if metadata else None,
        },
    )


async def insert_job(
    session: AsyncSession,
    *,
    table: str,
    job_id: str,
    job_type: str,
    payload: dict[str, Any],
    max_attempts: int,
    max_run_seconds: int | None = None,
    timeout_strategy: str | None = None,
) -> bool:
    """Insert a new job row with status='queued'.

    Uses an upsert that allows re-enqueue only if the row is in a terminal
    ``failed / enqueue_failed`` state (i.e. the previous broker-send failed).

    Returns:
        True if the row was inserted or re-enqueued, False if it already exists
        in a non-eligible state.
    """
    row = await session.execute(
        text(
            f"""
            INSERT INTO {table}
                (id, job_type, status, payload, attempt_count, max_attempts,
                 max_run_seconds, timeout_strategy, created_at, updated_at)
            VALUES
                (:id, :job_type, 'queued', CAST(:payload AS jsonb), 0, :max_attempts,
                 :max_run_seconds, :timeout_strategy, now(), now())
            ON CONFLICT (id) DO UPDATE
                SET status     = 'queued',
                    error_code = NULL,
                    last_error = NULL,
                    updated_at = now()
                WHERE {table}.status = 'failed' AND {table}.error_code = 'enqueue_failed'
            RETURNING id
            """
        ),
        {
            "id": job_id,
            "job_type": job_type,
            "payload": json.dumps(payload),
            "max_attempts": max_attempts,
            "max_run_seconds": max_run_seconds,
            "timeout_strategy": timeout_strategy,
        },
    )
    return row.first() is not None


async def claim_job(
    session: AsyncSession,
    *,
    table: str,
    attempts_table: str | None = None,
    job_id: str,
    worker_id: str = "",
    worker_hostname: str = "",
    worker_pid: int = 0,
    lease_expires_at: datetime | None = None,
    queue_name: str | None = None,
) -> ClaimRecord | None:
    """Atomically transition a queued job to processing and return its metadata.

    Increments attempt_count, sets started_at on first attempt, generates a
    fresh ``lease_token`` (returned so the caller can use it as a predicate on
    terminal UPDATEs), and appends a 'claimed' event to the attempts log.

    Returns:
        ClaimRecord with all job metadata including ``lease_token``, or None if
        the row is not in a claimable state.
    """
    lease_token = uuid.uuid4()
    row = await session.execute(
        text(
            f"""
            UPDATE {table}
            SET status        = 'processing',
                attempt_count = attempt_count + 1,
                started_at    = COALESCE(started_at, now()),
                updated_at    = now(),
                claimed_at    = now(),
                claimed_by_worker_id = :worker_id,
                claimed_by_hostname  = :worker_hostname,
                claimed_by_pid       = :worker_pid,
                progress_heartbeat_at = now(),
                last_heartbeat_at    = now(),
                lease_expires_at = :lease_expires_at,
                lease_token = CAST(:lease_token AS UUID),
                timeout_at = CASE
                    WHEN max_run_seconds IS NOT NULL
                    THEN now() + make_interval(secs => max_run_seconds)
                    ELSE NULL
                END
            WHERE id = :id
              AND status = 'queued'
            RETURNING job_type, payload, attempt_count, max_attempts,
                      claimed_by_worker_id, claimed_by_hostname, claimed_by_pid,
                      claimed_at, lease_token, max_run_seconds, timeout_strategy,
                      timeout_at, created_at
            """
        ),
        {
            "id": job_id,
            "worker_id": worker_id,
            "worker_hostname": worker_hostname,
            "worker_pid": worker_pid,
            "lease_expires_at": lease_expires_at,
            "lease_token": str(lease_token),
        },
    )
    mapping = row.mappings().one_or_none()
    if mapping is None:
        return None
    record = ClaimRecord.model_validate(dict(mapping))
    await _insert_attempt_event(
        session,
        attempts_table=attempts_table,
        job_id=job_id,
        attempt_number=record.attempt_count,
        event_type="claimed",
        worker_id=record.claimed_by_worker_id,
        worker_hostname=record.claimed_by_hostname,
        worker_pid=record.claimed_by_pid,
        lease_token=record.lease_token,
        job_type=record.job_type,
        queue_name=queue_name,
    )
    return record


async def mark_succeeded(
    session: AsyncSession,
    *,
    table: str,
    attempts_table: str | None = None,
    job_id: str,
    lease_token: UUID,
    queue_name: str | None = None,
) -> bool:
    """Transition processing→succeeded. Appends 'succeeded' event.

    The UPDATE predicates on both ``status='processing'`` and ``lease_token``.
    A belated call from a worker whose lease was rotated by the reaper will
    match zero rows and return False.

    Returns:
        True if the row was transitioned, False if no row matched (stale/rotated).
    """
    result = await session.execute(
        text(
            f"""
            UPDATE {table}
            SET status      = 'succeeded',
                finished_at = now(),
                updated_at  = now(),
                {_CLEAR_PROCESSING_META}
            WHERE id = :id
              AND status = 'processing'
              AND lease_token = CAST(:lease_token AS UUID)
            RETURNING attempt_count, claimed_at,
                      claimed_by_worker_id, claimed_by_hostname, claimed_by_pid,
                      job_type
            """
        ),
        {"id": job_id, "lease_token": str(lease_token)},
    )
    mapping = result.mappings().one_or_none()
    if mapping is None:
        return False
    await _insert_attempt_event(
        session,
        attempts_table=attempts_table,
        job_id=job_id,
        attempt_number=mapping["attempt_count"],
        event_type="succeeded",
        worker_id=mapping["claimed_by_worker_id"],
        worker_hostname=mapping["claimed_by_hostname"],
        worker_pid=mapping["claimed_by_pid"],
        lease_token=lease_token,
        finalized_by="worker",
        claimed_at=mapping["claimed_at"],
        job_type=mapping["job_type"],
        queue_name=queue_name,
    )
    return True


async def mark_succeeded_with_result(
    session: AsyncSession,
    *,
    table: str,
    attempts_table: str | None = None,
    job_id: str,
    lease_token: UUID,
    result: Any,
    queue_name: str | None = None,
) -> bool:
    """Transition processing→succeeded and store result. Appends 'succeeded' event.

    Args:
        result: JSON-serialisable value from normalize_result(). None → SQL NULL.

    Returns:
        True if the row was transitioned, False if no row matched.
    """
    result_json = json.dumps(result) if result is not None else None
    db_result = await session.execute(
        text(
            f"""
            UPDATE {table}
            SET status      = 'succeeded',
                result      = CAST(:result AS jsonb),
                finished_at = now(),
                updated_at  = now(),
                {_CLEAR_PROCESSING_META}
            WHERE id = :id
              AND status = 'processing'
              AND lease_token = CAST(:lease_token AS UUID)
            RETURNING attempt_count, claimed_at,
                      claimed_by_worker_id, claimed_by_hostname, claimed_by_pid,
                      job_type
            """
        ),
        {"id": job_id, "result": result_json, "lease_token": str(lease_token)},
    )
    mapping = db_result.mappings().one_or_none()
    if mapping is None:
        return False
    await _insert_attempt_event(
        session,
        attempts_table=attempts_table,
        job_id=job_id,
        attempt_number=mapping["attempt_count"],
        event_type="succeeded",
        worker_id=mapping["claimed_by_worker_id"],
        worker_hostname=mapping["claimed_by_hostname"],
        worker_pid=mapping["claimed_by_pid"],
        lease_token=lease_token,
        finalized_by="worker",
        claimed_at=mapping["claimed_at"],
        job_type=mapping["job_type"],
        queue_name=queue_name,
    )
    return True


async def mark_failed_terminal(
    session: AsyncSession,
    *,
    table: str,
    attempts_table: str | None = None,
    job_id: str,
    lease_token: UUID,
    error_code: str,
    last_error: str,
    finalized_by: str = "worker",
    stack_trace: str | None = None,
    queue_name: str | None = None,
) -> bool:
    """Transition processing→failed (terminal). Appends 'failed' or 'abandoned' event.

    ``finalized_by='worker'`` emits event_type='failed' (default).
    ``finalized_by='reaper'`` emits event_type='abandoned'.
    ``finalized_by='admin'`` emits event_type='failed'.

    Returns:
        True if the row was transitioned, False if no row matched.
    """
    result = await session.execute(
        text(
            f"""
            UPDATE {table}
            SET status      = 'failed',
                error_code  = :error_code,
                last_error  = :last_error,
                finished_at = now(),
                updated_at  = now(),
                {_CLEAR_PROCESSING_META}
            WHERE id = :id
              AND status = 'processing'
              AND lease_token = CAST(:lease_token AS UUID)
            RETURNING attempt_count, claimed_at,
                      claimed_by_worker_id, claimed_by_hostname, claimed_by_pid,
                      job_type
            """
        ),
        {
            "id": job_id,
            "error_code": error_code,
            "last_error": last_error,
            "lease_token": str(lease_token),
        },
    )
    mapping = result.mappings().one_or_none()
    if mapping is None:
        return False
    event_type = "abandoned" if finalized_by == "reaper" else "failed"
    await _insert_attempt_event(
        session,
        attempts_table=attempts_table,
        job_id=job_id,
        attempt_number=mapping["attempt_count"],
        event_type=event_type,
        worker_id=mapping["claimed_by_worker_id"],
        worker_hostname=mapping["claimed_by_hostname"],
        worker_pid=mapping["claimed_by_pid"],
        lease_token=lease_token,
        finalized_by=finalized_by,
        claimed_at=mapping["claimed_at"],
        error_code=error_code,
        error_message=last_error,
        stack_trace=stack_trace,
        job_type=mapping["job_type"],
        queue_name=queue_name,
    )
    return True


async def reset_for_retry(
    session: AsyncSession,
    *,
    table: str,
    attempts_table: str | None = None,
    job_id: str,
    lease_token: UUID,
    error_code: str | None = None,
    last_error: str | None = None,
    finalized_by: str = "worker",
    stack_trace: str | None = None,
    queue_name: str | None = None,
) -> int | None:
    """Transition processing→queued (retryable failure). Appends 'failed' or 'abandoned' event.

    ``finalized_by='worker'`` emits 'failed'; ``'reaper'`` emits 'abandoned'.
    The attempt_count is **not** incremented here — the next ``claim_job`` will
    increment it. The attempt that just ended is logged with the current value.

    Returns:
        The attempt_count of the attempt that just ended, or None if no row matched.
    """
    row = await session.execute(
        text(
            f"""
            UPDATE {table}
            SET status     = 'queued',
                updated_at = now(),
                {_CLEAR_PROCESSING_META}
            WHERE id = :id
              AND status = 'processing'
              AND lease_token = CAST(:lease_token AS UUID)
            RETURNING attempt_count, claimed_at,
                      claimed_by_worker_id, claimed_by_hostname, claimed_by_pid,
                      job_type
            """
        ),
        {"id": job_id, "lease_token": str(lease_token)},
    )
    mapping = row.mappings().one_or_none()
    if mapping is None:
        return None
    # In reset_for_retry the attempt is being cut short (not run to completion),
    # so both reaper and admin take the attempt "away" from the worker.
    # Worker-initiated retryable failures are per-attempt 'failed' events.
    event_type = "abandoned" if finalized_by in ("reaper", "admin") else "failed"
    await _insert_attempt_event(
        session,
        attempts_table=attempts_table,
        job_id=job_id,
        attempt_number=mapping["attempt_count"],
        event_type=event_type,
        worker_id=mapping["claimed_by_worker_id"],
        worker_hostname=mapping["claimed_by_hostname"],
        worker_pid=mapping["claimed_by_pid"],
        lease_token=lease_token,
        finalized_by=finalized_by,
        claimed_at=mapping["claimed_at"],
        error_code=error_code,
        error_message=last_error,
        stack_trace=stack_trace,
        job_type=mapping["job_type"],
        queue_name=queue_name,
    )
    return int(mapping["attempt_count"])


async def mark_enqueue_failed(
    session: AsyncSession,
    *,
    table: str,
    job_id: str,
    error: str,
) -> None:
    """Mark a job failed after the broker send failed immediately after insert.

    Only transitions from ``queued`` with ``attempt_count = 0`` (i.e. the row
    was just inserted and the broker send failed before any worker claimed it).
    No attempt event is written — no attempt was ever started.
    """
    _ = await session.execute(
        text(
            f"""
            UPDATE {table}
            SET status     = 'failed',
                error_code = 'enqueue_failed',
                last_error = :err,
                updated_at = now()
            WHERE id = :id
              AND status = 'queued'
              AND attempt_count = 0
            """
        ),
        {"id": job_id, "err": error},
    )


async def heartbeat_progress(
    session: AsyncSession,
    *,
    table: str,
    job_id: str,
    lease_token: UUID | None = None,
) -> bool:
    """Update progress_heartbeat_at / last_heartbeat_at to now() for a processing job.

    Predicates on ``lease_token`` when provided so a wedged prior worker whose
    lease was rotated by the reaper cannot bump the heartbeat on a new attempt.
    ``lease_token=None`` is only used by legacy call sites that have not been
    threaded yet; they retain the old status-only predicate for backward compat.

    Returns:
        True if a row was updated, False if the job is no longer processing
        under the caller's lease.
    """
    if lease_token is None:
        sql = f"""
            UPDATE {table}
            SET progress_heartbeat_at = now(),
                last_heartbeat_at     = now(),
                updated_at            = now()
            WHERE id = :id AND status = 'processing'
        """
        params: dict[str, Any] = {"id": job_id}
    else:
        sql = f"""
            UPDATE {table}
            SET progress_heartbeat_at = now(),
                last_heartbeat_at     = now(),
                updated_at            = now()
            WHERE id = :id
              AND status = 'processing'
              AND lease_token = CAST(:lease_token AS UUID)
        """
        params = {"id": job_id, "lease_token": str(lease_token)}
    result = await session.execute(text(sql), params)
    return result.rowcount > 0  # type: ignore[union-attr]


async def find_timed_out_jobs(
    session: AsyncSession, *, table: str
) -> list[TimedOutJob]:
    """Find processing jobs whose timeout_at has passed.

    Returns ``lease_token`` so the reaper can predicate its terminal UPDATE on
    the same token (race-safe without a lock).
    """
    schema, name = parse_qualified(table)
    t = make_jobs_table(name=name, schema=schema)
    stmt = select(
        t.c.id,
        t.c.timeout_strategy,
        t.c.attempt_count,
        t.c.max_attempts,
        t.c.lease_token,
    ).where(
        t.c.status == "processing",
        t.c.timeout_at.is_not(None),
        t.c.timeout_at < func.now(),
    )
    result = await session.execute(stmt)
    return [TimedOutJob.model_validate(dict(row)) for row in result.mappings()]
