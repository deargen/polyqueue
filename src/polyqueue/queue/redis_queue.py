"""RedisJobQueue — Redis-backed implementation of the JobQueue protocol.

Receipt == job_id for Redis (opaque to callers, but the backend derives
lease keys from it directly). SqsJobQueue would use SQS receipt_handle instead.

Key layout:
  jobs:pending              LIST    — job_id values, BRPOP to claim
  jobs:lease:{job_id}       STRING  — JSON blob {claimed_at, attempt}, TTL = lease_duration
  jobs:leases               ZSET    — job_id -> lease_expiry unix timestamp
  jobs:retry:scheduled      ZSET    — job_id -> retry_after unix timestamp
  jobs:maintenance:lock     STRING  — distributed lock; only one worker runs reaper/poller

Settlement order invariant:
  DB state is committed *before* Redis lease/retry keys are cleared. This ensures
  Postgres is the authoritative record. If Redis cleanup subsequently fails, a
  stale lease will be reaped by the maintenance loop, and claim() will skip the
  redelivered job_id because its DB status is no longer 'queued'.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from typing import TYPE_CHECKING, Any

from polyqueue.enqueue_options import EnqueueOptions, EnqueueResult
from polyqueue.metrics import MetricsHook, NoOpMetrics
from polyqueue.queue.db import (
    claim_job,
    insert_job,
    mark_enqueue_failed,
    mark_failed_terminal,
    reset_for_retry,
)
from polyqueue.queue.interface import ClaimedJob
from polyqueue.utils.payload import normalize_payload

if TYPE_CHECKING:
    from collections.abc import Callable
    from uuid import UUID

    from pydantic import BaseModel
    from redis.asyncio import Redis
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from polyqueue.worker.identity import WorkerInfo

logger = logging.getLogger(__name__)

_PENDING_KEY = "jobs:pending"
_LEASES_ZSET = "jobs:leases"
_RETRY_ZSET = "jobs:retry:scheduled"
_MAINTENANCE_LOCK = "jobs:maintenance:lock"

# Atomically move all due entries from retry ZSET → pending LIST.
# Using a Lua script prevents the ZREM/LPUSH split-brain on crash.
_MOVE_DUE_RETRIES_LUA = """
local due = redis.call('zrangebyscore', KEYS[1], '-inf', ARGV[1])
for _, job_id in ipairs(due) do
    redis.call('zrem', KEYS[1], job_id)
    redis.call('lpush', KEYS[2], job_id)
end
return #due
"""


def _lease_key(job_id: str) -> str:
    return f"jobs:lease:{job_id}"


class RedisJobQueue:
    """
    Async Redis job queue.

    Args:
        redis: An async Redis client instance.
        session_factory: SQLAlchemy async session factory for Postgres state updates.
        table_name: Postgres table that stores job rows (default: ``polyqueue_jobs``).
        table_schema: Postgres schema containing the table (default: ``public``).
        lease_duration_seconds: How long a lease lives before the reaper can reclaim it.
        heartbeat_interval_seconds: How often the heartbeat extends the lease.
        max_attempts: Default max attempts copied to DB row at enqueue time.
            The DB value is authoritative thereafter; this default is for new jobs only.
        retry_backoff_seconds: Per-attempt backoff list (index 0 = first attempt).
    """

    def __init__(
        self,
        redis: Redis,  # type: ignore[type-arg]
        session_factory: async_sessionmaker[AsyncSession],
        *,
        table_name: str = "polyqueue_jobs",
        table_schema: str = "polyqueue",
        lease_duration_seconds: int = 600,
        heartbeat_interval_seconds: int = 120,
        max_attempts: int = 3,
        retry_backoff_seconds: list[int] | None = None,
        default_max_run_seconds: int | None = None,
        default_timeout_strategy: str = "retry",
        metrics: MetricsHook | None = None,
    ) -> None:
        self._redis = redis
        self.session_factory = session_factory
        self._table = f"{table_schema}.{table_name}"
        self._attempts_table = f"{table_schema}.{table_name}_attempts"
        self._lease_duration = lease_duration_seconds
        self._heartbeat_interval = heartbeat_interval_seconds
        self._max_attempts = max_attempts
        self._retry_backoff = retry_backoff_seconds or [10, 60, 300]
        self._default_max_run_seconds = default_max_run_seconds
        self._default_timeout_strategy = default_timeout_strategy
        self._metrics: MetricsHook = metrics or NoOpMetrics()
        # Token generated fresh on each acquire_maintenance_lock() success so that
        # a stale release_maintenance_lock() call can never delete a newly-acquired lock.
        self._maintenance_token: str | None = None
        # active heartbeat tasks keyed by job_id
        self._heartbeat_tasks: dict[str, asyncio.Task[None]] = {}
        # lease_token per in-flight job_id (receipt == job_id in Redis); needed
        # by ack()/fail() to pass as predicate on terminal DB UPDATE.
        self._leases: dict[str, UUID] = {}
        self._worker_info: WorkerInfo | None = None

    # ──────────────────────────────────────────────
    # Protocol methods
    # ──────────────────────────────────────────────

    def set_worker_info(self, worker_info: WorkerInfo) -> None:
        self._worker_info = worker_info

    async def enqueue(
        self,
        target: str | Callable[..., Any],
        payload: dict[str, Any] | BaseModel,
        *,
        options: EnqueueOptions | None = None,
    ) -> EnqueueResult:
        """Insert Postgres row first, then push job_id to Redis pending list.

        If Redis push fails, the Postgres row is marked failed immediately so
        it can be re-enqueued once the broker recovers.
        """
        effective_options = options or EnqueueOptions()

        if callable(target):
            from polyqueue.jobs.dispatcher import (
                _infer_payload_model,
                job_type_for_handler,
            )

            job_type = job_type_for_handler(target)
            model = _infer_payload_model(target)
            if model is not None and isinstance(payload, dict):
                payload = model.model_validate(payload)
        else:
            job_type = target

        job_id = effective_options.job_id or str(uuid.uuid4())

        payload = normalize_payload(payload)
        effective_max_run = (
            effective_options.max_run_seconds
            if effective_options.max_run_seconds is not None
            else self._default_max_run_seconds
        )
        effective_strategy = (
            effective_options.timeout_strategy
            if effective_options.timeout_strategy is not None
            else self._default_timeout_strategy
        )
        async with self.session_factory() as session:
            inserted = await insert_job(
                session,
                table=self._table,
                job_id=job_id,
                job_type=job_type,
                payload=payload,
                max_attempts=self._max_attempts,
                max_run_seconds=effective_max_run,
                timeout_strategy=effective_strategy,
            )
            await session.commit()

        if not inserted:
            raise ValueError(
                f"Job {job_id!r} already exists and is not eligible for re-enqueue "
                "(status is not 'failed' / error_code is not 'enqueue_failed')"
            )

        try:
            await self._redis.lpush(_PENDING_KEY, job_id)
        except Exception as exc:
            logger.error("enqueue: Redis push failed for job %s: %s", job_id, exc)
            async with self.session_factory() as session:
                await mark_enqueue_failed(
                    session, table=self._table, job_id=job_id, error=str(exc)
                )
                await session.commit()
            raise
        self._metrics.job_enqueued(job_type)
        return EnqueueResult(job_id=job_id)

    async def claim(self) -> ClaimedJob | None:
        """BRPOP from pending list with a 1-second timeout.

        On success: update Postgres to processing, create lease, start heartbeat.
        Returns None if no job is available or if lease setup fails (partial failure
        schedules an immediate retry so the job is not lost).
        """
        result = await self._redis.brpop(_PENDING_KEY, timeout=1)
        if result is None:
            return None

        _, job_id_bytes = result
        job_id: str = job_id_bytes.decode()

        # Load payload from Postgres and atomically bump state.
        # Guard against stale Redis state: skip if row is no longer in a claimable state.
        wi = self._worker_info
        async with self.session_factory() as session:
            record = await claim_job(
                session,
                table=self._table,
                attempts_table=self._attempts_table,
                job_id=job_id,
                worker_id=wi.worker_id if wi else "",
                worker_hostname=wi.hostname if wi else "",
                worker_pid=wi.pid if wi else 0,
                queue_name="redis",
            )
            await session.commit()

        if record is None:
            logger.warning(
                "claim: job %s popped from Redis but not in a claimable state in Postgres — skipping",
                job_id,
            )
            return None

        job_type: str = record.job_type
        payload: dict[str, object] = record.payload
        attempt: int = record.attempt_count
        max_attempts: int = record.max_attempts

        # Create lease; if this fails, schedule an immediate retry and abort.
        # attempt_count is already incremented — the failed claim setup intentionally
        # consumes an attempt (the job was popped off the queue).
        try:
            lease_expiry = time.time() + self._lease_duration
            lease_blob = json.dumps({"claimed_at": time.time(), "attempt": attempt})
            pipe = self._redis.pipeline()
            pipe.set(_lease_key(job_id), lease_blob, ex=self._lease_duration)
            pipe.zadd(_LEASES_ZSET, {job_id: lease_expiry})
            await pipe.execute()
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "claim: lease creation failed for job %s (attempt %d already consumed): %s",
                job_id,
                attempt,
                exc,
            )
            # Best-effort retry scheduling. If this also fails, revert Postgres to
            # 'queued' so the reaper can recover the job rather than leaving it
            # stuck in 'processing' with no lease and no retry entry.
            try:
                await self._schedule_retry(job_id, backoff_seconds=1)
            except Exception as retry_exc:  # noqa: BLE001
                logger.error(
                    "claim: retry scheduling also failed for job %s — reverting to queued: %s",
                    job_id,
                    retry_exc,
                )
                async with self.session_factory() as session:
                    await reset_for_retry(
                        session,
                        table=self._table,
                        attempts_table=self._attempts_table,
                        job_id=job_id,
                        lease_token=record.lease_token,
                        error_code="lease_setup_failed",
                        last_error="Redis lease/retry setup failed",
                        queue_name="redis",
                    )
                    await session.commit()
            return None

        self._cancel_heartbeat(
            job_id
        )  # guard against stale task if job_id is somehow reused
        task = asyncio.create_task(self._heartbeat(job_id))
        self._heartbeat_tasks[job_id] = task
        self._leases[job_id] = record.lease_token

        return ClaimedJob(
            receipt=job_id,
            job_id=job_id,
            job_type=job_type,
            payload=payload,
            attempt=attempt,
            max_attempts=max_attempts,
            worker_id=record.claimed_by_worker_id or "",
            worker_hostname=record.claimed_by_hostname or "",
            worker_pid=record.claimed_by_pid or 0,
            claimed_at=record.claimed_at,
            queued_at=record.created_at,
            max_run_seconds=record.max_run_seconds,
            timeout_strategy=record.timeout_strategy,
            timeout_at=record.timeout_at,
            lease_token=record.lease_token,
        )

    async def ack(self, receipt: str) -> None:
        """Clear lease keys from Redis. DB was already marked succeeded by the worker.

        Idempotent: double-clearing is harmless. If cleanup fails, stale leases
        are reaped by maintenance; redelivered jobs skip by claim() status guard.
        """
        job_id = receipt
        self._cancel_heartbeat(job_id)
        self._leases.pop(job_id, None)

        try:
            pipe = self._redis.pipeline()
            pipe.delete(_lease_key(job_id))
            pipe.zrem(_LEASES_ZSET, job_id)
            await pipe.execute()
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "ack: Redis lease cleanup failed for job %s — "
                "stale lease will be reaped by maintenance loop: %s",
                job_id,
                exc,
            )

    async def fail(self, receipt: str, error: str, *, retryable: bool = True) -> None:
        """Settle the job. DB is updated first; lease keys are cleared after.

        If retryable: reset Postgres to queued (DB first), then clear lease and
        push to retry ZSET with backoff.
        If terminal: leave Postgres in whatever terminal state the worker already set,
        then clear lease keys.
        Idempotent: double-clearing lease keys is harmless.
        """
        job_id = receipt
        self._cancel_heartbeat(job_id)
        lease_token = self._leases.pop(job_id, None)

        if retryable:
            if lease_token is None:
                logger.warning(
                    "fail(retryable): no lease_token for job %s — skipping DB reset",
                    job_id,
                )
                return
            async with self.session_factory() as session:
                attempt = await reset_for_retry(
                    session,
                    table=self._table,
                    attempts_table=self._attempts_table,
                    job_id=job_id,
                    lease_token=lease_token,
                    error_code="handler_error",
                    last_error=error,
                    finalized_by="worker",
                    queue_name="redis",
                )
                await session.commit()

            if attempt is None:
                logger.warning(
                    "fail: job %s was not in processing state; skipping retry reschedule",
                    job_id,
                )
                return

            # DB is now committed. Clear lease; if Redis is down, reaper will recover.
            try:
                pipe = self._redis.pipeline()
                pipe.delete(_lease_key(job_id))
                pipe.zrem(_LEASES_ZSET, job_id)
                await pipe.execute()
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "fail: Redis lease cleanup failed for job %s — "
                    "stale lease will be reaped by maintenance loop: %s",
                    job_id,
                    exc,
                )

            backoff = self._backoff_for_attempt(attempt)
            try:
                await self._schedule_retry(job_id, backoff_seconds=backoff)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "fail: retry scheduling failed for job %s — "
                    "DB is already reset to queued; reaper will recover: %s",
                    job_id,
                    exc,
                )
        else:
            # Terminal: DB was already updated by the worker before calling fail().
            # Just clear the lease keys.
            try:
                pipe = self._redis.pipeline()
                pipe.delete(_lease_key(job_id))
                pipe.zrem(_LEASES_ZSET, job_id)
                await pipe.execute()
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "fail: Redis lease cleanup failed for job %s — "
                    "stale lease will be reaped by maintenance loop: %s",
                    job_id,
                    exc,
                )

    # ──────────────────────────────────────────────
    # Maintenance — called from worker maintenance loop
    # ──────────────────────────────────────────────

    async def reap_abandoned_leases(self) -> None:
        """Find jobs whose lease deadline has passed, confirm the TTL key is gone
        (truly abandoned), and either schedule retry or mark failed.
        """
        from sqlalchemy import text

        now = time.time()
        expired_raw = await self._redis.zrangebyscore(_LEASES_ZSET, "-inf", now)

        for job_id_bytes in expired_raw:
            job_id: str = job_id_bytes.decode()

            # If the TTL key still exists, the heartbeat is alive — not truly expired
            if await self._redis.exists(_lease_key(job_id)):
                continue

            async with self.session_factory() as session:
                row = await session.execute(
                    text(
                        f"""
                        SELECT attempt_count, max_attempts, lease_token
                        FROM {self._table}
                        WHERE id = :id AND status = 'processing'
                        """
                    ),
                    {"id": job_id},
                )
                record = row.mappings().one_or_none()

                if record is None:
                    # Already acked/failed — just clean up ZSET
                    await self._redis.zrem(_LEASES_ZSET, job_id)
                    continue

                attempt: int = record["attempt_count"]
                max_attempts: int = record["max_attempts"]
                reaped_token = record["lease_token"]

                if attempt < max_attempts:
                    backoff = self._backoff_for_attempt(attempt)
                    logger.warning(
                        "reaper: job %s lease expired after attempt %d/%d — scheduling retry in %ds",
                        job_id,
                        attempt,
                        max_attempts,
                        backoff,
                    )
                    attempt_after = await reset_for_retry(
                        session,
                        table=self._table,
                        attempts_table=self._attempts_table,
                        job_id=job_id,
                        lease_token=reaped_token,
                        error_code="lease_expired",
                        last_error="Worker lease expired; reaping",
                        finalized_by="reaper",
                        queue_name="redis",
                    )
                    await session.commit()

                    if attempt_after is None:
                        logger.info(
                            "reaper: job %s was already settled during retry reset",
                            job_id,
                        )
                        await self._redis.zrem(_LEASES_ZSET, job_id)
                        continue

                    try:
                        await self._schedule_retry(job_id, backoff_seconds=backoff)
                    except Exception as exc:  # noqa: BLE001
                        logger.error(
                            "reaper: _schedule_retry failed for job %s — pushing directly to pending: %s",
                            job_id,
                            exc,
                        )
                        try:
                            await self._redis.lpush(_PENDING_KEY, job_id)
                        except Exception as lpush_exc:  # noqa: BLE001
                            logger.critical(
                                "reaper: lpush also failed for job %s — job may be stuck as 'queued' in DB: %s",
                                job_id,
                                lpush_exc,
                            )
                else:
                    logger.error(
                        "reaper: job %s exhausted max_attempts (%d) — marking failed",
                        job_id,
                        max_attempts,
                    )
                    changed = await mark_failed_terminal(
                        session,
                        table=self._table,
                        attempts_table=self._attempts_table,
                        job_id=job_id,
                        lease_token=reaped_token,
                        error_code="lease_expired",
                        last_error="Lease expired after max attempts",
                        finalized_by="reaper",
                        queue_name="redis",
                    )
                    await session.commit()
                    if not changed:
                        logger.info(
                            "reaper: job %s was already settled before lease-expired terminal mark",
                            job_id,
                        )

            await self._redis.zrem(_LEASES_ZSET, job_id)

        # Reconcile: find jobs that are 'queued' in DB but may be missing from Redis.
        # Duplicate lpush is safe — claim() will skip any already-processing job.
        async with self.session_factory() as session:
            orphaned = await session.execute(
                text(
                    f"""
                    SELECT id FROM {self._table}
                    WHERE status = 'queued'
                      AND updated_at < now() - interval '5 minutes'
                    """
                )
            )
            rows = orphaned.mappings().all()

        for row in rows:
            orphan_id: str = row["id"]
            # Only push if absent from retry ZSET, leases ZSET, and pending LIST
            # to avoid accumulating duplicate entries on every reaper pass.
            in_retry = await self._redis.zscore(_RETRY_ZSET, orphan_id) is not None
            in_lease = await self._redis.zscore(_LEASES_ZSET, orphan_id) is not None
            in_pending = await self._redis.lpos(_PENDING_KEY, orphan_id) is not None
            if not in_retry and not in_lease and not in_pending:
                await self._redis.lpush(_PENDING_KEY, orphan_id)
                logger.warning(
                    "reaper: re-queued orphaned job %s missing from Redis structures",
                    orphan_id,
                )
            else:
                logger.debug(
                    "reaper: orphaned job %s already tracked in Redis (retry=%s lease=%s) — skipping",
                    orphan_id,
                    in_retry,
                    in_lease,
                )

    async def move_due_retries(self) -> None:
        """Move due entries from jobs:retry:scheduled to jobs:pending.

        Uses a server-side Lua script so that ZREM and LPUSH are atomic —
        a crash between the two cannot lose a job.
        """
        now = time.time()
        moved = (
            await self._redis.eval(  # Redis server-side Lua EVAL — not Python eval()
                _MOVE_DUE_RETRIES_LUA, 2, _RETRY_ZSET, _PENDING_KEY, now
            )
        )
        if moved:
            logger.debug("retry poller: re-enqueued %d due jobs", moved)

    async def acquire_maintenance_lock(self, ttl_seconds: int = 90) -> bool:
        """Try to acquire the single-leader maintenance lock. Returns True if acquired.

        Generates a fresh token per acquisition so that a stale release() call from
        a previous lease cannot delete the newly-acquired lock.
        """
        token = str(uuid.uuid4())
        result = await self._redis.set(
            _MAINTENANCE_LOCK, token, nx=True, ex=ttl_seconds
        )
        if result is not None:
            self._maintenance_token = token
            return True
        return False

    async def release_maintenance_lock(self) -> None:
        """Release the lock only if this instance still owns the current token.

        Uses a Redis server-side Lua script to atomically check ownership and delete.
        If the lock expired or was acquired by another worker, this is a no-op.
        """
        token = self._maintenance_token
        if token is None:
            return
        self._maintenance_token = None
        # Redis EVAL runs Lua atomically on the server — this is not Python eval().
        compare_and_delete_lua = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        await self._redis.eval(  # Redis server-side Lua EVAL — not Python eval()
            compare_and_delete_lua, 1, _MAINTENANCE_LOCK, token
        )

    # ──────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────

    async def close(self) -> None:
        """Cancel all heartbeat tasks and close the Redis client."""
        for job_id in list(self._heartbeat_tasks):
            self._cancel_heartbeat(job_id)
        await self._redis.aclose()

    async def _heartbeat(self, job_id: str) -> None:
        """Periodically extend the lease TTL while the job is running."""
        try:
            while True:
                await asyncio.sleep(self._heartbeat_interval)
                new_expiry = time.time() + self._lease_duration
                pipe = self._redis.pipeline()
                pipe.expire(_lease_key(job_id), self._lease_duration)
                pipe.zadd(_LEASES_ZSET, {job_id: new_expiry})
                await pipe.execute()
                logger.debug("heartbeat: extended lease for job %s", job_id)
        except asyncio.CancelledError:
            pass

    def _cancel_heartbeat(self, job_id: str) -> None:
        task = self._heartbeat_tasks.pop(job_id, None)
        if task is not None:
            task.cancel()

    async def _schedule_retry(self, job_id: str, *, backoff_seconds: int) -> None:
        retry_at = time.time() + backoff_seconds
        await self._redis.zadd(_RETRY_ZSET, {job_id: retry_at})
        logger.info(
            "retry scheduled: job %s re-enters pending in %ds", job_id, backoff_seconds
        )

    def _backoff_for_attempt(self, attempt: int) -> int:
        """Return backoff seconds for the given 1-based attempt number."""
        idx = max(0, attempt - 1)
        if idx < len(self._retry_backoff):
            return self._retry_backoff[idx]
        return self._retry_backoff[-1]
