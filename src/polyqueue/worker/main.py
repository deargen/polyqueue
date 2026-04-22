"""Worker entrypoint — serial polling loop with separate reaper and retry-poll tasks.

Usage:
    python -m polyqueue.worker

The worker rejects the 'none' backend — InProcessQueue is for in-app use only.

Environment variables (POLYQUEUE_* prefix, see polyqueue.config.PolyqueueSettings):
    POLYQUEUE_DB_URL, POLYQUEUE_BACKEND, POLYQUEUE_REDIS_URL, etc.

Or pass a PolyqueueSettings instance directly to main(settings=...).

Register job handlers before starting:
    from polyqueue.jobs.dispatcher import register
    register("my_job_type", my_async_handler)
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from polyqueue.enqueue_options import EnqueueOptions
from polyqueue.jobs.context import JobContext
from polyqueue.jobs.dispatcher import TerminalError, dispatch
from polyqueue.metrics import MetricsHook, NoOpMetrics
from polyqueue.queue.db import (
    heartbeat_progress,
    mark_failed_terminal,
    mark_succeeded_with_result,
)
from polyqueue.queue.factory import AnyJobQueue, get_queue
from polyqueue.utils.result import normalize_result
from polyqueue.worker.identity import WorkerInfo, make_worker_info

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from polyqueue.config import PolyqueueSettings
    from polyqueue.queue.interface import ClaimedJob

logger = logging.getLogger(__name__)


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _setup_signal_handlers(shutdown: asyncio.Event) -> None:
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, shutdown.set)
        except NotImplementedError:
            # Windows does not support add_signal_handler; fall back to signal.signal.
            signal.signal(sig, lambda *_: shutdown.set())


async def _run_progress_heartbeat(
    session_factory: async_sessionmaker[AsyncSession],
    table: str,
    job_id: str,
    interval: int,
    lease_token: UUID | None = None,
) -> None:
    """Periodically update progress_heartbeat_at while a job is running."""
    try:
        while True:
            await asyncio.sleep(interval)
            async with session_factory() as session:
                await heartbeat_progress(
                    session, table=table, job_id=job_id, lease_token=lease_token
                )
                await session.commit()
    except asyncio.CancelledError:
        pass


async def _settle_serialize_failure(
    queue: AnyJobQueue,
    job: ClaimedJob,
    session_factory: async_sessionmaker[AsyncSession],
    table: str,
    attempts_table: str,
    metrics: MetricsHook,
    duration_ms: int,
    exc: TypeError,
) -> None:
    """Terminal settlement when normalize_result() raises TypeError."""
    logger.error(
        "result_serialization_failed job_id=%s job_type=%s: %s",
        job.job_id,
        job.job_type,
        exc,
    )
    metrics.job_failed(job.job_type, "result_serialization_failed", duration_ms)
    if job.lease_token is None:
        logger.warning(
            "serialize_failure: job %s has no lease_token; skipping DB mark",
            job.job_id,
        )
        changed = False
    else:
        async with session_factory() as session:
            changed = await mark_failed_terminal(
                session,
                table=table,
                attempts_table=attempts_table,
                job_id=job.job_id,
                lease_token=job.lease_token,
                error_code="result_serialization_failed",
                last_error=str(exc),
                finalized_by="worker",
            )
            await session.commit()
    if not changed:
        logger.warning(
            "job %s was already settled before result_serialization_failed mark",
            job.job_id,
        )
    with contextlib.suppress(Exception):
        await queue.fail(job.receipt, str(exc), retryable=False)


async def _handle(
    queue: AnyJobQueue,
    job: ClaimedJob,
    session_factory: async_sessionmaker[AsyncSession],
    table: str,
    attempts_table: str,
    worker_info: WorkerInfo,
    progress_interval: int,
    metrics: MetricsHook,
) -> None:
    """Dispatch one job with timeout enforcement and progress heartbeat."""

    async def _progress_cb() -> None:
        async with session_factory() as session:
            await heartbeat_progress(
                session,
                table=table,
                job_id=job.job_id,
                lease_token=job.lease_token,
            )
            await session.commit()

    ctx = JobContext.make(
        job_id=job.job_id,
        job_type=job.job_type,
        attempt=job.attempt,
        max_attempts=job.max_attempts,
        session_factory=session_factory,
        worker_id=worker_info.worker_id,
        worker_hostname=worker_info.hostname,
        worker_pid=worker_info.pid,
        claimed_at=job.claimed_at,
        timeout_at=job.timeout_at,
        max_run_seconds=job.max_run_seconds,
        progress_callback=_progress_cb,
        queued_at=job.queued_at,
        options=EnqueueOptions(
            job_id=job.job_id,
            max_run_seconds=job.max_run_seconds,
            timeout_strategy=job.timeout_strategy,
        ),
    )
    start = datetime.now(UTC)

    timeout_seconds: float | None = None
    if job.max_run_seconds is not None:
        timeout_seconds = float(job.max_run_seconds)

    logger.info(
        "job_started job_id=%s job_type=%s attempt=%d worker_id=%s timeout_at=%s",
        job.job_id,
        job.job_type,
        job.attempt,
        worker_info.worker_id,
        job.timeout_at,
    )
    metrics.job_started(job.job_type, worker_info.worker_id)

    progress_task = asyncio.create_task(
        _run_progress_heartbeat(
            session_factory, table, job.job_id, progress_interval, job.lease_token
        )
    )

    dispatch_result = None
    try:
        if timeout_seconds is not None:
            dispatch_result = await asyncio.wait_for(
                dispatch(ctx, job.payload), timeout=timeout_seconds
            )
        else:
            dispatch_result = await dispatch(ctx, job.payload)

    except TimeoutError:
        progress_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await progress_task
        duration_ms = int((datetime.now(UTC) - start).total_seconds() * 1000)
        strategy = job.timeout_strategy or "retry"
        logger.error(
            "job_timeout job_id=%s job_type=%s attempt=%d duration_ms=%d strategy=%s",
            job.job_id,
            job.job_type,
            job.attempt,
            duration_ms,
            strategy,
        )
        metrics.job_timed_out(job.job_type, strategy, duration_ms)
        if strategy == "fail":
            async with session_factory() as session:
                changed = await mark_failed_terminal(
                    session,
                    table=table,
                    attempts_table=attempts_table,
                    job_id=job.job_id,
                    lease_token=job.lease_token,  # type: ignore[arg-type]
                    error_code="timeout",
                    last_error=f"Job exceeded max_run_seconds={job.max_run_seconds}",
                    finalized_by="worker",
                )
                await session.commit()
            if not changed:
                logger.warning(
                    "job %s was already settled before timeout terminal mark",
                    job.job_id,
                )
            with contextlib.suppress(Exception):
                await queue.fail(job.receipt, "timeout", retryable=False)
        elif strategy == "retry":
            retryable = job.attempt < job.max_attempts
            if not retryable:
                async with session_factory() as session:
                    changed = await mark_failed_terminal(
                        session,
                        table=table,
                        attempts_table=attempts_table,
                        job_id=job.job_id,
                        lease_token=job.lease_token,  # type: ignore[arg-type]
                        error_code="timeout",
                        last_error=f"Timeout after max attempts ({job.max_attempts})",
                        finalized_by="worker",
                    )
                    await session.commit()
                if not changed:
                    logger.warning(
                        "job %s was already settled before timeout terminal mark",
                        job.job_id,
                    )
            try:
                await queue.fail(job.receipt, "timeout", retryable=retryable)
            except Exception as exc:  # noqa: BLE001
                logger.error("fail() raised for timed-out job %s: %s", job.job_id, exc)
        else:  # "ignore"
            logger.warning(
                "job_timeout_ignored job_id=%s — strategy=ignore, not changing state",
                job.job_id,
            )
        return

    except TerminalError as exc:
        progress_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await progress_task
        duration_ms = int((datetime.now(UTC) - start).total_seconds() * 1000)
        logger.error(
            "job_failed_terminal job_id=%s job_type=%s attempt=%d duration_ms=%d error=%s",
            job.job_id,
            job.job_type,
            job.attempt,
            duration_ms,
            exc,
        )
        metrics.job_failed(job.job_type, "terminal_error", duration_ms)
        async with session_factory() as session:
            changed = await mark_failed_terminal(
                session,
                table=table,
                attempts_table=attempts_table,
                job_id=job.job_id,
                lease_token=job.lease_token,  # type: ignore[arg-type]
                error_code="terminal_error",
                last_error=str(exc),
                finalized_by="worker",
            )
            await session.commit()
        if not changed:
            logger.warning(
                "job %s was already settled before terminal error mark", job.job_id
            )
        with contextlib.suppress(Exception):
            await queue.fail(job.receipt, str(exc), retryable=False)
        return

    except Exception as exc:  # noqa: BLE001
        progress_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await progress_task
        from sqlalchemy import text as sa_text

        duration_ms = int((datetime.now(UTC) - start).total_seconds() * 1000)
        retryable = job.attempt < job.max_attempts
        if retryable:
            logger.warning(
                "job_failed_retryable job_id=%s job_type=%s attempt=%d/%d duration_ms=%d error=%s",
                job.job_id,
                job.job_type,
                job.attempt,
                job.max_attempts,
                duration_ms,
                exc,
            )
            async with session_factory() as session:
                _ = await session.execute(
                    sa_text(f"""
                        UPDATE {table}
                        SET error_code = 'handler_error', last_error = :err, updated_at = now()
                        WHERE id = :id
                          AND status = 'processing'
                          AND lease_token = CAST(:tok AS UUID)
                    """),
                    {
                        "id": job.job_id,
                        "err": str(exc),
                        "tok": str(job.lease_token) if job.lease_token else None,
                    },
                )
                await session.commit()
            metrics.job_retried(job.job_type, job.attempt)
        else:
            logger.error(
                "job_failed_terminal job_id=%s job_type=%s attempt=%d/%d duration_ms=%d error=%s",
                job.job_id,
                job.job_type,
                job.attempt,
                job.max_attempts,
                duration_ms,
                exc,
            )
            async with session_factory() as session:
                changed = await mark_failed_terminal(
                    session,
                    table=table,
                    attempts_table=attempts_table,
                    job_id=job.job_id,
                    lease_token=job.lease_token,  # type: ignore[arg-type]
                    error_code="max_attempts_exceeded",
                    last_error=str(exc),
                    finalized_by="worker",
                )
                await session.commit()
            if not changed:
                logger.warning(
                    "job %s was already settled before max-attempts terminal mark",
                    job.job_id,
                )
            metrics.job_failed(job.job_type, "max_attempts_exceeded", duration_ms)
        try:
            await queue.fail(job.receipt, str(exc), retryable=retryable)
        except Exception as settle_exc:  # noqa: BLE001
            logger.error("fail() raised for job %s: %s", job.job_id, settle_exc)
        return

    # Success
    progress_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await progress_task
    duration_ms = int((datetime.now(UTC) - start).total_seconds() * 1000)
    logger.info(
        "job_succeeded job_id=%s job_type=%s attempt=%d duration_ms=%d",
        job.job_id,
        job.job_type,
        job.attempt,
        duration_ms,
    )

    # Normalise the result before any DB writes.  TypeError means the handler
    # returned a non-JSON-serialisable value; treat as terminal so the job is
    # not retried (retrying won't fix a bad return type).
    try:
        normalised = normalize_result(dispatch_result)
    except TypeError as exc:
        await _settle_serialize_failure(
            queue, job, session_factory, table, attempts_table, metrics, duration_ms, exc
        )
        return

    # Settlement order invariant: DB committed before broker message settled.
    async with session_factory() as db_session:
        transitioned = await mark_succeeded_with_result(
            db_session,
            table=table,
            attempts_table=attempts_table,
            job_id=job.job_id,
            lease_token=job.lease_token,  # type: ignore[arg-type]
            result=normalised,
        )
        await db_session.commit()

    if transitioned:
        metrics.job_succeeded(job.job_type, duration_ms)
    else:
        # Job was already settled by a concurrent delivery (e.g. lease expired and
        # redelivered while this attempt was still running).  The prior settlement
        # in DB is authoritative; we still ack the broker message to clear this
        # delivery, but we do not emit a success metric for a stale transition.
        logger.warning(
            "success path: job %s was not in processing state; stale success — "
            "acking broker to clear duplicate delivery",
            job.job_id,
        )

    try:
        await queue.ack(job.receipt)
    except Exception as settle_exc:  # noqa: BLE001
        logger.error("ack() raised for job %s: %s", job.job_id, settle_exc)


async def _run_worker_heartbeat(
    session_factory: async_sessionmaker[AsyncSession],
    workers_table: str,
    worker_id: str,
    interval: int,
    current_job_ref: list[ClaimedJob | None],
    metrics: MetricsHook | None = None,
) -> None:
    """Periodically update the worker registry heartbeat."""
    from polyqueue.worker.registry import heartbeat_worker

    try:
        while True:
            await asyncio.sleep(interval)
            job = current_job_ref[0]
            async with session_factory() as session:
                await heartbeat_worker(
                    session,
                    table=workers_table,
                    worker_id=worker_id,
                    current_job_id=job.job_id if job else None,
                    current_job_started_at=job.claimed_at
                    if job and job.claimed_at
                    else None,
                )
                await session.commit()
            if metrics:
                metrics.worker_heartbeat(worker_id)
    except asyncio.CancelledError:
        pass


async def _run_reaper(
    queue: AnyJobQueue,
    session_factory: async_sessionmaker[AsyncSession],
    table: str,
    attempts_table: str,
    workers_table: str,
    stale_threshold_seconds: int,
    interval: int,
) -> None:
    """Periodically reap abandoned leases and reconcile timed-out jobs."""
    while True:
        try:
            await asyncio.sleep(interval)
            if await queue.acquire_maintenance_lock():
                try:
                    await queue.reap_abandoned_leases()
                    await _reconcile_timeouts(session_factory, table, attempts_table)
                    # Reap stale workers
                    from polyqueue.worker.registry import reap_stale_workers

                    async with session_factory() as session:
                        reaped = await reap_stale_workers(
                            session,
                            table=workers_table,
                            stale_threshold_seconds=stale_threshold_seconds,
                        )
                        await session.commit()
                    if reaped:
                        logger.info("reaper: marked %d stale worker(s) as dead", reaped)
                finally:
                    await queue.release_maintenance_lock()
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("reaper: unexpected error")


async def _reconcile_timeouts(
    session_factory: async_sessionmaker[AsyncSession],
    table: str,
    attempts_table: str,
) -> None:
    """Backstop: find processing jobs past timeout_at and settle them in DB.

    This catches cases where the worker-local timeout missed (crash, wedge).
    Only touches Postgres — broker delivery settlement is left to the broker's
    own visibility/lock expiry mechanism, because the maintenance loop does not
    hold a valid broker receipt for SQS/Azure backends.
    """
    from polyqueue.queue.db import find_timed_out_jobs, reset_for_retry

    async with session_factory() as session:
        timed_out = await find_timed_out_jobs(session, table=table)

    for row in timed_out:
        job_id = row.id
        strategy = row.timeout_strategy or "retry"
        attempt = row.attempt_count
        max_attempts = row.max_attempts
        reaped_token = row.lease_token

        logger.warning(
            "reaper: job %s exceeded timeout_at — strategy=%s attempt=%d/%d",
            job_id,
            strategy,
            attempt,
            max_attempts,
        )
        if reaped_token is None:
            logger.info(
                "reaper: job %s has no lease_token (concurrently settled?) — skipping",
                job_id,
            )
            continue

        if strategy == "fail":
            async with session_factory() as session:
                changed = await mark_failed_terminal(
                    session,
                    table=table,
                    attempts_table=attempts_table,
                    job_id=job_id,
                    lease_token=reaped_token,
                    error_code="timeout",
                    last_error="Backstop: job exceeded timeout_at (maintenance loop)",
                    finalized_by="reaper",
                )
                await session.commit()
            if not changed:
                logger.info("reaper: timed-out job %s was already settled", job_id)
        elif strategy == "retry":
            retryable = attempt < max_attempts
            if retryable:
                async with session_factory() as session:
                    attempt_after = await reset_for_retry(
                        session,
                        table=table,
                        attempts_table=attempts_table,
                        job_id=job_id,
                        lease_token=reaped_token,
                        error_code="timeout",
                        last_error="Backstop: job exceeded timeout_at",
                        finalized_by="reaper",
                    )
                    await session.commit()
                if attempt_after is None:
                    logger.info(
                        "reaper: timed-out job %s was already settled/reset", job_id
                    )
                else:
                    # Broker redelivery is handled by visibility/lock expiry.
                    # For Redis (receipt == job_id), the reaper's orphan reconciliation
                    # will re-push the job_id to the pending list on its next pass.
                    logger.info(
                        "reaper: timed-out job %s reset to queued — "
                        "broker will redeliver via expiry or reaper orphan reconciliation",
                        job_id,
                    )
            else:
                async with session_factory() as session:
                    changed = await mark_failed_terminal(
                        session,
                        table=table,
                        attempts_table=attempts_table,
                        job_id=job_id,
                        lease_token=reaped_token,
                        error_code="timeout",
                        last_error="Backstop: timeout after max attempts (maintenance loop)",
                        finalized_by="reaper",
                    )
                    await session.commit()
                if not changed:
                    logger.info("reaper: timed-out job %s was already settled", job_id)
        else:  # "ignore"
            logger.info("reaper: timed-out job %s — strategy=ignore, skipping", job_id)


async def _run_retry_poll(queue: AnyJobQueue, interval: int) -> None:
    """Frequently move due retries from the scheduled ZSET to the pending queue.

    Runs on every worker independently (move_due_retries is idempotent). A short
    interval here (default 1s) decouples retry precision from the slower reaper cycle,
    so a 10s backoff actually fires in ~10s rather than up to reaper_interval seconds.
    """
    while True:
        try:
            await asyncio.sleep(interval)
            await queue.move_due_retries()
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("retry poll: unexpected error")


async def main(
    settings: PolyqueueSettings | None = None,
    metrics: MetricsHook | None = None,
) -> None:
    _setup_logging()
    _metrics = metrics or NoOpMetrics()

    from polyqueue.config import PolyqueueSettings as _Settings

    _resolved = settings if settings is not None else _Settings()

    if _resolved.queue_backend == "none":
        raise ValueError(
            "The 'none' backend (InProcessQueue) cannot be used with the polyqueue worker. "
            "It is designed for in-app use only (embed in your FastAPI/ASGI lifespan). "
            "Set POLYQUEUE_BACKEND to 'redis', 'sqs', 'azure_service_bus', or 'pgmq'."
        )

    queue, session_factory, engine = get_queue(_resolved, metrics=_metrics)
    assert session_factory is not None, (
        "session_factory must not be None for worker backends"
    )

    worker_info = make_worker_info(_resolved.worker_name)
    queue.set_worker_info(worker_info)

    table = _resolved.qualified_table()
    attempts_table = _resolved.qualified_attempts_table()
    workers_table = _resolved.qualified_workers_table()
    progress_interval = _resolved.progress_heartbeat_interval_seconds

    # Register this worker in the registry
    from polyqueue.worker.registry import deregister_worker, register_worker

    async with session_factory() as session:
        await register_worker(
            session,
            table=workers_table,
            worker_info=worker_info,
            backend=_resolved.queue_backend,
        )
        await session.commit()

    _metrics.worker_started(worker_info.worker_id)

    shutdown = asyncio.Event()
    _setup_signal_handlers(shutdown)

    logger.info(
        "worker: starting up (backend=%s worker_id=%s hostname=%s pid=%d)",
        _resolved.queue_backend,
        worker_info.worker_id,
        worker_info.hostname,
        worker_info.pid,
    )
    reaper_task = asyncio.create_task(
        _run_reaper(
            queue,
            session_factory,
            table,
            attempts_table,
            workers_table,
            _resolved.worker_stale_threshold_seconds,
            _resolved.reaper_interval_seconds,
        )
    )
    retry_poll_task = asyncio.create_task(
        _run_retry_poll(queue, _resolved.retry_poll_interval_seconds)
    )
    bg_tasks = [reaper_task, retry_poll_task]

    current_job_ref: list[ClaimedJob | None] = [None]
    worker_hb_task = asyncio.create_task(
        _run_worker_heartbeat(
            session_factory,
            workers_table,
            worker_info.worker_id,
            _resolved.worker_heartbeat_interval_seconds,
            current_job_ref,
            _metrics,
        )
    )
    bg_tasks.append(worker_hb_task)

    try:
        while not shutdown.is_set():
            job = await queue.claim()
            if job is None:
                current_job_ref[0] = None
                await asyncio.sleep(1)
                continue

            current_job_ref[0] = job
            await _handle(
                queue,
                job,
                session_factory,
                table,
                attempts_table,
                worker_info,
                progress_interval,
                _metrics,
            )
            current_job_ref[0] = None
    except asyncio.CancelledError:
        logger.warning("worker: main loop cancelled")
        raise
    finally:
        _metrics.worker_stopped(worker_info.worker_id)

        if current_job_ref[0] is not None:
            logger.info(
                "worker: settling in-flight job %s before exit",
                current_job_ref[0].job_id,
            )
            with contextlib.suppress(Exception):
                await queue.fail(
                    current_job_ref[0].receipt, "worker_shutdown", retryable=True
                )

        # Deregister from worker registry
        with contextlib.suppress(Exception):
            async with session_factory() as session:
                await deregister_worker(
                    session, table=workers_table, worker_id=worker_info.worker_id
                )
                await session.commit()

        for t in bg_tasks:
            t.cancel()
        await asyncio.gather(*bg_tasks, return_exceptions=True)

        with contextlib.suppress(Exception):
            await queue.close()
        if engine is not None:
            with contextlib.suppress(Exception):
                await engine.dispose()

        logger.info("worker: exited cleanly")


def main_sync(
    settings: PolyqueueSettings | None = None, metrics: MetricsHook | None = None
) -> None:
    """Synchronous entrypoint for the polyqueue-worker CLI script."""
    asyncio.run(main(settings, metrics))


if __name__ == "__main__":
    main_sync()
