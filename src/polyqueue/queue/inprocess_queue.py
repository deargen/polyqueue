"""InProcessQueue — no-broker fallback that runs jobs inline via asyncio.create_task().

Use this when no external broker (Redis / SQS / Azure SB) is available, e.g.
in development, CI, or single-process deployments that don't need distributed workers.

Behaviour contract:
- enqueue(): immediately spawns an asyncio task that dispatches the job.
  The task runs concurrently in the same event loop as the caller — no external
  process or broker is involved.
- The current process must import handler-registration modules before enqueue
  (for example via ``make_job(...)`` or ``@register_handler(...)``), otherwise
  dispatch fails with ``Unknown job type``.
- claim() / ack() / fail() / maintenance methods: all no-ops; the adapter self-manages
  job lifecycle via the spawned tasks.
- No Postgres ``jobs`` table is required; enqueue() does NOT persist anything.
  Jobs exist only in memory for the duration of the task.

Concurrency: controlled entirely by the event loop. For CPU-bound work or strict
serialisation, use a real broker backend with a single worker process instead.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import TYPE_CHECKING, Any

from polyqueue.enqueue_options import EnqueueOptions, EnqueueResult
from polyqueue.jobs.context import JobContext
from polyqueue.utils.payload import normalize_payload

if TYPE_CHECKING:
    from collections.abc import Callable

    from pydantic import BaseModel
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from polyqueue.queue.interface import ClaimedJob
    from polyqueue.worker.identity import WorkerInfo

logger = logging.getLogger(__name__)


class InProcessQueue:
    """No-worker fallback: dispatches jobs as local asyncio tasks.

    Args:
        session_factory: Passed through to the handler's JobContext so it can
            open DB sessions.  May be None if registered handlers do not need
            DB access.
        max_attempts: How many times to retry a handler that raises a non-terminal
            exception before giving up.
        retry_backoff_seconds: Per-attempt sleep before retry (index 0 = first retry).
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession] | None = None,  # type: ignore[type-arg]
        *,
        max_attempts: int = 3,
        retry_backoff_seconds: list[int] | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._max_attempts = max_attempts
        self._retry_backoff = retry_backoff_seconds or [10, 60, 300]
        # Strong references so GC does not cancel in-flight tasks.
        self._tasks: set[asyncio.Task[None]] = set()
        self._worker_info: WorkerInfo | None = None

    # ── JobQueue Protocol ──────────────────────────────────────────────────────

    def set_worker_info(self, worker_info: WorkerInfo) -> None:
        self._worker_info = worker_info

    async def enqueue(
        self,
        target: str | Callable[..., Any],
        payload: dict[str, Any] | BaseModel,
        *,
        options: EnqueueOptions | None = None,
    ) -> EnqueueResult:
        """Spawn an asyncio task that dispatches the job immediately.

        Returns as soon as the task is scheduled; the handler runs concurrently.

        Note: ``max_run_seconds`` and ``timeout_strategy`` are accepted for
        protocol compatibility but ignored — the in-process backend does not
        persist job metadata or enforce time limits.
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

        if (
            effective_options.max_run_seconds is not None
            or effective_options.timeout_strategy is not None
        ):
            logger.warning(
                "inprocess.enqueue: max_run_seconds/timeout_strategy ignored for job_id=%s",
                job_id,
            )
        payload_dict = normalize_payload(payload)
        task = asyncio.create_task(
            self._run_with_retry(job_id, job_type, payload_dict),
            name=f"polyqueue:{job_type}:{job_id}",
        )
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        logger.info(
            "inprocess.enqueue job_id=%s job_type=%s (running as asyncio task)",
            job_id,
            job_type,
        )
        return EnqueueResult(job_id=job_id)

    async def claim(self) -> ClaimedJob | None:
        """No-op — jobs are dispatched on enqueue, not polled."""
        return None

    async def ack(self, receipt: str) -> None:
        """No-op — lifecycle is managed internally."""

    async def fail(self, receipt: str, error: str, *, retryable: bool = True) -> None:
        """No-op — lifecycle is managed internally."""

    async def reap_abandoned_leases(self) -> None:
        """No-op — no leases exist in the in-process adapter."""

    async def move_due_retries(self) -> None:
        """No-op — retries are handled by asyncio.sleep() inside the task."""

    async def acquire_maintenance_lock(self, ttl_seconds: int = 90) -> bool:
        """Always returns False — no maintenance is needed."""
        return False

    async def release_maintenance_lock(self) -> None:
        """No-op."""

    async def close(self) -> None:
        """Cancel all in-flight tasks and wait for them to settle."""
        if not self._tasks:
            return
        tasks = list(self._tasks)
        for t in tasks:
            _ = t.cancel()
        _ = await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("inprocess.close: cancelled %d in-flight task(s)", len(tasks))

    # ── Internal helpers ───────────────────────────────────────────────────────

    async def _run_with_retry(
        self, job_id: str, job_type: str, payload: dict[str, Any]
    ) -> None:
        """Run the handler, retrying up to max_attempts on transient errors."""
        from polyqueue.jobs.dispatcher import TerminalError, dispatch

        for attempt in range(1, self._max_attempts + 1):
            wi = self._worker_info
            ctx = JobContext.make(
                job_id=job_id,
                job_type=job_type,
                attempt=attempt,
                max_attempts=self._max_attempts,
                session_factory=self._session_factory,
                worker_id=wi.worker_id if wi else "",
                worker_hostname=wi.hostname if wi else "",
                worker_pid=wi.pid if wi else 0,
            )
            try:
                await dispatch(ctx, payload)
            except TerminalError as exc:
                logger.error(
                    "inprocess job_failed_terminal job_id=%s job_type=%s attempt=%d error=%s",
                    job_id,
                    job_type,
                    attempt,
                    exc,
                )
                return
            except asyncio.CancelledError:
                logger.warning(
                    "inprocess job_cancelled job_id=%s job_type=%s attempt=%d",
                    job_id,
                    job_type,
                    attempt,
                )
                raise
            except Exception as exc:  # noqa: BLE001
                if attempt < self._max_attempts:
                    backoff = self._backoff_for_attempt(attempt)
                    logger.warning(
                        "inprocess job_failed_retryable job_id=%s job_type=%s attempt=%d/%d "
                        "error=%s — retrying in %ds",
                        job_id,
                        job_type,
                        attempt,
                        self._max_attempts,
                        exc,
                        backoff,
                    )
                    await asyncio.sleep(backoff)
                else:
                    logger.error(
                        "inprocess job_failed_terminal job_id=%s job_type=%s attempt=%d/%d error=%s",
                        job_id,
                        job_type,
                        attempt,
                        self._max_attempts,
                        exc,
                    )
            else:
                logger.info(
                    "inprocess job_succeeded job_id=%s job_type=%s attempt=%d",
                    job_id,
                    job_type,
                    attempt,
                )
                return

    def _backoff_for_attempt(self, attempt: int) -> int:
        """Return backoff seconds for the given 1-based attempt number."""
        idx = max(0, attempt - 1)
        if idx < len(self._retry_backoff):
            return self._retry_backoff[idx]
        return self._retry_backoff[-1]
