"""JobContext — framework-neutral execution context passed to every handler.

This is the stable app-facing surface.  Handlers should only depend on
``JobContext``, ``TerminalError``, and ``register`` — never on queue
internals.
"""

from __future__ import annotations

import logging
from collections.abc import (  # noqa: TC003 — Pydantic needs these at runtime
    Awaitable,
    Callable,
)
from datetime import datetime  # noqa: TC003

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker  # noqa: TC002

from polyqueue.enqueue_options import EnqueueOptions  # noqa: TC001


class JobContext(BaseModel):
    """Execution context passed to every job handler."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    job_id: str
    job_type: str
    attempt: int
    max_attempts: int
    session_factory: async_sessionmaker[AsyncSession] | None  # type: ignore[type-arg]
    logger: logging.Logger = Field(exclude=True)

    # ── Worker identity ───────────────────────────────────────────────────
    worker_id: str = ""
    worker_hostname: str = ""
    worker_pid: int = 0

    # ── Time limits ───────────────────────────────────────────────────────
    claimed_at: datetime | None = None
    timeout_at: datetime | None = None
    max_run_seconds: int | None = None

    # ── Enqueue context ───────────────────────────────────────────────────
    queued_at: datetime | None = None
    # DB-persisted effective options reconstructed at claim time.
    # Only job_id, max_run_seconds, and timeout_strategy are round-tripped
    # through the DB — any fields not stored (e.g. future extensions) are not
    # preserved from the original EnqueueOptions passed at enqueue time.
    options: EnqueueOptions | None = None

    # ── Progress heartbeat ────────────────────────────────────────────────
    progress_callback: Callable[[], Awaitable[None]] | None = Field(
        default=None, exclude=True
    )

    async def heartbeat_progress(self) -> None:
        """Report that the handler is still making progress.

        Calls the runtime-provided callback which updates
        ``progress_heartbeat_at`` in Postgres.
        """
        if self.progress_callback is not None:
            await self.progress_callback()

    @classmethod
    def make(
        cls,
        *,
        job_id: str,
        job_type: str,
        attempt: int,
        max_attempts: int,
        session_factory: async_sessionmaker[AsyncSession] | None,  # type: ignore[type-arg]
        worker_id: str = "",
        worker_hostname: str = "",
        worker_pid: int = 0,
        claimed_at: datetime | None = None,
        timeout_at: datetime | None = None,
        max_run_seconds: int | None = None,
        progress_callback: Callable[[], Awaitable[None]] | None = None,
        queued_at: datetime | None = None,
        options: EnqueueOptions | None = None,
    ) -> JobContext:
        """Construct a context with a logger scoped to this job."""
        log = logging.getLogger(f"polyqueue.job.{job_type}.{job_id}")
        return cls(
            job_id=job_id,
            job_type=job_type,
            attempt=attempt,
            max_attempts=max_attempts,
            session_factory=session_factory,
            logger=log,
            worker_id=worker_id,
            worker_hostname=worker_hostname,
            worker_pid=worker_pid,
            claimed_at=claimed_at,
            timeout_at=timeout_at,
            max_run_seconds=max_run_seconds,
            progress_callback=progress_callback,
            queued_at=queued_at,
            options=options,
        )
