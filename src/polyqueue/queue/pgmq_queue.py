"""PgmqJobQueue — PGMQ-backed implementation of the JobQueue protocol.

Receipt semantics:
  Receipt is the PGMQ message id (msg_id) encoded as str.

Lease semantics:
  PGMQ visibility timeout replaces Redis lease keys. claim() reads with a VT,
  and a heartbeat task periodically extends VT via pgmq.set_vt() while the job
  is processing.

Retry backoff:
  fail(retryable=True) resets Postgres to queued first, then updates message VT
  forward by the computed backoff via pgmq.set_vt().

Settlement order invariant:
  DB state is committed *before* broker settlement (delete/archive). If broker
  settlement fails and message is re-delivered, claim()'s status guard skips it.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import TYPE_CHECKING, Any

from sqlalchemy import text

from polyqueue.enqueue_options import EnqueueOptions, EnqueueResult
from polyqueue.metrics import MetricsHook, NoOpMetrics
from polyqueue.queue.db import (
    claim_job,
    insert_job,
    mark_enqueue_failed,
    reset_for_retry,
)
from polyqueue.queue.interface import ClaimedJob
from polyqueue.utils.payload import normalize_payload

if TYPE_CHECKING:
    from collections.abc import Callable
    from uuid import UUID

    from pydantic import BaseModel
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from polyqueue.worker.identity import WorkerInfo

logger = logging.getLogger(__name__)


class PgmqJobQueue:
    """Async PGMQ job queue via SQL calls over SQLAlchemy + asyncpg."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        table_name: str = "polyqueue_jobs",
        table_schema: str = "polyqueue",
        queue_name: str = "polyqueue",
        visibility_timeout_seconds: int = 660,
        heartbeat_interval_seconds: int = 120,
        poll_seconds: int = 5,
        poll_interval_ms: int = 100,
        create_queue_if_missing: bool = True,
        use_unlogged_queue: bool = False,
        archive_on_success: bool = False,
        archive_on_terminal_fail: bool = False,
        max_attempts: int = 3,
        retry_backoff_seconds: list[int] | None = None,
        default_max_run_seconds: int | None = None,
        default_timeout_strategy: str = "retry",
        metrics: MetricsHook | None = None,
    ) -> None:
        self.session_factory = session_factory
        self._table = f"{table_schema}.{table_name}"
        self._attempts_table = f"{table_schema}.{table_name}_attempts"
        self._queue_name = queue_name
        self._visibility_timeout = visibility_timeout_seconds
        self._heartbeat_interval = heartbeat_interval_seconds
        self._poll_seconds = poll_seconds
        self._poll_interval_ms = poll_interval_ms
        self._create_queue_if_missing = create_queue_if_missing
        self._use_unlogged_queue = use_unlogged_queue
        self._archive_on_success = archive_on_success
        self._archive_on_terminal_fail = archive_on_terminal_fail
        self._max_attempts = max_attempts
        self._retry_backoff = retry_backoff_seconds or [10, 60, 300]
        self._default_max_run_seconds = default_max_run_seconds
        self._default_timeout_strategy = default_timeout_strategy
        self._metrics: MetricsHook = metrics or NoOpMetrics()

        self._worker_info: WorkerInfo | None = None
        # receipt → (job_id, lease_token)
        self._in_flight: dict[str, tuple[str, UUID]] = {}
        self._heartbeat_tasks: dict[str, asyncio.Task[None]] = {}
        self._queue_ready = False
        self._queue_ready_lock = asyncio.Lock()

    def set_worker_info(self, worker_info: WorkerInfo) -> None:
        self._worker_info = worker_info

    async def enqueue(
        self,
        target: str | Callable[..., Any],
        payload: dict[str, Any] | BaseModel,
        *,
        options: EnqueueOptions | None = None,
    ) -> EnqueueResult:
        """Insert Postgres row first, then send a tiny job_id payload to PGMQ."""
        if not await self._ensure_queue():
            raise ValueError(
                f"PGMQ queue {self._queue_name!r} does not exist and "
                "pgmq_create_queue_if_missing is disabled"
            )

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
            async with self.session_factory() as session:
                _ = await session.execute(
                    text(
                        """
                        SELECT pgmq.send(
                            :queue_name,
                            CAST(:message AS jsonb),
                            0
                        )
                        """
                    ),
                    {
                        "queue_name": self._queue_name,
                        "message": json.dumps({"job_id": job_id}),
                    },
                )
                await session.commit()
        except Exception as exc:
            logger.error("enqueue: PGMQ send failed for job %s: %s", job_id, exc)
            async with self.session_factory() as session:
                await mark_enqueue_failed(
                    session, table=self._table, job_id=job_id, error=str(exc)
                )
                await session.commit()
            raise

        self._metrics.job_enqueued(job_type)
        return EnqueueResult(job_id=job_id)

    async def claim(self) -> ClaimedJob | None:
        """Read one message from PGMQ with poll + visibility timeout."""
        if not await self._ensure_queue():
            return None

        async with self.session_factory() as session:
            row = await session.execute(
                text(
                    """
                    SELECT * FROM pgmq.read_with_poll(
                        :queue_name,
                        :vt_seconds,
                        1,
                        :max_poll_seconds,
                        :poll_interval_ms
                    )
                    """
                ),
                {
                    "queue_name": self._queue_name,
                    "vt_seconds": self._visibility_timeout,
                    "max_poll_seconds": self._poll_seconds,
                    "poll_interval_ms": self._poll_interval_ms,
                },
            )
            record = row.mappings().one_or_none()
            await session.commit()

        if record is None:
            return None

        msg_id = record.get("msg_id")
        raw_message = record.get("message")
        if not isinstance(msg_id, int):
            logger.warning("claim: invalid PGMQ msg_id=%r, dropping message", msg_id)
            return None

        message: dict[str, Any]
        if isinstance(raw_message, dict):
            message = raw_message
        elif isinstance(raw_message, str):
            try:
                parsed = json.loads(raw_message)
            except json.JSONDecodeError:
                logger.warning(
                    "claim: invalid PGMQ message JSON for msg_id=%s, deleting", msg_id
                )
                await self._delete_message(msg_id)
                return None
            if not isinstance(parsed, dict):
                logger.warning(
                    "claim: non-object PGMQ message for msg_id=%s, deleting", msg_id
                )
                await self._delete_message(msg_id)
                return None
            message = parsed
        else:
            logger.warning(
                "claim: unsupported PGMQ message type=%s for msg_id=%s, deleting",
                type(raw_message).__name__,
                msg_id,
            )
            await self._delete_message(msg_id)
            return None

        job_id = message.get("job_id")
        if not isinstance(job_id, str):
            logger.warning("claim: missing job_id in PGMQ message %s, deleting", msg_id)
            await self._delete_message(msg_id)
            return None

        wi = self._worker_info
        async with self.session_factory() as session:
            db_record = await claim_job(
                session,
                table=self._table,
                attempts_table=self._attempts_table,
                job_id=job_id,
                worker_id=wi.worker_id if wi else "",
                worker_hostname=wi.hostname if wi else "",
                worker_pid=wi.pid if wi else 0,
                queue_name=self._queue_name,
            )
            await session.commit()

        if db_record is None:
            logger.warning(
                "claim: job %s received from PGMQ but not in claimable Postgres state — deleting",
                job_id,
            )
            await self._delete_message(msg_id)
            return None

        receipt = str(msg_id)
        self._in_flight[receipt] = (job_id, db_record.lease_token)
        self._heartbeat_tasks[receipt] = asyncio.create_task(self._heartbeat(msg_id))

        return ClaimedJob(
            receipt=receipt,
            job_id=job_id,
            job_type=db_record.job_type,
            payload=db_record.payload,
            attempt=db_record.attempt_count,
            max_attempts=db_record.max_attempts,
            worker_id=db_record.claimed_by_worker_id or "",
            worker_hostname=db_record.claimed_by_hostname or "",
            worker_pid=db_record.claimed_by_pid or 0,
            claimed_at=db_record.claimed_at,
            queued_at=db_record.created_at,
            max_run_seconds=db_record.max_run_seconds,
            timeout_strategy=db_record.timeout_strategy,
            timeout_at=db_record.timeout_at,
            lease_token=db_record.lease_token,
        )

    async def ack(self, receipt: str) -> None:
        """Delete/archive the message. DB was already marked succeeded by worker."""
        self._cancel_heartbeat(receipt)
        _ = self._in_flight.pop(receipt, None)
        msg_id = self._parse_receipt(receipt)
        if msg_id is None:
            logger.warning("ack: invalid receipt %r", receipt)
            return

        if self._archive_on_success:
            await self._archive_message(msg_id)
        else:
            await self._delete_message(msg_id)

    async def fail(self, receipt: str, error: str, *, retryable: bool = True) -> None:
        """Settle PGMQ delivery after DB state update logic."""
        self._cancel_heartbeat(receipt)
        entry = self._in_flight.pop(receipt, None)
        if entry is None:
            logger.warning("fail: no in-flight record for receipt %s", receipt)
            return
        job_id, lease_token = entry

        msg_id = self._parse_receipt(receipt)
        if msg_id is None:
            logger.warning("fail: invalid receipt %r", receipt)
            return

        if retryable:
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
                    queue_name=self._queue_name,
                )
                await session.commit()

            if attempt is None:
                logger.warning(
                    "fail: job %s was not in processing state; skipping retry", job_id
                )
                return

            backoff = self._backoff_for_attempt(attempt)
            await self._set_vt(msg_id, backoff)
            return

        if self._archive_on_terminal_fail:
            await self._archive_message(msg_id)
        else:
            await self._delete_message(msg_id)

    async def reap_abandoned_leases(self) -> None:
        """No-op: PGMQ visibility timeout handles redelivery."""

    async def move_due_retries(self) -> None:
        """No-op: retries use set_vt on the same broker message."""

    async def acquire_maintenance_lock(self, ttl_seconds: int = 90) -> bool:
        _ = ttl_seconds
        return False

    async def release_maintenance_lock(self) -> None:
        pass

    async def close(self) -> None:
        for receipt in list(self._heartbeat_tasks):
            self._cancel_heartbeat(receipt)

    async def _ensure_queue(self) -> bool:
        if self._queue_ready:
            return True

        async with self._queue_ready_lock:
            if self._queue_ready:
                return True
            exists = await self._queue_exists()
            if exists:
                self._queue_ready = True
                return True
            if not self._create_queue_if_missing:
                logger.warning(
                    "PGMQ queue %s not found and create_queue_if_missing is disabled",
                    self._queue_name,
                )
                return False
            await self._create_queue()
            self._queue_ready = True
            return True

    async def _queue_exists(self) -> bool:
        async with self.session_factory() as session:
            rows = await session.execute(text("SELECT * FROM pgmq.list_queues()"))
            for mapping in rows.mappings():
                candidate = (
                    mapping.get("queue_name")
                    or mapping.get("queue")
                    or mapping.get("name")
                )
                if candidate == self._queue_name:
                    return True
        return False

    async def _create_queue(self) -> None:
        fn = "pgmq.create_unlogged" if self._use_unlogged_queue else "pgmq.create"
        async with self.session_factory() as session:
            _ = await session.execute(
                text(f"SELECT {fn}(:queue_name)"),
                {"queue_name": self._queue_name},
            )
            await session.commit()

    async def _delete_message(self, msg_id: int) -> None:
        async with self.session_factory() as session:
            _ = await session.execute(
                text(
                    "SELECT pgmq.delete(CAST(:queue_name AS text), CAST(:msg_id AS bigint))"
                ),
                {"queue_name": self._queue_name, "msg_id": msg_id},
            )
            await session.commit()

    async def _archive_message(self, msg_id: int) -> None:
        async with self.session_factory() as session:
            _ = await session.execute(
                text(
                    "SELECT pgmq.archive(CAST(:queue_name AS text), CAST(:msg_id AS bigint))"
                ),
                {"queue_name": self._queue_name, "msg_id": msg_id},
            )
            await session.commit()

    async def _set_vt(self, msg_id: int, vt_seconds: int) -> None:
        async with self.session_factory() as session:
            _ = await session.execute(
                text(
                    "SELECT pgmq.set_vt(CAST(:queue_name AS text), "
                    "CAST(:msg_id AS bigint), CAST(:vt_seconds AS integer))"
                ),
                {
                    "queue_name": self._queue_name,
                    "msg_id": msg_id,
                    "vt_seconds": vt_seconds,
                },
            )
            await session.commit()

    async def _heartbeat(self, msg_id: int) -> None:
        try:
            while True:
                await asyncio.sleep(self._heartbeat_interval)
                try:
                    await self._set_vt(msg_id, self._visibility_timeout)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "PGMQ heartbeat: failed to extend VT for msg_id=%s: %s",
                        msg_id,
                        exc,
                    )
        except asyncio.CancelledError:
            pass

    def _cancel_heartbeat(self, receipt: str) -> None:
        task = self._heartbeat_tasks.pop(receipt, None)
        if task is not None:
            task.cancel()

    def _parse_receipt(self, receipt: str) -> int | None:
        try:
            return int(receipt)
        except ValueError:
            return None

    def _backoff_for_attempt(self, attempt: int) -> int:
        idx = max(0, attempt - 1)
        if idx < len(self._retry_backoff):
            return self._retry_backoff[idx]
        return self._retry_backoff[-1]
