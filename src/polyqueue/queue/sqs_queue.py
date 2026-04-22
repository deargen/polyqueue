"""SqsJobQueue — SQS-backed implementation of the JobQueue protocol.

Receipt == SQS ReceiptHandle (opaque to callers).

Lease semantics:
  SQS visibility timeout replaces the Redis lease model.
  A heartbeat task extends the visibility timeout every heartbeat_interval_seconds.
  If the worker crashes, the message re-appears automatically after the visibility
  timeout expires — no reaper needed.

Retry backoff:
  fail(retryable=True) deletes the in-flight message and re-sends with DelaySeconds
  for precise control. SQS native retry/DLQ is NOT used for business retries so
  that backoff timing is consistent across all backends.

Settlement order invariant:
  DB state is committed *before* the SQS message is deleted. This ensures Postgres
  is the authoritative record. If SQS delete subsequently fails and the message is
  redelivered, claim()'s status guard will skip the already-settled job.

Postgres stays source of truth: attempt_count is incremented on claim() here,
not read from SQS ApproximateReceiveCount.

Maintenance:
  reap_abandoned_leases() and move_due_retries() are no-ops — SQS handles re-delivery.
  acquire_maintenance_lock() always returns False so the worker skips maintenance loops.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import TYPE_CHECKING, Any

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


class SqsJobQueue:
    """
    Async SQS job queue (wraps the synchronous boto3 SQS client in a thread).

    Args:
        queue_url: Full SQS queue URL.
        session_factory: SQLAlchemy async session factory for Postgres state updates.
        region: AWS region (used to create the boto3 client).
        visibility_timeout_seconds: Initial visibility timeout on receive.
            Must be > heartbeat_interval_seconds so renewal has time to run.
        heartbeat_interval_seconds: How often to extend the visibility timeout.
        max_attempts: Default max attempts copied to DB at enqueue.
        retry_backoff_seconds: Per-attempt backoff list (index 0 = first attempt).
    """

    def __init__(
        self,
        queue_url: str,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        table_name: str = "polyqueue_jobs",
        table_schema: str = "polyqueue",
        region: str = "us-east-1",
        visibility_timeout_seconds: int = 660,
        heartbeat_interval_seconds: int = 120,
        max_attempts: int = 3,
        retry_backoff_seconds: list[int] | None = None,
        default_max_run_seconds: int | None = None,
        default_timeout_strategy: str = "retry",
        metrics: MetricsHook | None = None,
    ) -> None:
        import boto3

        self._queue_url = queue_url
        self.session_factory = session_factory
        self._table = f"{table_schema}.{table_name}"
        self._attempts_table = f"{table_schema}.{table_name}_attempts"
        self._visibility_timeout = visibility_timeout_seconds
        self._heartbeat_interval = heartbeat_interval_seconds
        self._max_attempts = max_attempts
        self._retry_backoff = retry_backoff_seconds or [10, 60, 300]
        self._default_max_run_seconds = default_max_run_seconds
        self._default_timeout_strategy = default_timeout_strategy
        self._metrics: MetricsHook = metrics or NoOpMetrics()
        self._sqs = boto3.client("sqs", region_name=region)
        # in-flight deliveries keyed by receipt_handle
        # value is (job_id, lease_token) so ack()/fail() can predicate DB updates
        self._in_flight: dict[str, tuple[str, UUID]] = {}
        self._heartbeat_tasks: dict[str, asyncio.Task[None]] = {}
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
        """Insert Postgres row first, then send job_id to SQS."""
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
            await asyncio.to_thread(
                self._sqs.send_message,
                QueueUrl=self._queue_url,
                MessageBody=job_id,
            )
        except Exception as exc:
            logger.error("enqueue: SQS send failed for job %s: %s", job_id, exc)
            async with self.session_factory() as session:
                await mark_enqueue_failed(
                    session, table=self._table, job_id=job_id, error=str(exc)
                )
                await session.commit()
            raise
        self._metrics.job_enqueued(job_type)
        return EnqueueResult(job_id=job_id)

    async def claim(self) -> ClaimedJob | None:
        """Receive one message from SQS (short poll, 1s wait).

        On success: update Postgres to processing, start visibility heartbeat.
        Skips messages whose Postgres row is already in a terminal state.
        """
        response = await asyncio.to_thread(
            self._sqs.receive_message,
            QueueUrl=self._queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=1,
            VisibilityTimeout=self._visibility_timeout,
        )
        messages = response.get("Messages", [])
        if not messages:
            return None

        msg = messages[0]
        job_id: str = msg["Body"]
        receipt_handle: str = msg["ReceiptHandle"]

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
                queue_name=self._queue_url,
            )
            await session.commit()

        if record is None:
            logger.warning(
                "claim: job %s received from SQS but not in a claimable state in Postgres — deleting",
                job_id,
            )
            await self._delete_message(receipt_handle)
            return None

        self._in_flight[receipt_handle] = (job_id, record.lease_token)
        task = asyncio.create_task(self._heartbeat(receipt_handle))
        self._heartbeat_tasks[receipt_handle] = task

        return ClaimedJob(
            receipt=receipt_handle,
            job_id=job_id,
            job_type=record.job_type,
            payload=record.payload,
            attempt=record.attempt_count,
            max_attempts=record.max_attempts,
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
        """Delete the SQS message. DB was already marked succeeded by the worker.

        If delete fails and message is redelivered, claim() status guard skips it.
        """
        receipt_handle = receipt
        self._cancel_heartbeat(receipt_handle)
        entry = self._in_flight.pop(receipt_handle, None)

        if entry is None:
            logger.warning(
                "ack: no in-flight record for receipt %s...", receipt_handle[:20]
            )
            return

        try:
            await self._delete_message(receipt_handle)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "ack: SQS delete failed for receipt %s... — "
                "any redelivery will be skipped by claim() status guard: %s",
                receipt_handle[:20],
                exc,
            )

    async def fail(self, receipt: str, error: str, *, retryable: bool = True) -> None:
        """Settle the job. DB is updated first; SQS message is deleted after.

        If retryable: reset Postgres to queued (DB first), then delete + re-send with backoff.
        If terminal: leave Postgres in whatever terminal state the worker already set,
        then delete the SQS message.
        """
        receipt_handle = receipt
        self._cancel_heartbeat(receipt_handle)
        entry = self._in_flight.pop(receipt_handle, None)

        if entry is None:
            logger.warning(
                "fail: no in-flight record for receipt %s...", receipt_handle[:20]
            )
            return
        job_id, lease_token = entry

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
                    queue_name=self._queue_url,
                )
                await session.commit()

            if attempt is None:
                logger.warning(
                    "fail: job %s was not in processing state; "
                    "skipping retry — letting visibility timeout handle redelivery",
                    job_id,
                )
                return

            # DB committed. Now delete the in-flight message and reschedule.
            await self._delete_message(receipt_handle)
            backoff = self._backoff_for_attempt(attempt)
            try:
                await self.reschedule(job_id, backoff_seconds=backoff)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "fail: reschedule failed for job %s — "
                    "DB is already reset to queued; SQS visibility timeout will redeliver: %s",
                    job_id,
                    exc,
                )
        else:
            # Terminal: DB was already updated by the worker. Just delete the message.
            await self._delete_message(receipt_handle)

    async def reschedule(self, job_id: str, backoff_seconds: int) -> None:
        """Re-send a job_id to SQS with a delay."""
        if backoff_seconds > 900:
            logger.warning(
                "SQS reschedule: backoff %ds exceeds SQS max of 900s — clamping for job %s",
                backoff_seconds,
                job_id,
            )
        delay = min(backoff_seconds, 900)  # SQS max DelaySeconds = 900
        await asyncio.to_thread(
            self._sqs.send_message,
            QueueUrl=self._queue_url,
            MessageBody=job_id,
            DelaySeconds=delay,
        )
        logger.info("SQS retry scheduled: job %s re-enters queue in %ds", job_id, delay)

    # ──────────────────────────────────────────────
    # Maintenance — no-ops for SQS
    # ──────────────────────────────────────────────

    async def close(self) -> None:
        """Cancel all heartbeat tasks."""
        for handle in list(self._heartbeat_tasks):
            self._cancel_heartbeat(handle)

    async def reap_abandoned_leases(self) -> None:
        """No-op: SQS visibility timeout handles re-delivery automatically."""

    async def move_due_retries(self) -> None:
        """No-op: SQS DelaySeconds handles retry scheduling."""

    async def acquire_maintenance_lock(self, ttl_seconds: int = 90) -> bool:
        """No-op: no maintenance loops needed for SQS."""
        return False

    async def release_maintenance_lock(self) -> None:
        pass

    # ──────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────

    async def _delete_message(self, receipt_handle: str) -> None:
        """Delete a message, ignoring stale-handle errors but re-raising transport failures."""
        try:
            await asyncio.to_thread(
                self._sqs.delete_message,
                QueueUrl=self._queue_url,
                ReceiptHandle=receipt_handle,
            )
        except Exception as exc:
            exc_str = str(exc)
            if (
                "ReceiptHandleIsInvalid" in exc_str
                or "InvalidParameterValue" in exc_str
            ):
                logger.debug(
                    "SQS delete_message: stale handle for receipt %s...",
                    receipt_handle[:20],
                )
                return
            raise

    async def _heartbeat(self, receipt_handle: str) -> None:
        """Periodically extend the SQS visibility timeout while the job is running."""
        try:
            while True:
                await asyncio.sleep(self._heartbeat_interval)
                try:
                    await asyncio.to_thread(
                        self._sqs.change_message_visibility,
                        QueueUrl=self._queue_url,
                        ReceiptHandle=receipt_handle,
                        VisibilityTimeout=self._visibility_timeout,
                    )
                    logger.debug(
                        "SQS heartbeat: extended visibility for receipt %s...",
                        receipt_handle[:20],
                    )
                except Exception as exc:  # noqa: BLE001 — heartbeat failures are non-fatal; log and continue
                    logger.warning(
                        "SQS heartbeat: failed to extend visibility for receipt %s...: %s",
                        receipt_handle[:20],
                        exc,
                    )
        except asyncio.CancelledError:
            pass

    def _cancel_heartbeat(self, receipt_handle: str) -> None:
        task = self._heartbeat_tasks.pop(receipt_handle, None)
        if task is not None:
            task.cancel()

    def _backoff_for_attempt(self, attempt: int) -> int:
        idx = max(0, attempt - 1)
        if idx < len(self._retry_backoff):
            return self._retry_backoff[idx]
        return self._retry_backoff[-1]
