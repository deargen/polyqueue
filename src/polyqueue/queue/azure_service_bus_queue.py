"""AzureServiceBusJobQueue — Azure Service Bus backed implementation of the JobQueue protocol.

Receipt == str(msg.lock_token) — an opaque per-delivery token, not job_id.
ServiceBusReceivedMessage never leaves this file.

Receipt semantics:
  Each delivery gets a fresh lock_token (UUID). This is the delivery identity, not the
  business identity. If the same job is re-delivered after lock expiry, it gets a new
  lock_token and a new receipt. job_id is business identity and lives in ClaimedJob.job_id.

Lease semantics:
  AutoLockRenewer (azure-servicebus 7.x async) replaces the Redis lease model.
  If the worker crashes the lock expires and Azure re-delivers the message.

Retry backoff:
  fail(retryable=True) completes the message (to prevent DLQ count exhaustion) and
  schedules a new send with a future enqueue time for precise backoff control.

Settlement order invariant:
  DB state is committed *before* the Azure Service Bus message is completed. This
  ensures Postgres is the authoritative record. If the broker complete subsequently
  fails, any redelivery will be skipped by claim()'s status guard.

Postgres stays source of truth: attempt_count is incremented on claim() here.

Maintenance:
  reap_abandoned_leases() and move_due_retries() are no-ops.
  acquire_maintenance_lock() always returns False.

Install:
  pip install polyqueue[azure-service-bus]
"""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime, timedelta
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

    from azure.servicebus.aio import (
        AutoLockRenewer,
        ServiceBusReceiver,
        ServiceBusSender,
    )
    from azure.servicebus.aio._async_message import ServiceBusReceivedMessage
    from pydantic import BaseModel
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from polyqueue.worker.identity import WorkerInfo

logger = logging.getLogger(__name__)


def _decode_message_body(msg: ServiceBusReceivedMessage) -> str:
    """Extract the UTF-8 job_id string from an Azure Service Bus message body.

    The SDK may return bytes directly or an AMQP data section (iterable of bytes
    chunks). Raises ValueError on decode failure to allow the caller to complete
    the message and log a terminal error rather than silently crashing.
    """
    raw = msg.body
    try:
        if isinstance(raw, bytes):
            return raw.decode()
        return b"".join(raw).decode()
    except Exception as exc:
        raise ValueError(
            f"Azure SB message body could not be decoded as UTF-8 job_id: {exc!r}. "
            f"Raw body type: {type(raw).__name__}"
        ) from exc


class AzureServiceBusJobQueue:
    """
    Async Azure Service Bus job queue.

    Args:
        connection_string: Azure Service Bus connection string.
        queue_name: Service Bus queue name.
        session_factory: SQLAlchemy async session factory for Postgres state updates.
        max_attempts: Default max attempts copied to DB at enqueue.
        retry_backoff_seconds: Per-attempt backoff list (index 0 = first attempt).
        max_lock_renewal_duration_seconds: Upper bound on AutoLockRenewer duration (default 1h).
            Set lower to bound how long a wedged handler can hold a message.
    """

    def __init__(
        self,
        connection_string: str,
        queue_name: str,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        table_name: str = "polyqueue_jobs",
        table_schema: str = "polyqueue",
        max_attempts: int = 3,
        retry_backoff_seconds: list[int] | None = None,
        max_lock_renewal_duration_seconds: int = 3600,
        default_max_run_seconds: int | None = None,
        default_timeout_strategy: str = "retry",
        metrics: MetricsHook | None = None,
    ) -> None:
        try:
            from azure.servicebus.aio import ServiceBusClient
        except ImportError as exc:
            raise ImportError(
                "Azure Service Bus support is not installed. "
                "Install with: pip install polyqueue[azure-service-bus]"
            ) from exc

        self._connection_string = connection_string
        self._queue_name = queue_name
        self.session_factory = session_factory
        self._table = f"{table_schema}.{table_name}"
        self._attempts_table = f"{table_schema}.{table_name}_attempts"
        self._max_attempts = max_attempts
        self._retry_backoff = retry_backoff_seconds or [10, 60, 300]
        self._max_lock_renewal_duration = max_lock_renewal_duration_seconds
        self._default_max_run_seconds = default_max_run_seconds
        self._default_timeout_strategy = default_timeout_strategy
        self._metrics: MetricsHook = metrics or NoOpMetrics()

        self._client = ServiceBusClient.from_connection_string(connection_string)
        self._sender: ServiceBusSender | None = None
        self._receiver: ServiceBusReceiver | None = None
        self._lock_renewer: AutoLockRenewer | None = None

        # in-flight deliveries keyed by lock_token (receipt)
        # value is (job_id, message, lease_token) for fail()/ack() DB updates
        self._in_flight: dict[
            str, tuple[str, ServiceBusReceivedMessage, UUID]
        ] = {}
        self._worker_info: WorkerInfo | None = None

    async def _ensure_open(self) -> None:
        if self._sender is None:
            self._sender = self._client.get_queue_sender(queue_name=self._queue_name)
            await self._sender.__aenter__()
        if self._receiver is None:
            from azure.servicebus import ServiceBusReceiveMode

            self._receiver = self._client.get_queue_receiver(
                queue_name=self._queue_name,
                receive_mode=ServiceBusReceiveMode.PEEK_LOCK,
            )
            await self._receiver.__aenter__()
        if self._lock_renewer is None:
            from azure.servicebus.aio import AutoLockRenewer

            self._lock_renewer = AutoLockRenewer()

    async def close(self) -> None:
        if self._lock_renewer is not None:
            await self._lock_renewer.close()
        if self._sender is not None:
            await self._sender.__aexit__(None, None, None)
        if self._receiver is not None:
            await self._receiver.__aexit__(None, None, None)
        # Use the public close() API instead of __aexit__ — the client was never
        # entered as a context manager, so calling __aexit__ on it is incorrect.
        await self._client.close()

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
        """Insert Postgres row first, then send job_id to Azure Service Bus."""
        from azure.servicebus import ServiceBusMessage

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
            await self._ensure_open()
            assert self._sender is not None
            await self._sender.send_messages(ServiceBusMessage(job_id))
        except Exception as exc:
            logger.error("enqueue: Azure SB send failed for job %s: %s", job_id, exc)
            async with self.session_factory() as session:
                await mark_enqueue_failed(
                    session, table=self._table, job_id=job_id, error=str(exc)
                )
                await session.commit()
            raise
        self._metrics.job_enqueued(job_type)
        return EnqueueResult(job_id=job_id)

    async def claim(self) -> ClaimedJob | None:
        """Receive one message from Azure Service Bus (max_wait_time=1s).

        Receipt is str(msg.lock_token) — delivery identity, not job_id.
        On success: register AutoLockRenewer, update Postgres to processing.
        """
        await self._ensure_open()
        assert self._receiver is not None
        assert self._lock_renewer is not None

        messages = await self._receiver.receive_messages(
            max_message_count=1, max_wait_time=1
        )
        if not messages:
            return None

        msg = messages[0]
        try:
            job_id: str = _decode_message_body(msg)
        except ValueError:
            logger.error(
                "claim: received Azure SB message with undecodable body — completing to prevent re-delivery loop"
            )
            await self._receiver.complete_message(msg)
            return None
        lock_token: str = str(msg.lock_token)  # delivery identity

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
                queue_name=self._queue_name,
            )
            await session.commit()

        if record is None:
            logger.warning(
                "claim: job %s received from Azure SB but not in claimable Postgres state — completing",
                job_id,
            )
            await self._receiver.complete_message(msg)
            return None

        # Register lock renewal; AutoLockRenewer will renew until we complete/abandon
        self._lock_renewer.register(
            self._receiver,
            msg,
            max_lock_renewal_duration=self._max_lock_renewal_duration,
        )
        self._in_flight[lock_token] = (job_id, msg, record.lease_token)

        return ClaimedJob(
            receipt=lock_token,
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
        """Complete the Azure SB message. DB was already marked succeeded by the worker.

        If complete fails, redelivery skips by claim() status guard.
        """
        lock_token = receipt
        entry = self._in_flight.pop(lock_token, None)
        if entry is None:
            logger.warning("ack: no in-flight message for lock_token %s", lock_token)
            return

        job_id, msg, _lease_token = entry

        await self._ensure_open()
        assert self._receiver is not None
        try:
            await self._complete_message(msg)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "ack: Azure SB complete failed for job %s — "
                "any redelivery will be skipped by claim() status guard: %s",
                job_id,
                exc,
            )

    async def fail(self, receipt: str, error: str, *, retryable: bool = True) -> None:
        """Settle the job. DB is updated first; Azure SB message is completed after.

        If retryable: reset Postgres to queued (DB first), then complete the message
        (to avoid DLQ count exhaustion) and schedule a new delivery with backoff.
        If terminal: leave Postgres in whatever terminal state the worker already set,
        then complete the message.
        """
        lock_token = receipt
        entry = self._in_flight.pop(lock_token, None)
        if entry is None:
            logger.warning("fail: no in-flight message for lock_token %s", lock_token)
            return

        job_id, msg, lease_token = entry

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
                    "fail: job %s was not in processing state; "
                    "skipping retry — letting lock expiry handle redelivery",
                    job_id,
                )
                return

            # DB committed. Now complete the message and reschedule.
            await self._ensure_open()
            assert self._receiver is not None
            # Always complete (not abandon) to keep Azure DLQ delivery count predictable
            try:
                await self._complete_message(msg)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "fail: Azure SB complete failed for job %s — "
                    "DB is already reset to queued; lock expiry will redeliver: %s",
                    job_id,
                    exc,
                )
                return

            backoff = self._backoff_for_attempt(attempt)
            try:
                await self.reschedule(job_id, backoff_seconds=backoff)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "fail: reschedule failed for job %s — "
                    "DB is already reset to queued; lock expiry will redeliver: %s",
                    job_id,
                    exc,
                )
        else:
            # Terminal: DB was already updated by the worker. Complete the message.
            await self._ensure_open()
            assert self._receiver is not None
            try:
                await self._complete_message(msg)
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "fail: Azure SB complete failed (terminal) for job %s — "
                    "lock expiry will redeliver; claim() will skip: %s",
                    job_id,
                    exc,
                )

    async def reschedule(self, job_id: str, backoff_seconds: int) -> None:
        """Schedule job_id back onto the queue with a future enqueue time."""
        from azure.servicebus import ServiceBusMessage

        await self._ensure_open()
        assert self._sender is not None
        enqueue_at = datetime.now(UTC) + timedelta(seconds=backoff_seconds)
        await self._sender.schedule_messages(ServiceBusMessage(job_id), enqueue_at)
        logger.info(
            "Azure SB retry scheduled: job %s re-enters queue in %ds",
            job_id,
            backoff_seconds,
        )

    # ──────────────────────────────────────────────
    # Maintenance — no-ops for Azure Service Bus
    # ──────────────────────────────────────────────

    async def reap_abandoned_leases(self) -> None:
        """No-op: Azure Service Bus lock expiry handles re-delivery automatically."""

    async def move_due_retries(self) -> None:
        """No-op: schedule_messages handles retry scheduling."""

    async def acquire_maintenance_lock(self, ttl_seconds: int = 90) -> bool:
        """No-op: no maintenance loops needed for Azure Service Bus."""
        return False

    async def release_maintenance_lock(self) -> None:
        pass

    # ──────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────

    async def _complete_message(self, msg: ServiceBusReceivedMessage) -> None:
        """Complete a message, ignoring already-settled errors but re-raising transport failures."""
        assert self._receiver is not None
        try:
            await self._receiver.complete_message(msg)
        except Exception as exc:
            exc_type = type(exc).__name__
            exc_str = str(exc).lower()
            if (
                "alreadysettled" in exc_type.lower()
                or ("already" in exc_str and "settled" in exc_str)
                or ("lock" in exc_str and "expired" in exc_str)
            ):
                logger.debug(
                    "Azure SB complete_message: already-settled message ignored: %s",
                    exc,
                )
                return
            raise

    def _backoff_for_attempt(self, attempt: int) -> int:
        idx = max(0, attempt - 1)
        if idx < len(self._retry_backoff):
            return self._retry_backoff[idx]
        return self._retry_backoff[-1]
