"""Tests for AzureServiceBusJobQueue using the official Azure Service Bus emulator.

Start before running:
    podman run -d --rm --name azuresb-test \
        -e ACCEPT_EULA=Y \
        -p 5672:5672 \
        mcr.microsoft.com/azure-messaging/servicebus-emulator:latest

The emulator uses AMQP on port 5672. Connection string format for the emulator:
    Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;
    SharedAccessKey=SAS_KEY_VALUE=;UseDevelopmentEmulator=true;

Queue name must be pre-created in the emulator config or via management API.
"""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio

from polyqueue.enqueue_options import EnqueueOptions

from .conftest import AZURE_SB_CONN

QUEUE_NAME = "polyqueue-test"


def _can_import_azure() -> bool:
    try:
        import azure.servicebus  # noqa: F401
    except ImportError:
        return False
    else:
        return True


async def _drain_queue(q) -> None:
    """Receive and complete all pending messages to reset queue state between tests."""
    assert q._receiver is not None
    while True:
        msgs = await q._receiver.receive_messages(max_message_count=10, max_wait_time=1)
        if not msgs:
            break
        for msg in msgs:
            await q._receiver.complete_message(msg)


@pytest_asyncio.fixture
async def queue(session_factory):
    if not _can_import_azure():
        pytest.skip("azure-servicebus not installed")

    from polyqueue.queue.azure_service_bus_queue import AzureServiceBusJobQueue

    try:
        q = AzureServiceBusJobQueue(
            AZURE_SB_CONN,
            QUEUE_NAME,
            session_factory,
            max_attempts=3,
            retry_backoff_seconds=[1, 2, 5],
        )
        # Probe connectivity and drain any leftover messages from previous tests
        await q._ensure_open()
        await _drain_queue(q)
    except Exception:
        pytest.skip("Azure Service Bus emulator not reachable")

    yield q
    await _drain_queue(q)
    await q.close()


# ──────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────


async def test_enqueue_and_claim(queue, session_factory):
    job_id = str(uuid.uuid4())
    await queue.enqueue(
        "test_job", {"key": "value"}, options=EnqueueOptions(job_id=job_id)
    )

    job = await queue.claim()
    assert job is not None
    assert job.job_id == job_id
    assert job.job_type == "test_job"
    assert job.payload == {"key": "value"}
    assert job.attempt == 1


async def test_claim_returns_none_when_empty(queue):
    job = await queue.claim()
    assert job is None


async def test_ack_does_not_touch_db(queue, session_factory):
    """ack() completes the Azure SB message; DB status is NOT changed by ack()."""
    from sqlalchemy import text

    job_id = str(uuid.uuid4())
    await queue.enqueue("test_job", {}, options=EnqueueOptions(job_id=job_id))
    job = await queue.claim()
    assert job is not None

    await queue.ack(job.receipt)

    async with session_factory() as session:
        row = await session.execute(
            text("SELECT status FROM jobs WHERE id = :id"), {"id": job_id}
        )
        record = row.mappings().one()
    assert record["status"] == "processing"  # ack() does NOT touch the DB


async def test_fail_retryable_reschedules(queue, session_factory):
    import asyncio

    job_id = str(uuid.uuid4())
    await queue.enqueue("test_job", {}, options=EnqueueOptions(job_id=job_id))
    job = await queue.claim()
    assert job is not None

    await queue.fail(job.receipt, "transient error", retryable=True)

    # schedule_messages uses a future enqueue time of backoff_seconds=1
    await asyncio.sleep(2)
    job2 = await queue.claim()
    assert job2 is not None
    assert job2.job_id == job_id
    assert job2.attempt == 2


async def test_fail_terminal_no_reschedule(queue, session_factory):
    import asyncio

    job_id = str(uuid.uuid4())
    await queue.enqueue("test_job", {}, options=EnqueueOptions(job_id=job_id))
    job = await queue.claim()
    assert job is not None

    await queue.fail(job.receipt, "fatal error", retryable=False)

    await asyncio.sleep(1)
    job2 = await queue.claim()
    assert job2 is None


async def test_ack_idempotent(queue, session_factory):
    job_id = str(uuid.uuid4())
    await queue.enqueue("test_job", {}, options=EnqueueOptions(job_id=job_id))
    job = await queue.claim()
    assert job is not None

    await queue.ack(job.receipt)
    await queue.ack(job.receipt)  # must not raise


async def test_claim_skips_terminal_row(queue, session_factory):
    from sqlalchemy import text

    job_id = str(uuid.uuid4())
    await queue.enqueue("test_job", {}, options=EnqueueOptions(job_id=job_id))

    async with session_factory() as session:
        await session.execute(
            text("UPDATE jobs SET status = 'failed' WHERE id = :id"), {"id": job_id}
        )
        await session.commit()

    job = await queue.claim()
    assert job is None


async def test_stale_receipt_fail_then_ack_does_not_raise(queue, session_factory):
    """Calling fail() then ack() on the same receipt must not raise."""
    job_id = str(uuid.uuid4())
    await queue.enqueue("test_job", {}, options=EnqueueOptions(job_id=job_id))
    job = await queue.claim()
    assert job is not None

    await queue.fail(job.receipt, "first error", retryable=False)
    # Receipt is now stale (lock_token no longer in _in_flight) — must not raise
    await queue.ack(job.receipt)
