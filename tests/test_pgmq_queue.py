"""Tests for PgmqJobQueue against a Postgres instance with pgmq extension enabled."""

from __future__ import annotations

import asyncio
import uuid

import pytest
import pytest_asyncio
from sqlalchemy import text

from polyqueue.enqueue_options import EnqueueOptions


async def _ensure_pgmq_extension(session_factory) -> None:
    try:
        async with session_factory() as session:
            await session.execute(text("CREATE EXTENSION IF NOT EXISTS pgmq"))
            await session.commit()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"pgmq extension is not available in test Postgres: {exc}")


@pytest_asyncio.fixture
async def queue(session_factory):
    from polyqueue.queue.pgmq_queue import PgmqJobQueue

    await _ensure_pgmq_extension(session_factory)
    queue_name = f"polyqueue_{uuid.uuid4().hex[:12]}"

    q = PgmqJobQueue(
        session_factory,
        table_name="jobs",
        queue_name=queue_name,
        visibility_timeout_seconds=6,
        heartbeat_interval_seconds=1,
        poll_seconds=1,
        poll_interval_ms=100,
        max_attempts=3,
        retry_backoff_seconds=[1, 2, 5],
    )
    await q._ensure_queue()
    yield q
    await q.close()


async def _queue_message_count(queue, session_factory) -> int:
    async with session_factory() as session:
        row = await session.execute(
            text(f"SELECT count(*) AS n FROM pgmq.q_{queue._queue_name}")
        )
        return int(row.scalar_one())


async def _message_vt(queue, session_factory, msg_id: int):
    async with session_factory() as session:
        row = await session.execute(
            text(f"SELECT vt FROM pgmq.q_{queue._queue_name} WHERE msg_id = :msg_id"),
            {"msg_id": msg_id},
        )
        return row.scalar_one_or_none()


async def test_enqueue_and_claim(queue):
    job_id = str(uuid.uuid4())
    await queue.enqueue(
        "test_job",
        {"key": "value"},
        options=EnqueueOptions(job_id=job_id),
    )

    job = await queue.claim()
    assert job is not None
    assert job.job_id == job_id
    assert job.job_type == "test_job"
    assert job.payload == {"key": "value"}
    assert job.attempt == 1
    queue._cancel_heartbeat(job.receipt)


async def test_claim_returns_none_when_empty(queue):
    assert await queue.claim() is None


async def test_ack_does_not_touch_db(queue, session_factory):
    job_id = str(uuid.uuid4())
    await queue.enqueue("test_job", {}, options=EnqueueOptions(job_id=job_id))
    job = await queue.claim()
    assert job is not None

    await queue.ack(job.receipt)

    async with session_factory() as session:
        row = await session.execute(
            text("SELECT status FROM jobs WHERE id = :id"),
            {"id": job_id},
        )
        record = row.mappings().one()
    assert record["status"] == "processing"


async def test_fail_retryable_reschedules(queue):
    job_id = str(uuid.uuid4())
    await queue.enqueue("test_job", {}, options=EnqueueOptions(job_id=job_id))
    job = await queue.claim()
    assert job is not None

    await queue.fail(job.receipt, "transient error", retryable=True)
    await asyncio.sleep(1.2)

    job2 = await queue.claim()
    assert job2 is not None
    assert job2.job_id == job_id
    assert job2.attempt == 2
    queue._cancel_heartbeat(job2.receipt)


async def test_fail_terminal_no_reschedule(queue):
    job_id = str(uuid.uuid4())
    await queue.enqueue("test_job", {}, options=EnqueueOptions(job_id=job_id))
    job = await queue.claim()
    assert job is not None

    await queue.fail(job.receipt, "fatal error", retryable=False)
    await asyncio.sleep(1)
    assert await queue.claim() is None


async def test_ack_idempotent(queue):
    job_id = str(uuid.uuid4())
    await queue.enqueue("test_job", {}, options=EnqueueOptions(job_id=job_id))
    job = await queue.claim()
    assert job is not None

    await queue.ack(job.receipt)
    await queue.ack(job.receipt)


async def test_claim_skips_terminal_row_and_deletes_broker_message(
    queue, session_factory
):
    job_id = str(uuid.uuid4())
    await queue.enqueue("test_job", {}, options=EnqueueOptions(job_id=job_id))

    async with session_factory() as session:
        await session.execute(
            text("UPDATE jobs SET status = 'failed' WHERE id = :id"),
            {"id": job_id},
        )
        await session.commit()

    assert await queue.claim() is None
    assert await _queue_message_count(queue, session_factory) == 0


async def test_stale_receipt_fail_then_ack_does_not_raise(queue):
    job_id = str(uuid.uuid4())
    await queue.enqueue("test_job", {}, options=EnqueueOptions(job_id=job_id))
    job = await queue.claim()
    assert job is not None

    await queue.fail(job.receipt, "first error", retryable=False)
    await queue.ack(job.receipt)


async def test_visibility_heartbeat_extends_vt(queue, session_factory):
    job_id = str(uuid.uuid4())
    await queue.enqueue("test_job", {}, options=EnqueueOptions(job_id=job_id))
    job = await queue.claim()
    assert job is not None

    msg_id = int(job.receipt)
    vt1 = await _message_vt(queue, session_factory, msg_id)
    assert vt1 is not None
    await asyncio.sleep(1.2)
    vt2 = await _message_vt(queue, session_factory, msg_id)
    assert vt2 is not None
    assert vt2 > vt1

    await queue.ack(job.receipt)
