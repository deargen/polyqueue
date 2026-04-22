"""Tests for RedisJobQueue using a local Redis container.

Start before running:
    podman run -d --rm --name redis-test -p 16379:6379 redis:7-alpine
"""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio

from polyqueue.enqueue_options import EnqueueOptions

from .conftest import REDIS_URL


def _redis_client():
    try:
        from redis.asyncio import Redis

        return Redis.from_url(REDIS_URL, decode_responses=False)
    except ImportError:
        return None


@pytest_asyncio.fixture
async def redis(session_factory):
    client = _redis_client()
    if client is None:
        pytest.skip("redis package not installed")
    try:
        await client.ping()
    except Exception:
        pytest.skip(f"Redis not reachable at {REDIS_URL}")

    # Flush test keys before each test
    await client.flushdb()
    yield client
    await client.flushdb()
    await client.aclose()


@pytest_asyncio.fixture
async def queue(redis, session_factory):
    from polyqueue.queue.redis_queue import RedisJobQueue

    return RedisJobQueue(
        redis,
        session_factory,
        lease_duration_seconds=30,
        heartbeat_interval_seconds=10,
        max_attempts=3,
        retry_backoff_seconds=[0, 1, 2],
    )


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

    # Cancel heartbeat to avoid dangling tasks
    queue._cancel_heartbeat(job.receipt)


async def test_claim_returns_none_when_empty(queue):
    job = await queue.claim()
    assert job is None


async def test_ack_clears_lease_keys(queue, session_factory):
    """ack() clears Redis lease keys; DB status is NOT changed by ack()."""
    from sqlalchemy import text

    job_id = str(uuid.uuid4())
    await queue.enqueue("test_job", {}, options=EnqueueOptions(job_id=job_id))
    job = await queue.claim()
    assert job is not None

    # Verify lease key exists before ack
    lease_key = f"jobs:lease:{job_id}"
    assert await queue._redis.exists(lease_key)

    await queue.ack(job.receipt)

    # Lease key should be gone
    assert not await queue._redis.exists(lease_key)
    assert await queue._redis.zscore("jobs:leases", job_id) is None

    # DB status remains 'processing' — ack() does NOT touch the DB
    async with session_factory() as session:
        row = await session.execute(
            text("SELECT status FROM jobs WHERE id = :id"),
            {"id": job_id},
        )
        record = row.mappings().one()

    assert record["status"] == "processing"


async def test_fail_retryable_reschedules(queue, session_factory):
    job_id = str(uuid.uuid4())
    await queue.enqueue("test_job", {}, options=EnqueueOptions(job_id=job_id))
    job = await queue.claim()
    assert job is not None

    await queue.fail(job.receipt, "transient error", retryable=True)

    # Job should appear back in pending after the retry poller moves it
    await queue.move_due_retries()
    job2 = await queue.claim()
    assert job2 is not None
    assert job2.job_id == job_id
    assert job2.attempt == 2
    queue._cancel_heartbeat(job2.receipt)


async def test_fail_terminal_does_not_reschedule(queue, session_factory):
    job_id = str(uuid.uuid4())
    await queue.enqueue("test_job", {}, options=EnqueueOptions(job_id=job_id))
    job = await queue.claim()
    assert job is not None

    await queue.fail(job.receipt, "fatal error", retryable=False)

    # Nothing should be rescheduled
    await queue.move_due_retries()
    job2 = await queue.claim()
    assert job2 is None


async def test_ack_idempotent(queue, session_factory):
    job_id = str(uuid.uuid4())
    await queue.enqueue("test_job", {}, options=EnqueueOptions(job_id=job_id))
    job = await queue.claim()
    assert job is not None

    await queue.ack(job.receipt)
    # Second call should not raise
    await queue.ack(job.receipt)


async def test_claim_skips_terminal_row(queue, session_factory):
    """If a job row is already terminal, claim() should drop the message."""
    from sqlalchemy import text

    job_id = str(uuid.uuid4())
    await queue.enqueue("test_job", {}, options=EnqueueOptions(job_id=job_id))

    # Manually mark the row as failed before claiming
    async with session_factory() as session:
        await session.execute(
            text("UPDATE jobs SET status = 'failed' WHERE id = :id"),
            {"id": job_id},
        )
        await session.commit()

    job = await queue.claim()
    assert job is None


async def test_fail_schedule_retry_failure_leaves_queued(queue, session_factory):
    """If _schedule_retry fails, fail() leaves the job as queued so the reaper can recover it."""
    from sqlalchemy import text

    job_id = str(uuid.uuid4())
    await queue.enqueue("test", {"x": 1}, options=EnqueueOptions(job_id=job_id))
    job = await queue.claim()
    assert job is not None

    original_schedule = queue._schedule_retry

    async def failing_schedule(*args, **kwargs):
        raise RuntimeError("Redis unavailable")

    queue._schedule_retry = failing_schedule
    try:
        await queue.fail(job.receipt, "error", retryable=True)
    finally:
        queue._schedule_retry = original_schedule

    async with session_factory() as session:
        row = await session.execute(
            text("SELECT status FROM jobs WHERE id = :id"),
            {"id": job_id},
        )
        record = row.mappings().one()

    # Job is reset to queued; reaper orphan reconciliation will re-push it.
    assert record["status"] == "queued"


async def test_db_max_attempts_is_authoritative(session_factory, redis):
    """ClaimedJob.max_attempts must come from the DB row, not the worker-config default.

    The queue is created with max_attempts=5 (high default), but the DB row has
    max_attempts=1. A retry decision based on job.max_attempts should produce
    max_attempts=1, not 5.
    """
    from polyqueue.queue.redis_queue import RedisJobQueue

    # Adapter configured with a high default — should not bleed into individual jobs
    q = RedisJobQueue(
        redis,
        session_factory,
        lease_duration_seconds=30,
        heartbeat_interval_seconds=10,
        max_attempts=5,
        retry_backoff_seconds=[0, 1, 2],
    )

    job_id = str(uuid.uuid4())
    # Enqueue with the adapter's default (5), then override the DB row to 1
    await q.enqueue("test_job", {}, options=EnqueueOptions(job_id=job_id))
    from sqlalchemy import text

    async with session_factory() as session:
        await session.execute(
            text("UPDATE jobs SET max_attempts = 1 WHERE id = :id"),
            {"id": job_id},
        )
        await session.commit()

    job = await q.claim()
    assert job is not None
    assert job.max_attempts == 1, (
        "ClaimedJob.max_attempts should reflect the DB value, not adapter default"
    )
    # Worker retry logic: job.attempt (1) < job.max_attempts (1) is False → terminal
    assert not (job.attempt < job.max_attempts)
    q._cancel_heartbeat(job.receipt)
