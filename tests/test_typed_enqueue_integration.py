"""Integration tests for handler-first typed enqueue and context propagation.

Tests that only require Postgres run unconditionally (skip if Postgres unavailable).
Tests that require Redis use the `queue` fixture and skip if Redis is unavailable.

Start containers:
    podman compose -f tests/docker-compose.yml up -d
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from pydantic import BaseModel

from polyqueue.enqueue_options import EnqueueOptions
from polyqueue.job_api import make_job
from polyqueue.jobs.context import JobContext
from polyqueue.jobs.dispatcher import _HANDLERS, _HANDLERS_BY_FN

from .conftest import REDIS_URL

# ──────────────────────────────────────────────
# Shared test handler + payload
# ──────────────────────────────────────────────


class AddPayload(BaseModel):
    a: int
    b: int


@pytest.fixture(autouse=True)
def register_handlers():
    """Register test handlers and clean up the registry after each test."""
    _HANDLERS.clear()
    _HANDLERS_BY_FN.clear()

    async def handle_add(ctx: JobContext, payload: AddPayload) -> None:
        pass

    make_job("add", handle_add)
    yield handle_add

    _HANDLERS.clear()
    _HANDLERS_BY_FN.clear()


# ──────────────────────────────────────────────
# Redis fixtures (skip if unavailable)
# ──────────────────────────────────────────────


def _redis_client():
    try:
        from redis.asyncio import Redis

        return Redis.from_url(REDIS_URL, decode_responses=False)
    except ImportError:
        return None


@pytest_asyncio.fixture
async def redis_client(session_factory):
    client = _redis_client()
    if client is None:
        pytest.skip("redis package not installed")
    try:
        await client.ping()
    except Exception:
        pytest.skip(f"Redis not reachable at {REDIS_URL}")
    await client.flushdb()
    yield client
    await client.flushdb()
    await client.aclose()


@pytest_asyncio.fixture
async def queue(redis_client, session_factory):
    from polyqueue.queue.redis_queue import RedisJobQueue

    return RedisJobQueue(
        redis_client,
        session_factory,
        lease_duration_seconds=30,
        heartbeat_interval_seconds=10,
        max_attempts=3,
        retry_backoff_seconds=[0, 1, 2],
    )


# ──────────────────────────────────────────────
# Redis-backed integration tests
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_handler_first_enqueue_stores_correct_job_type(queue, register_handlers):
    """Handler-first enqueue resolves and stores the registered job type."""
    handle_add = register_handlers

    await queue.enqueue(handle_add, AddPayload(a=1, b=2))

    job = await queue.claim()
    assert job is not None
    assert job.job_type == "add"
    assert job.payload == {"a": 1, "b": 2}
    queue._cancel_heartbeat(job.receipt)


@pytest.mark.asyncio
async def test_string_backward_compat(queue):
    """Legacy string-first enqueue still works."""
    await queue.enqueue("add", AddPayload(a=3, b=4))

    job = await queue.claim()
    assert job is not None
    assert job.job_type == "add"
    assert job.payload == {"a": 3, "b": 4}
    queue._cancel_heartbeat(job.receipt)


@pytest.mark.asyncio
async def test_enqueue_options_propagate_to_claimed_job(queue, register_handlers):
    """EnqueueOptions.max_run_seconds and timeout_strategy reach the ClaimedJob."""
    handle_add = register_handlers

    opts = EnqueueOptions(max_run_seconds=7, timeout_strategy="retry")
    await queue.enqueue(handle_add, AddPayload(a=1, b=2), options=opts)

    job = await queue.claim()
    assert job is not None
    assert job.max_run_seconds == 7
    assert job.timeout_strategy == "retry"
    queue._cancel_heartbeat(job.receipt)


@pytest.mark.asyncio
async def test_claimed_job_queued_at_populated(queue, register_handlers):
    """ClaimedJob.queued_at is populated from the DB created_at column."""
    handle_add = register_handlers

    await queue.enqueue(handle_add, AddPayload(a=5, b=6))

    job = await queue.claim()
    assert job is not None
    assert job.queued_at is not None
    queue._cancel_heartbeat(job.receipt)


# ──────────────────────────────────────────────
# Pure-unit test (no Redis needed) — context propagation
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_job_context_queued_at_and_options_populated(session_factory):
    """JobContext.queued_at and .options are populated as the worker would build them."""
    from polyqueue.queue.db import claim_job, insert_job

    job_id = "ctx-test-001"
    async with session_factory() as session:
        await insert_job(
            session,
            table="jobs",
            job_id=job_id,
            job_type="add",
            payload={"a": 1, "b": 2},
            max_attempts=3,
            max_run_seconds=5,
            timeout_strategy="retry",
        )
        await session.commit()

    async with session_factory() as session:
        record = await claim_job(
            session,
            table="jobs",
            job_id=job_id,
            worker_id="test-worker",
            worker_hostname="localhost",
            worker_pid=1,
        )
        await session.commit()

    assert record is not None
    assert record.created_at is not None  # queued_at comes from created_at

    ctx = JobContext.make(
        job_id=job_id,
        job_type="add",
        attempt=record.attempt_count,
        max_attempts=record.max_attempts,
        session_factory=None,
        queued_at=record.created_at,
        options=EnqueueOptions(
            job_id=job_id,
            max_run_seconds=record.max_run_seconds,
            timeout_strategy=record.timeout_strategy,
        ),
    )

    assert ctx.queued_at is not None
    assert ctx.options is not None
    assert ctx.options.max_run_seconds == 5
    assert ctx.options.timeout_strategy == "retry"
    assert ctx.options.job_id == job_id
