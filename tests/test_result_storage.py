"""Integration test: worker success path persists the handler return value."""

import uuid

import pytest
import pytest_asyncio
from pydantic import BaseModel

from polyqueue.enqueue_options import EnqueueOptions
from polyqueue.job_api import make_job
from polyqueue.jobs.dispatcher import _HANDLERS
from polyqueue.worker.main import _handle

from .conftest import REDIS_URL


class SumResult(BaseModel):
    total: int


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


@pytest_asyncio.fixture(autouse=True)
def _clear_handlers():
    _HANDLERS.clear()
    yield
    _HANDLERS.clear()


@pytest.mark.asyncio
async def test_handler_return_value_persisted(queue, session_factory):
    from sqlalchemy import text

    from polyqueue.worker.identity import make_worker_info

    async def handle_sum(ctx, payload) -> SumResult:  # type: ignore[no-untyped-def]
        return SumResult(total=payload["a"] + payload["b"])

    make_job("sum", handle_sum)

    job_id = str(uuid.uuid4())
    await queue.enqueue("sum", {"a": 10, "b": 5}, options=EnqueueOptions(job_id=job_id))
    job = await queue.claim()
    assert job is not None

    worker_info = make_worker_info("")
    from polyqueue.metrics import NoOpMetrics

    await _handle(
        queue,
        job,
        session_factory,
        "jobs",
        worker_info,
        progress_interval=30,
        metrics=NoOpMetrics(),
    )

    async with session_factory() as session:
        row = await session.execute(
            text("SELECT status, result FROM jobs WHERE id = :id"),
            {"id": job_id},
        )
        record = row.mappings().one()

    assert record["status"] == "succeeded"
    assert record["result"] == {"total": 15}


@pytest.mark.asyncio
async def test_unserializable_result_marks_job_failed_terminal(queue, session_factory):
    """Handler returning a non-JSON-serialisable value → terminal failure, not retry."""
    import datetime

    from sqlalchemy import text

    from polyqueue.worker.identity import make_worker_info

    async def handle_bad(ctx, payload):  # type: ignore[no-untyped-def]
        # datetime is not JSON-serialisable
        return {"ts": datetime.datetime.now(tz=datetime.timezone.utc)}

    make_job("bad_result", handle_bad)

    job_id = str(uuid.uuid4())
    await queue.enqueue("bad_result", {}, options=EnqueueOptions(job_id=job_id))
    job = await queue.claim()
    assert job is not None

    worker_info = make_worker_info("")
    from polyqueue.metrics import NoOpMetrics

    await _handle(
        queue,
        job,
        session_factory,
        "jobs",
        worker_info,
        progress_interval=30,
        metrics=NoOpMetrics(),
    )

    async with session_factory() as session:
        row = await session.execute(
            text("SELECT status, error_code FROM jobs WHERE id = :id"),
            {"id": job_id},
        )
        record = row.mappings().one()

    assert record["status"] == "failed"
    assert record["error_code"] == "result_serialization_failed"


@pytest.mark.asyncio
async def test_void_handler_result_is_null(queue, session_factory):
    from sqlalchemy import text

    from polyqueue.worker.identity import make_worker_info

    async def handle_noop(ctx, payload) -> None:  # type: ignore[no-untyped-def]
        pass

    make_job("noop", handle_noop)

    job_id = str(uuid.uuid4())
    await queue.enqueue("noop", {}, options=EnqueueOptions(job_id=job_id))
    job = await queue.claim()
    assert job is not None

    worker_info = make_worker_info("")
    from polyqueue.metrics import NoOpMetrics

    await _handle(
        queue,
        job,
        session_factory,
        "jobs",
        worker_info,
        progress_interval=30,
        metrics=NoOpMetrics(),
    )

    async with session_factory() as session:
        row = await session.execute(
            text("SELECT status, result FROM jobs WHERE id = :id"),
            {"id": job_id},
        )
        record = row.mappings().one()

    assert record["status"] == "succeeded"
    assert record["result"] is None
