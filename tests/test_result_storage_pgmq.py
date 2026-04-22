"""Integration test: worker success path persists the handler return value (PGMQ backend)."""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio
from pydantic import BaseModel
from sqlalchemy import text

from polyqueue.enqueue_options import EnqueueOptions
from polyqueue.job_api import make_job
from polyqueue.jobs.dispatcher import _HANDLERS
from polyqueue.worker.main import _handle


async def _ensure_pgmq_extension(session_factory) -> None:
    try:
        async with session_factory() as session:
            await session.execute(text("CREATE EXTENSION IF NOT EXISTS pgmq"))
            await session.commit()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"pgmq extension is not available in test Postgres: {exc}")


class SumResult(BaseModel):
    total: int


@pytest_asyncio.fixture
async def queue(session_factory):
    from polyqueue.queue.pgmq_queue import PgmqJobQueue

    await _ensure_pgmq_extension(session_factory)
    queue_name = f"polyqueue_{uuid.uuid4().hex[:12]}"
    q = PgmqJobQueue(
        session_factory,
        table_name="jobs",
        queue_name=queue_name,
        visibility_timeout_seconds=30,
        heartbeat_interval_seconds=5,
        poll_seconds=1,
        poll_interval_ms=100,
        max_attempts=3,
        retry_backoff_seconds=[0, 1, 2],
    )
    await q._ensure_queue()
    yield q
    await q.close()


@pytest_asyncio.fixture(autouse=True)
def _clear_handlers():
    _HANDLERS.clear()
    yield
    _HANDLERS.clear()


@pytest.mark.asyncio
async def test_handler_return_value_persisted(queue, session_factory):
    from polyqueue.metrics import NoOpMetrics
    from polyqueue.worker.identity import make_worker_info

    async def handle_sum(ctx, payload) -> SumResult:  # type: ignore[no-untyped-def]
        return SumResult(total=payload["a"] + payload["b"])

    make_job("sum", handle_sum)

    job_id = str(uuid.uuid4())
    await queue.enqueue("sum", {"a": 10, "b": 5}, options=EnqueueOptions(job_id=job_id))
    job = await queue.claim()
    assert job is not None

    worker_info = make_worker_info("")
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
