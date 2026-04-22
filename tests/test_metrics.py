"""Tests for MetricsHook wiring in queue backends.

Verifies that:
- job_enqueued fires exactly once after a successful broker publish
- job_enqueued does NOT fire when the broker publish fails
- NoOpMetrics satisfies the MetricsHook protocol without errors

Start Redis before running:
    podman run -d --rm --name redis-test -p 16379:6379 redis:7-alpine
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from polyqueue.enqueue_options import EnqueueOptions
from polyqueue.metrics import NoOpMetrics

from .conftest import REDIS_URL

# ──────────────────────────────────────────────
# Spy / fake
# ──────────────────────────────────────────────


class FakeMetrics:
    """Records all metric calls for assertion in tests."""

    def __init__(self) -> None:
        self.job_enqueued_calls: list[str] = []
        self.job_started_calls: list[tuple[str, str]] = []
        self.job_succeeded_calls: list[tuple[str, int]] = []
        self.job_failed_calls: list[tuple[str, str, int]] = []
        self.job_timed_out_calls: list[tuple[str, str, int]] = []
        self.job_retried_calls: list[tuple[str, int]] = []
        self.worker_heartbeat_calls: list[str] = []
        self.worker_started_calls: list[str] = []
        self.worker_stopped_calls: list[str] = []

    def job_enqueued(self, job_type: str) -> None:
        self.job_enqueued_calls.append(job_type)

    def job_started(self, job_type: str, worker_id: str) -> None:
        self.job_started_calls.append((job_type, worker_id))

    def job_succeeded(self, job_type: str, duration_ms: int) -> None:
        self.job_succeeded_calls.append((job_type, duration_ms))

    def job_failed(self, job_type: str, error_code: str, duration_ms: int) -> None:
        self.job_failed_calls.append((job_type, error_code, duration_ms))

    def job_timed_out(self, job_type: str, strategy: str, duration_ms: int) -> None:
        self.job_timed_out_calls.append((job_type, strategy, duration_ms))

    def job_retried(self, job_type: str, attempt: int) -> None:
        self.job_retried_calls.append((job_type, attempt))

    def worker_heartbeat(self, worker_id: str) -> None:
        self.worker_heartbeat_calls.append(worker_id)

    def worker_started(self, worker_id: str) -> None:
        self.worker_started_calls.append(worker_id)

    def worker_stopped(self, worker_id: str) -> None:
        self.worker_stopped_calls.append(worker_id)


@pytest.fixture
def fake_metrics() -> FakeMetrics:
    return FakeMetrics()


# ──────────────────────────────────────────────
# Redis fixtures (mirrors test_redis_queue.py)
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
async def queue_with_metrics(redis_client, session_factory, fake_metrics):
    from polyqueue.queue.redis_queue import RedisJobQueue

    return RedisJobQueue(
        redis_client,
        session_factory,
        lease_duration_seconds=30,
        heartbeat_interval_seconds=10,
        max_attempts=3,
        retry_backoff_seconds=[0, 1, 2],
        metrics=fake_metrics,
    )


# ──────────────────────────────────────────────
# Protocol conformance (duck-typing smoke tests)
# ──────────────────────────────────────────────


def test_noop_metrics_callable() -> None:
    """NoOpMetrics implements all MetricsHook methods without raising."""
    m = NoOpMetrics()
    m.job_enqueued("t")
    m.job_started("t", "w")
    m.job_succeeded("t", 100)
    m.job_failed("t", "err", 100)
    m.job_timed_out("t", "retry", 100)
    m.job_retried("t", 1)
    m.worker_heartbeat("w")
    m.worker_started("w")
    m.worker_stopped("w")


def test_fake_metrics_records_calls() -> None:
    """FakeMetrics records every call made to it."""
    m = FakeMetrics()
    m.job_enqueued("type_a")
    m.job_enqueued("type_b")
    m.job_started("type_a", "worker_1")
    assert m.job_enqueued_calls == ["type_a", "type_b"]
    assert m.job_started_calls == [("type_a", "worker_1")]


# ──────────────────────────────────────────────
# job_enqueued wiring
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_job_enqueued_fires_on_success(
    queue_with_metrics, fake_metrics, session_factory
):
    """job_enqueued is emitted exactly once after a successful broker push."""
    job_id = str(uuid.uuid4())
    await queue_with_metrics.enqueue(
        "my_job", {"x": 1}, options=EnqueueOptions(job_id=job_id)
    )

    assert fake_metrics.job_enqueued_calls == ["my_job"]


@pytest.mark.asyncio
async def test_job_enqueued_not_fired_on_broker_failure(
    queue_with_metrics, fake_metrics, session_factory
):
    """job_enqueued is NOT emitted when the Redis push raises."""
    job_id = str(uuid.uuid4())

    with (
        patch.object(
            queue_with_metrics._redis,
            "lpush",
            AsyncMock(side_effect=ConnectionError("redis down")),
        ),
        pytest.raises(ConnectionError),
    ):
        await queue_with_metrics.enqueue(
            "my_job", {"x": 1}, options=EnqueueOptions(job_id=job_id)
        )

    assert fake_metrics.job_enqueued_calls == []


@pytest.mark.asyncio
async def test_job_enqueued_counts_multiple_enqueues(
    queue_with_metrics, fake_metrics, session_factory
):
    """job_enqueued records every successfully published job."""
    ids = [str(uuid.uuid4()) for _ in range(3)]
    for job_id in ids:
        await queue_with_metrics.enqueue(
            "batch_job", {}, options=EnqueueOptions(job_id=job_id)
        )

    assert fake_metrics.job_enqueued_calls == ["batch_job", "batch_job", "batch_job"]
