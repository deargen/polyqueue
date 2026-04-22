"""Tests for SqsJobQueue using a local ElasticMQ container.

Start before running:
    podman run -d --rm --name elasticmq-test -p 19324:9324 \
        softwaremill/elasticmq-native

ElasticMQ is a lightweight SQS-compatible server. Queue is auto-created on first use
via a custom endpoint URL.
"""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio

from polyqueue.enqueue_options import EnqueueOptions

from .conftest import SQS_ENDPOINT

QUEUE_NAME = "polyqueue-test"


def _sqs_client(region: str = "elasticmq"):
    try:
        import boto3

        return boto3.client(
            "sqs",
            region_name=region,
            endpoint_url=SQS_ENDPOINT,
            aws_access_key_id="x",
            aws_secret_access_key="x",
        )
    except ImportError:
        return None


@pytest.fixture
def sqs_queue_url():
    client = _sqs_client()
    if client is None:
        pytest.skip("boto3 not installed")
    try:
        resp = client.create_queue(QueueName=QUEUE_NAME)
    except Exception:
        pytest.skip(f"ElasticMQ not reachable at {SQS_ENDPOINT}")
    return resp["QueueUrl"]


@pytest_asyncio.fixture
async def queue(sqs_queue_url, session_factory):
    from polyqueue.queue.sqs_queue import SqsJobQueue

    q = SqsJobQueue(
        sqs_queue_url,
        session_factory,
        region="elasticmq",
        visibility_timeout_seconds=30,
        heartbeat_interval_seconds=10,
        max_attempts=3,
        retry_backoff_seconds=[1, 2, 5],
    )
    # Patch the boto3 client to point at ElasticMQ
    q._sqs = _sqs_client()
    yield q
    # Purge queue after test
    import contextlib

    with contextlib.suppress(Exception):
        _sqs_client().purge_queue(QueueUrl=sqs_queue_url)


# ──────────────────────────────────────────────
# Tests (same semantics as Redis)
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
    queue._cancel_heartbeat(job.receipt)


async def test_claim_returns_none_when_empty(queue):
    job = await queue.claim()
    assert job is None


async def test_ack_does_not_touch_db(queue, session_factory):
    """ack() settles the SQS message; DB status is NOT changed by ack()."""
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

    # ElasticMQ respects DelaySeconds=1 — wait briefly
    await asyncio.sleep(2)
    job2 = await queue.claim()
    assert job2 is not None
    assert job2.job_id == job_id
    assert job2.attempt == 2
    queue._cancel_heartbeat(job2.receipt)


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
    # Receipt is now stale — second call must be idempotent
    await queue.ack(job.receipt)
