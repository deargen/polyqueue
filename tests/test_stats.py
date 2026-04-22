"""Tests for the stats module (require running Postgres)."""

import pytest

from polyqueue.queue.db import insert_job
from polyqueue.stats import QueueSnapshot, get_job_type_counts, get_queue_snapshot


@pytest.mark.asyncio
async def test_snapshot_empty(session_factory):
    async with session_factory() as session:
        snap = await get_queue_snapshot(
            session, jobs_table="jobs", workers_table="polyqueue_workers"
        )
    assert snap.queued_jobs == 0
    assert snap.processing_jobs == 0
    assert snap.healthy_workers == 0
    assert snap.oldest_queued_age_seconds is None
    assert isinstance(snap, QueueSnapshot)


@pytest.mark.asyncio
async def test_snapshot_with_jobs(session_factory):
    async with session_factory() as session:
        await insert_job(
            session,
            table="jobs",
            job_id="s1",
            job_type="test",
            payload={},
            max_attempts=3,
        )
        await insert_job(
            session,
            table="jobs",
            job_id="s2",
            job_type="test",
            payload={},
            max_attempts=3,
        )
        await session.commit()
    async with session_factory() as session:
        snap = await get_queue_snapshot(
            session, jobs_table="jobs", workers_table="polyqueue_workers"
        )
    assert snap.queued_jobs == 2
    assert snap.oldest_queued_age_seconds is not None
    assert snap.oldest_queued_age_seconds >= 0


@pytest.mark.asyncio
async def test_snapshot_counts_by_status(session_factory):
    from polyqueue.queue.db import claim_job, mark_failed_terminal, mark_succeeded

    async with session_factory() as session:
        await insert_job(
            session, table="jobs", job_id="c1", job_type="t", payload={}, max_attempts=3
        )
        await insert_job(
            session, table="jobs", job_id="c2", job_type="t", payload={}, max_attempts=3
        )
        await insert_job(
            session, table="jobs", job_id="c3", job_type="t", payload={}, max_attempts=3
        )
        await session.commit()

    async with session_factory() as session:
        c1 = await claim_job(
            session,
            table="jobs",
            job_id="c1",
            worker_id="w",
            worker_hostname="h",
            worker_pid=1,
        )
        c2 = await claim_job(
            session,
            table="jobs",
            job_id="c2",
            worker_id="w",
            worker_hostname="h",
            worker_pid=1,
        )
        await session.commit()
    assert c1 is not None and c2 is not None

    async with session_factory() as session:
        await mark_succeeded(
            session, table="jobs", job_id="c1", lease_token=c1.lease_token
        )
        await mark_failed_terminal(
            session,
            table="jobs",
            job_id="c2",
            lease_token=c2.lease_token,
            error_code="err",
            last_error="oops",
        )
        await session.commit()

    async with session_factory() as session:
        snap = await get_queue_snapshot(
            session, jobs_table="jobs", workers_table="polyqueue_workers"
        )

    assert snap.queued_jobs == 1
    assert snap.processing_jobs == 0
    assert snap.succeeded_jobs == 1
    assert snap.failed_jobs == 1


@pytest.mark.asyncio
async def test_get_job_type_counts(session_factory):
    async with session_factory() as session:
        await insert_job(
            session,
            table="jobs",
            job_id="j1",
            job_type="alpha",
            payload={},
            max_attempts=3,
        )
        await insert_job(
            session,
            table="jobs",
            job_id="j2",
            job_type="alpha",
            payload={},
            max_attempts=3,
        )
        await insert_job(
            session,
            table="jobs",
            job_id="j3",
            job_type="beta",
            payload={},
            max_attempts=3,
        )
        await session.commit()

    async with session_factory() as session:
        counts = await get_job_type_counts(session, jobs_table="jobs", status="queued")

    assert len(counts) == 2
    by_type = {c.job_type: c.count for c in counts}
    assert by_type["alpha"] == 2
    assert by_type["beta"] == 1
