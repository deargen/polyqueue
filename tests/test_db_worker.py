"""Tests for worker-aware DB helpers (require running Postgres).

These tests exercise the state-transition helpers in isolation, so they don't
pass an ``attempts_table`` — ``_insert_attempt_event`` skips the INSERT when the
table name is None. Tests that need to assert on the attempt log should pass
``attempts_table='jobs_attempts'`` explicitly.
"""

import pytest
from sqlalchemy import text

from polyqueue.queue.db import (
    claim_job,
    heartbeat_progress,
    insert_job,
    mark_failed_terminal,
    mark_succeeded,
    reset_for_retry,
)


@pytest.mark.asyncio
async def test_claim_sets_worker_fields(session_factory):
    async with session_factory() as session:
        await insert_job(
            session,
            table="jobs",
            job_id="w1",
            job_type="test",
            payload={"x": 1},
            max_attempts=3,
        )
        await session.commit()

    async with session_factory() as session:
        row = await claim_job(
            session,
            table="jobs",
            job_id="w1",
            worker_id="host:1:abc",
            worker_hostname="host",
            worker_pid=1,
        )
        await session.commit()

    assert row is not None
    assert row.claimed_by_worker_id == "host:1:abc"
    assert row.claimed_by_hostname == "host"
    assert row.claimed_by_pid == 1
    assert row.lease_token is not None


@pytest.mark.asyncio
async def test_claim_with_max_run_seconds(session_factory):
    async with session_factory() as session:
        await insert_job(
            session,
            table="jobs",
            job_id="t1",
            job_type="test",
            payload={},
            max_attempts=3,
            max_run_seconds=60,
            timeout_strategy="retry",
        )
        await session.commit()

    async with session_factory() as session:
        row = await claim_job(
            session,
            table="jobs",
            job_id="t1",
            worker_id="w",
            worker_hostname="h",
            worker_pid=1,
        )
        await session.commit()

    assert row is not None
    assert row.max_run_seconds == 60
    assert row.timeout_strategy == "retry"
    assert row.timeout_at is not None


@pytest.mark.asyncio
async def test_mark_succeeded_keeps_worker_identity_clears_lease(session_factory):
    async with session_factory() as session:
        await insert_job(
            session,
            table="jobs",
            job_id="s1",
            job_type="test",
            payload={},
            max_attempts=3,
        )
        await session.commit()

    async with session_factory() as session:
        claim = await claim_job(
            session,
            table="jobs",
            job_id="s1",
            worker_id="w",
            worker_hostname="h",
            worker_pid=1,
        )
        await session.commit()

    assert claim is not None

    async with session_factory() as session:
        ok = await mark_succeeded(
            session, table="jobs", job_id="s1", lease_token=claim.lease_token
        )
        await session.commit()
    assert ok is True

    async with session_factory() as session:
        row = await session.execute(
            text(
                "SELECT claimed_by_worker_id, claimed_at, lease_token, "
                "timeout_at, status FROM jobs WHERE id = 's1'"
            )
        )
        r = row.mappings().one()
        # Worker identity and claimed_at are retained for forensics
        assert r["claimed_by_worker_id"] == "w"
        assert r["claimed_at"] is not None
        # Only in-flight-gating state is cleared
        assert r["lease_token"] is None
        assert r["status"] == "succeeded"


@pytest.mark.asyncio
async def test_mark_failed_terminal_keeps_worker_identity(session_factory):
    async with session_factory() as session:
        await insert_job(
            session,
            table="jobs",
            job_id="f1",
            job_type="test",
            payload={},
            max_attempts=3,
        )
        await session.commit()

    async with session_factory() as session:
        claim = await claim_job(
            session,
            table="jobs",
            job_id="f1",
            worker_id="w",
            worker_hostname="h",
            worker_pid=1,
        )
        await session.commit()

    assert claim is not None

    async with session_factory() as session:
        ok = await mark_failed_terminal(
            session,
            table="jobs",
            job_id="f1",
            lease_token=claim.lease_token,
            error_code="test",
            last_error="test error",
        )
        await session.commit()
    assert ok is True

    async with session_factory() as session:
        row = await session.execute(
            text(
                "SELECT claimed_by_worker_id, lease_token, status "
                "FROM jobs WHERE id = 'f1'"
            )
        )
        r = row.mappings().one()
        assert r["claimed_by_worker_id"] == "w"
        assert r["lease_token"] is None
        assert r["status"] == "failed"


@pytest.mark.asyncio
async def test_reset_for_retry_keeps_worker_identity(session_factory):
    async with session_factory() as session:
        await insert_job(
            session,
            table="jobs",
            job_id="r1",
            job_type="test",
            payload={},
            max_attempts=3,
        )
        await session.commit()

    async with session_factory() as session:
        claim = await claim_job(
            session,
            table="jobs",
            job_id="r1",
            worker_id="w",
            worker_hostname="h",
            worker_pid=1,
        )
        await session.commit()

    assert claim is not None

    async with session_factory() as session:
        attempt = await reset_for_retry(
            session, table="jobs", job_id="r1", lease_token=claim.lease_token
        )
        await session.commit()

    assert attempt == 1

    async with session_factory() as session:
        row = await session.execute(
            text(
                "SELECT claimed_by_worker_id, lease_token, status "
                "FROM jobs WHERE id = 'r1'"
            )
        )
        r = row.mappings().one()
        assert r["claimed_by_worker_id"] == "w"
        assert r["lease_token"] is None
        assert r["status"] == "queued"


@pytest.mark.asyncio
async def test_heartbeat_progress(session_factory):
    async with session_factory() as session:
        await insert_job(
            session,
            table="jobs",
            job_id="h1",
            job_type="test",
            payload={},
            max_attempts=3,
        )
        await session.commit()

    async with session_factory() as session:
        claim = await claim_job(
            session,
            table="jobs",
            job_id="h1",
            worker_id="w",
            worker_hostname="h",
            worker_pid=1,
        )
        await session.commit()

    assert claim is not None

    async with session_factory() as session:
        updated = await heartbeat_progress(
            session, table="jobs", job_id="h1", lease_token=claim.lease_token
        )
        await session.commit()
    assert updated is True

    async with session_factory() as session:
        row = await session.execute(
            text("SELECT progress_heartbeat_at FROM jobs WHERE id = 'h1'")
        )
        r = row.mappings().one()
        assert r["progress_heartbeat_at"] is not None


@pytest.mark.asyncio
async def test_terminal_update_fails_with_wrong_lease_token(session_factory):
    """Race-safety guard: a wrong lease_token must not transition the row."""
    import uuid

    async with session_factory() as session:
        await insert_job(
            session,
            table="jobs",
            job_id="race1",
            job_type="test",
            payload={},
            max_attempts=3,
        )
        await session.commit()

    async with session_factory() as session:
        claim = await claim_job(
            session,
            table="jobs",
            job_id="race1",
            worker_id="w",
            worker_hostname="h",
            worker_pid=1,
        )
        await session.commit()
    assert claim is not None

    async with session_factory() as session:
        ok = await mark_succeeded(
            session, table="jobs", job_id="race1", lease_token=uuid.uuid4()
        )
        await session.commit()
    assert ok is False, "stale lease_token must not transition the row"
