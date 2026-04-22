"""Tests for worker registry DB helpers (require running Postgres)."""

from datetime import datetime, timezone

import pytest

from polyqueue.worker.identity import make_worker_info
from polyqueue.worker.registry import (
    deregister_worker,
    heartbeat_worker,
    list_workers,
    reap_stale_workers,
    register_worker,
)


@pytest.mark.asyncio
async def test_register_and_list(session_factory):
    wi = make_worker_info("test-worker")
    async with session_factory() as session:
        await register_worker(
            session, table="polyqueue_workers", worker_info=wi, backend="redis"
        )
        await session.commit()
    async with session_factory() as session:
        workers = await list_workers(session, table="polyqueue_workers")
    assert len(workers) >= 1
    assert any(w.worker_id == wi.worker_id for w in workers)


@pytest.mark.asyncio
async def test_register_sets_fields(session_factory):
    wi = make_worker_info("fields-test")
    async with session_factory() as session:
        await register_worker(
            session,
            table="polyqueue_workers",
            worker_info=wi,
            backend="redis",
            worker_name="fields-test",
            version="1.0.0",
            metadata={"env": "test"},
        )
        await session.commit()
    async with session_factory() as session:
        workers = await list_workers(session, table="polyqueue_workers")
    row = next(w for w in workers if w.worker_id == wi.worker_id)
    assert row.hostname == wi.hostname
    assert row.pid == wi.pid
    assert row.backend == "redis"
    assert row.worker_name == "fields-test"
    assert row.version == "1.0.0"
    assert row.status == "running"


@pytest.mark.asyncio
async def test_register_upsert(session_factory):
    """Re-registering the same worker_id should update fields, not insert a duplicate."""
    wi = make_worker_info("upsert-test")
    async with session_factory() as session:
        await register_worker(
            session, table="polyqueue_workers", worker_info=wi, backend="redis"
        )
        await session.commit()
    async with session_factory() as session:
        await register_worker(
            session, table="polyqueue_workers", worker_info=wi, backend="sqs"
        )
        await session.commit()
    async with session_factory() as session:
        workers = await list_workers(session, table="polyqueue_workers")
    matches = [w for w in workers if w.worker_id == wi.worker_id]
    assert len(matches) == 1
    assert matches[0].backend == "sqs"


@pytest.mark.asyncio
async def test_heartbeat(session_factory):
    wi = make_worker_info("hb-test")
    async with session_factory() as session:
        await register_worker(session, table="polyqueue_workers", worker_info=wi)
        await session.commit()
    async with session_factory() as session:
        ok = await heartbeat_worker(
            session,
            table="polyqueue_workers",
            worker_id=wi.worker_id,
            current_job_id="job-1",
        )
        await session.commit()
    assert ok


@pytest.mark.asyncio
async def test_heartbeat_sets_current_job(session_factory):
    wi = make_worker_info("hb-job-test")
    async with session_factory() as session:
        await register_worker(session, table="polyqueue_workers", worker_info=wi)
        await session.commit()
    async with session_factory() as session:
        await heartbeat_worker(
            session,
            table="polyqueue_workers",
            worker_id=wi.worker_id,
            current_job_id="job-42",
            current_job_started_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        await session.commit()
    async with session_factory() as session:
        workers = await list_workers(session, table="polyqueue_workers")
    row = next(w for w in workers if w.worker_id == wi.worker_id)
    assert row.current_job_id == "job-42"
    assert row.current_job_started_at is not None


@pytest.mark.asyncio
async def test_heartbeat_returns_false_for_unknown_worker(session_factory):
    async with session_factory() as session:
        ok = await heartbeat_worker(
            session,
            table="polyqueue_workers",
            worker_id="nonexistent-worker-id",
        )
        await session.commit()
    assert not ok


@pytest.mark.asyncio
async def test_deregister(session_factory):
    wi = make_worker_info("dereg-test")
    async with session_factory() as session:
        await register_worker(session, table="polyqueue_workers", worker_info=wi)
        await session.commit()
    async with session_factory() as session:
        await deregister_worker(
            session, table="polyqueue_workers", worker_id=wi.worker_id
        )
        await session.commit()
    async with session_factory() as session:
        workers = await list_workers(
            session, table="polyqueue_workers", status="stopped"
        )
    assert any(w.worker_id == wi.worker_id for w in workers)


@pytest.mark.asyncio
async def test_deregister_clears_job_fields(session_factory):
    wi = make_worker_info("dereg-job-test")
    async with session_factory() as session:
        await register_worker(session, table="polyqueue_workers", worker_info=wi)
        await session.commit()
    async with session_factory() as session:
        await heartbeat_worker(
            session,
            table="polyqueue_workers",
            worker_id=wi.worker_id,
            current_job_id="job-99",
        )
        await session.commit()
    async with session_factory() as session:
        await deregister_worker(
            session, table="polyqueue_workers", worker_id=wi.worker_id
        )
        await session.commit()
    async with session_factory() as session:
        workers = await list_workers(
            session, table="polyqueue_workers", status="stopped"
        )
    row = next(w for w in workers if w.worker_id == wi.worker_id)
    assert row.current_job_id is None
    assert row.current_job_started_at is None


@pytest.mark.asyncio
async def test_reap_stale_workers(session_factory):
    wi = make_worker_info("reap-test")
    async with session_factory() as session:
        await register_worker(session, table="polyqueue_workers", worker_info=wi)
        # Manually backdate heartbeat to simulate staleness
        from sqlalchemy import text

        await session.execute(
            text(
                "UPDATE polyqueue_workers SET last_heartbeat_at = now() - interval '120 seconds'"
                " WHERE worker_id = :wid"
            ),
            {"wid": wi.worker_id},
        )
        await session.commit()
    async with session_factory() as session:
        count = await reap_stale_workers(
            session,
            table="polyqueue_workers",
            stale_threshold_seconds=60,
        )
        await session.commit()
    assert count >= 1
    async with session_factory() as session:
        workers = await list_workers(session, table="polyqueue_workers", status="dead")
    assert any(w.worker_id == wi.worker_id for w in workers)


@pytest.mark.asyncio
async def test_reap_does_not_touch_stopped_workers(session_factory):
    wi = make_worker_info("reap-stopped-test")
    async with session_factory() as session:
        await register_worker(session, table="polyqueue_workers", worker_info=wi)
        from sqlalchemy import text

        await session.execute(
            text(
                "UPDATE polyqueue_workers SET last_heartbeat_at = now() - interval '120 seconds',"
                " status = 'stopped'"
                " WHERE worker_id = :wid"
            ),
            {"wid": wi.worker_id},
        )
        await session.commit()
    async with session_factory() as session:
        count = await reap_stale_workers(
            session,
            table="polyqueue_workers",
            stale_threshold_seconds=60,
        )
        await session.commit()
    assert count == 0


@pytest.mark.asyncio
async def test_list_workers_no_filter(session_factory):
    wi1 = make_worker_info("list-1")
    wi2 = make_worker_info("list-2")
    async with session_factory() as session:
        await register_worker(session, table="polyqueue_workers", worker_info=wi1)
        await register_worker(session, table="polyqueue_workers", worker_info=wi2)
        await session.commit()
    async with session_factory() as session:
        workers = await list_workers(session, table="polyqueue_workers")
    ids = {w.worker_id for w in workers}
    assert wi1.worker_id in ids
    assert wi2.worker_id in ids


@pytest.mark.asyncio
async def test_list_workers_status_filter(session_factory):
    wi_running = make_worker_info("status-running")
    wi_stopped = make_worker_info("status-stopped")
    async with session_factory() as session:
        await register_worker(
            session, table="polyqueue_workers", worker_info=wi_running
        )
        await register_worker(
            session, table="polyqueue_workers", worker_info=wi_stopped
        )
        await deregister_worker(
            session, table="polyqueue_workers", worker_id=wi_stopped.worker_id
        )
        await session.commit()
    async with session_factory() as session:
        running = await list_workers(
            session, table="polyqueue_workers", status="running"
        )
        stopped = await list_workers(
            session, table="polyqueue_workers", status="stopped"
        )
    running_ids = {w.worker_id for w in running}
    stopped_ids = {w.worker_id for w in stopped}
    assert wi_running.worker_id in running_ids
    assert wi_stopped.worker_id not in running_ids
    assert wi_stopped.worker_id in stopped_ids
    assert wi_running.worker_id not in stopped_ids
