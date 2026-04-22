"""Tests for mark_succeeded_with_result()."""

import uuid

import pytest

from polyqueue.queue.db import (
    claim_job,
    insert_job,
    mark_succeeded_with_result,
)


@pytest.mark.asyncio
async def test_mark_succeeded_with_result_persists_result(session_factory):
    from sqlalchemy import text

    job_id = str(uuid.uuid4())
    table = "jobs"

    async with session_factory() as session:
        await insert_job(
            session,
            table=table,
            job_id=job_id,
            job_type="test",
            payload={"x": 1},
            max_attempts=3,
        )
        await session.commit()

    async with session_factory() as session:
        claim = await claim_job(session, table=table, job_id=job_id)
        await session.commit()
    assert claim is not None

    result_data = {"total": 42}
    async with session_factory() as session:
        ok = await mark_succeeded_with_result(
            session,
            table=table,
            job_id=job_id,
            lease_token=claim.lease_token,
            result=result_data,
        )
        await session.commit()

    assert ok is True

    async with session_factory() as session:
        row = await session.execute(
            text("SELECT status, result, finished_at FROM jobs WHERE id = :id"),
            {"id": job_id},
        )
        record = row.mappings().one()

    assert record["status"] == "succeeded"
    assert record["result"] == result_data
    assert record["finished_at"] is not None


@pytest.mark.asyncio
async def test_mark_succeeded_with_result_none_result(session_factory):
    from sqlalchemy import text

    job_id = str(uuid.uuid4())
    table = "jobs"

    async with session_factory() as session:
        await insert_job(
            session,
            table=table,
            job_id=job_id,
            job_type="test",
            payload={},
            max_attempts=3,
        )
        await session.commit()

    async with session_factory() as session:
        claim = await claim_job(session, table=table, job_id=job_id)
        await session.commit()
    assert claim is not None

    async with session_factory() as session:
        ok = await mark_succeeded_with_result(
            session,
            table=table,
            job_id=job_id,
            lease_token=claim.lease_token,
            result=None,
        )
        await session.commit()

    assert ok is True

    async with session_factory() as session:
        row = await session.execute(
            text("SELECT status, result FROM jobs WHERE id = :id"),
            {"id": job_id},
        )
        record = row.mappings().one()

    assert record["status"] == "succeeded"
    assert record["result"] is None


@pytest.mark.asyncio
async def test_mark_succeeded_with_result_returns_false_if_not_processing(
    session_factory,
):
    job_id = str(uuid.uuid4())
    table = "jobs"

    async with session_factory() as session:
        await insert_job(
            session,
            table=table,
            job_id=job_id,
            job_type="test",
            payload={},
            max_attempts=3,
        )
        await session.commit()

    # Job is 'queued', not 'processing'. Any lease_token matches zero rows.
    async with session_factory() as session:
        ok = await mark_succeeded_with_result(
            session,
            table=table,
            job_id=job_id,
            lease_token=uuid.uuid4(),
            result={"x": 1},
        )
        await session.commit()

    assert ok is False
