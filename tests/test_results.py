"""Unit tests for get_job_result* helpers."""

import uuid

import pytest
from pydantic import BaseModel

from polyqueue.queue.db import claim_job, insert_job, mark_succeeded_with_result
from polyqueue.results import (
    JobNotFoundError,
    JobNotSucceededError,
    get_job_result,
    get_job_result_typed,
    get_job_result_typed_via_registry,
)


class SumResult(BaseModel):
    total: int


async def _setup_succeeded_job(
    session_factory, result, *, job_type: str = "sum"
) -> str:
    """Helper: insert → claim → mark_succeeded_with_result. Returns job_id."""
    job_id = str(uuid.uuid4())
    table = "jobs"

    async with session_factory() as session:
        await insert_job(
            session,
            table=table,
            job_id=job_id,
            job_type=job_type,
            payload={"a": 1, "b": 2},
            max_attempts=3,
        )
        await session.commit()

    async with session_factory() as session:
        claim = await claim_job(session, table=table, job_id=job_id)
        await session.commit()
    assert claim is not None

    async with session_factory() as session:
        await mark_succeeded_with_result(
            session,
            table=table,
            job_id=job_id,
            lease_token=claim.lease_token,
            result=result,
        )
        await session.commit()

    return job_id


@pytest.mark.asyncio
async def test_get_job_result_returns_raw_dict(session_factory):
    job_id = await _setup_succeeded_job(session_factory, {"total": 99})
    async with session_factory() as session:
        result = await get_job_result(session, table="jobs", job_id=job_id)
    assert result == {"total": 99}


@pytest.mark.asyncio
async def test_get_job_result_none_result(session_factory):
    job_id = await _setup_succeeded_job(session_factory, None)
    async with session_factory() as session:
        result = await get_job_result(session, table="jobs", job_id=job_id)
    assert result is None


@pytest.mark.asyncio
async def test_get_job_result_raises_job_not_found(session_factory):
    async with session_factory() as session:
        with pytest.raises(JobNotFoundError, match="not found"):
            await get_job_result(session, table="jobs", job_id="does-not-exist")


@pytest.mark.asyncio
async def test_get_job_result_raises_job_not_succeeded_when_queued(session_factory):
    job_id = str(uuid.uuid4())
    table = "jobs"

    async with session_factory() as session:
        await insert_job(
            session,
            table=table,
            job_id=job_id,
            job_type="sum",
            payload={},
            max_attempts=3,
        )
        await session.commit()

    async with session_factory() as session:
        with pytest.raises(JobNotSucceededError, match="queued"):
            await get_job_result(session, table=table, job_id=job_id)


@pytest.mark.asyncio
async def test_get_job_result_typed_returns_model(session_factory):
    job_id = await _setup_succeeded_job(session_factory, {"total": 7})
    async with session_factory() as session:
        result = await get_job_result_typed(
            session, table="jobs", job_id=job_id, result_model=SumResult
        )
    assert isinstance(result, SumResult)
    assert result.total == 7


@pytest.mark.asyncio
async def test_get_job_result_typed_none_result_returns_none(session_factory):
    job_id = await _setup_succeeded_job(session_factory, None)
    async with session_factory() as session:
        result = await get_job_result_typed(
            session,
            table="jobs",
            job_id=job_id,
            result_model=SumResult,
        )
    assert result is None


@pytest.mark.asyncio
async def test_get_job_result_typed_via_registry(session_factory):
    from polyqueue.job_api import make_job
    from polyqueue.jobs.dispatcher import _HANDLERS

    _HANDLERS.clear()

    async def handle_sum(ctx, payload) -> SumResult:  # type: ignore[no-untyped-def]
        return SumResult(total=3)

    make_job("sum", handle_sum)

    job_id = await _setup_succeeded_job(session_factory, {"total": 55}, job_type="sum")

    async with session_factory() as session:
        result = await get_job_result_typed_via_registry(
            session, table="jobs", job_id=job_id
        )

    assert isinstance(result, SumResult)
    assert result.total == 55
    _HANDLERS.clear()


@pytest.mark.asyncio
async def test_get_job_result_typed_via_registry_no_result_model(session_factory):
    """Falls back to raw dict when handler has no result_model."""
    from polyqueue.job_api import make_job
    from polyqueue.jobs.dispatcher import _HANDLERS

    _HANDLERS.clear()

    async def handle_raw(ctx, payload) -> None:  # type: ignore[no-untyped-def]
        pass

    make_job("raw_sum", handle_raw)

    job_id = await _setup_succeeded_job(
        session_factory, {"total": 11}, job_type="raw_sum"
    )
    async with session_factory() as session:
        result = await get_job_result_typed_via_registry(
            session, table="jobs", job_id=job_id
        )

    assert result == {"total": 11}
    _HANDLERS.clear()
