"""Tests for make_job() and JobCommands."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from polyqueue.jobs.context import JobContext
from polyqueue.jobs.dispatcher import _HANDLERS, _HANDLERS_BY_FN


class AddPayload(BaseModel):
    a: int
    b: int


class AddResult(BaseModel):
    sum: int


@pytest.fixture(autouse=True)
def _clear_registry():
    _HANDLERS.clear()
    _HANDLERS_BY_FN.clear()
    yield
    _HANDLERS.clear()
    _HANDLERS_BY_FN.clear()


def _mock_session(record: dict | None) -> MagicMock:
    """Return a mock AsyncSession that yields ``record`` from execute()."""
    session = MagicMock()
    cursor = MagicMock()
    cursor.mappings.return_value.one_or_none.return_value = record
    session.execute = AsyncMock(return_value=cursor)
    return session


# ── construction ──────────────────────────────────────────────────────────────


def test_make_job_registers_handler_and_returns_commands() -> None:
    from polyqueue.job_api import JobCommands, make_job

    async def handle_add(ctx: JobContext, payload: AddPayload) -> AddResult:
        return AddResult(sum=payload.a + payload.b)

    cmd = make_job("add", handle_add)

    assert isinstance(cmd, JobCommands)
    assert cmd._job_type == "add"
    assert "add" in _HANDLERS


def test_make_job_infers_result_model() -> None:
    from polyqueue.job_api import make_job

    async def handle_add(ctx: JobContext, payload: AddPayload) -> AddResult:
        return AddResult(sum=payload.a + payload.b)

    cmd = make_job("add", handle_add)
    assert cmd._result_model is AddResult


def test_make_job_result_model_none_for_void_handler() -> None:
    from polyqueue.job_api import make_job

    async def handle_add(ctx: JobContext, payload: AddPayload) -> None:
        pass

    cmd = make_job("add", handle_add)
    assert cmd._result_model is None


def test_job_spec_returns_commands_without_registering_handler() -> None:
    from polyqueue.job_api import JobCommands, job_spec

    cmd = job_spec("add", AddPayload, AddResult)

    assert isinstance(cmd, JobCommands)
    assert cmd._job_type == "add"
    assert cmd._result_model is AddResult
    assert "add" not in _HANDLERS


def test_job_spec_rejects_non_pydantic_result_type() -> None:
    from polyqueue.job_api import job_spec

    with pytest.raises(TypeError, match="result_type must be a pydantic BaseModel"):
        job_spec("add", AddPayload, int)  # type: ignore[arg-type]


def test_register_handler_decorator_registers_handler() -> None:
    from polyqueue.job_api import job_spec, register_handler

    cmd = job_spec("add", AddPayload, AddResult)

    @register_handler(cmd)
    async def handle_add(ctx: JobContext, payload: AddPayload) -> AddResult:
        return AddResult(sum=payload.a + payload.b)

    assert "add" in _HANDLERS
    assert _HANDLERS["add"].fn is handle_add
    assert _HANDLERS["add"].payload_model is AddPayload
    assert _HANDLERS["add"].result_model is AddResult


# ── enqueue ───────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_enqueue_returns_enqueue_result() -> None:
    from polyqueue.enqueue_options import EnqueueResult
    from polyqueue.job_api import make_job

    async def handle_add(ctx: JobContext, payload: AddPayload) -> AddResult:
        return AddResult(sum=payload.a + payload.b)

    cmd = make_job("add", handle_add)
    mock_queue = AsyncMock()
    mock_queue.enqueue.return_value = EnqueueResult(job_id="generated-uuid")

    result = await cmd.enqueue(mock_queue, AddPayload(a=1, b=2))

    assert isinstance(result, EnqueueResult)
    assert result.job_id == "generated-uuid"


@pytest.mark.asyncio
async def test_enqueue_delegates_to_queue() -> None:
    from polyqueue.enqueue_options import EnqueueOptions
    from polyqueue.job_api import make_job

    async def handle_add(ctx: JobContext, payload: AddPayload) -> AddResult:
        return AddResult(sum=payload.a + payload.b)

    cmd = make_job("add", handle_add)
    mock_queue = AsyncMock()
    payload = AddPayload(a=1, b=2)
    opts = EnqueueOptions(max_run_seconds=5)

    await cmd.enqueue(mock_queue, payload, options=opts)

    # JobCommands enqueues by job_type string, not by handler fn reference,
    # so the enqueue side never needs to import the handler module.
    mock_queue.enqueue.assert_called_once_with("add", payload, options=opts)


@pytest.mark.asyncio
async def test_job_spec_enqueue_delegates_to_queue_by_job_type() -> None:
    from polyqueue.enqueue_options import EnqueueOptions
    from polyqueue.job_api import job_spec

    cmd = job_spec("add", AddPayload, AddResult)
    mock_queue = AsyncMock()
    payload = AddPayload(a=1, b=2)
    opts = EnqueueOptions(max_run_seconds=5)

    await cmd.enqueue(mock_queue, payload, options=opts)

    mock_queue.enqueue.assert_called_once_with("add", payload, options=opts)


@pytest.mark.asyncio
async def test_inprocess_queue_dispatches_registered_job_spec_handler() -> None:
    import asyncio

    from polyqueue.job_api import job_spec, register_handler
    from polyqueue.queue.inprocess_queue import InProcessQueue

    cmd = job_spec("add", AddPayload, AddResult)
    called = asyncio.Event()
    got: dict[str, int] = {}

    @register_handler(cmd)
    async def handle_add(ctx: JobContext, payload: AddPayload) -> AddResult:
        got["sum"] = payload.a + payload.b
        called.set()
        return AddResult(sum=got["sum"])

    queue = InProcessQueue(max_attempts=1, retry_backoff_seconds=[0])
    await cmd.enqueue(queue, AddPayload(a=2, b=3))

    await asyncio.wait_for(called.wait(), timeout=1.0)
    assert got["sum"] == 5

    await queue.close()


# ── get_status ────────────────────────────────────────────────────────────────

_FULL_STATUS_RECORD = {
    "id": "j1",
    "job_type": "add",
    "status": "processing",
    "attempt_count": 2,
    "max_attempts": 3,
    "created_at": None,
    "started_at": None,
    "finished_at": None,
    "claimed_by_worker_id": "w1",
    "claimed_by_hostname": "host1",
    "claimed_by_pid": 1234,
    "error_code": None,
    "last_error": None,
}


@pytest.mark.asyncio
async def test_get_status_returns_job_status_model() -> None:
    from polyqueue.job_api import make_job
    from polyqueue.results import JobStatus

    async def handle_add(ctx: JobContext, payload: AddPayload) -> AddResult:
        return AddResult(sum=payload.a + payload.b)

    cmd = make_job("add", handle_add)
    session = _mock_session(_FULL_STATUS_RECORD)
    result = await cmd.get_status(session, table="public.jobs", job_id="j1")

    assert isinstance(result, JobStatus)
    assert result.status == "processing"
    assert result.job_id == "j1"
    assert result.job_type == "add"
    assert result.attempt == 2
    assert result.max_attempts == 3
    assert result.worker_id == "w1"
    assert result.worker_hostname == "host1"
    assert result.worker_pid == 1234


@pytest.mark.asyncio
async def test_get_status_reflects_all_status_values() -> None:
    from polyqueue.job_api import make_job
    from polyqueue.results import JobStatus

    async def handle_add(ctx: JobContext, payload: AddPayload) -> AddResult:
        return AddResult(sum=payload.a + payload.b)

    cmd = make_job("add", handle_add)

    for status_val in ("queued", "processing", "succeeded", "failed"):
        record = {**_FULL_STATUS_RECORD, "status": status_val}
        session = _mock_session(record)
        result = await cmd.get_status(session, table="public.jobs", job_id="j1")
        assert isinstance(result, JobStatus)
        assert result.status == status_val


@pytest.mark.asyncio
async def test_get_status_raises_on_missing_job() -> None:
    from polyqueue.job_api import make_job
    from polyqueue.results import JobNotFoundError

    async def handle_add(ctx: JobContext, payload: AddPayload) -> AddResult:
        return AddResult(sum=payload.a + payload.b)

    cmd = make_job("add", handle_add)
    with pytest.raises(JobNotFoundError):
        session = _mock_session(None)
        await cmd.get_status(session, table="public.jobs", job_id="missing")


# ── get_result ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_result_returns_typed_model() -> None:
    from polyqueue.job_api import make_job

    async def handle_add(ctx: JobContext, payload: AddPayload) -> AddResult:
        return AddResult(sum=payload.a + payload.b)

    cmd = make_job("add", handle_add)
    session = _mock_session({"status": "succeeded", "result": {"sum": 3}})

    result = await cmd.get_result(session, table="public.jobs", job_id="j1")

    assert isinstance(result, AddResult)
    assert result.sum == 3


@pytest.mark.asyncio
async def test_get_result_returns_none_when_result_is_null() -> None:
    from polyqueue.job_api import make_job

    async def handle_add(ctx: JobContext, payload: AddPayload) -> AddResult:
        return AddResult(sum=0)

    cmd = make_job("add", handle_add)
    session = _mock_session({"status": "succeeded", "result": None})

    result = await cmd.get_result(session, table="public.jobs", job_id="j1")

    assert result is None


@pytest.mark.asyncio
async def test_get_result_raw_when_handler_returns_none_type() -> None:
    from polyqueue.job_api import make_job

    async def handle_add(ctx: JobContext, payload: AddPayload) -> None:
        pass

    cmd = make_job("add", handle_add)
    session = _mock_session({"status": "succeeded", "result": None})

    result = await cmd.get_result(session, table="public.jobs", job_id="j1")

    assert result is None


@pytest.mark.asyncio
async def test_get_result_raises_if_job_not_succeeded() -> None:
    from polyqueue.job_api import make_job
    from polyqueue.results import JobNotSucceededError

    async def handle_add(ctx: JobContext, payload: AddPayload) -> AddResult:
        return AddResult(sum=0)

    cmd = make_job("add", handle_add)
    session = _mock_session({"status": "processing", "result": None})

    with pytest.raises(JobNotSucceededError):
        await cmd.get_result(session, table="public.jobs", job_id="j1")
