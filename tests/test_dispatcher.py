"""Tests for the dispatcher's Pydantic-aware registration and dispatch."""

import pytest
from pydantic import BaseModel

from polyqueue.jobs.context import JobContext
from polyqueue.jobs.dispatcher import (
    _HANDLERS,
    _HANDLERS_BY_FN,
    TerminalError,
    _infer_payload_model,
    _register,
    dispatch,
    infer_result_model,
    job_type_for_handler,
)

# ── Fixtures ──────────────────────────────────────────────────────────────────


class AddPayload(BaseModel):
    a: int
    b: int


class GreetPayload(BaseModel):
    name: str
    fail: bool = False


@pytest.fixture(autouse=True)
def _clear_registry():
    """Ensure a clean handler registry for every test."""
    _HANDLERS.clear()
    _HANDLERS_BY_FN.clear()
    yield
    _HANDLERS.clear()
    _HANDLERS_BY_FN.clear()


def _make_ctx(job_type: str = "test") -> JobContext:
    return JobContext.make(
        job_id="test-id",
        job_type=job_type,
        attempt=1,
        max_attempts=3,
        session_factory=None,
    )


# ── _infer_payload_model ─────────────────────────────────────────────────────


def test_infer_pydantic_model() -> None:
    async def handler(ctx: JobContext, payload: AddPayload) -> None: ...

    assert _infer_payload_model(handler) is AddPayload


def test_infer_optional_pydantic_model() -> None:
    async def handler(ctx: JobContext, payload: AddPayload | None) -> None: ...

    assert _infer_payload_model(handler) is AddPayload


def test_infer_dict_returns_none() -> None:
    from typing import Any

    async def handler(ctx: JobContext, payload: dict[str, Any]) -> None: ...

    assert _infer_payload_model(handler) is None


def test_infer_no_annotation_returns_none() -> None:
    async def handler(ctx, payload) -> None: ...  # type: ignore[no-untyped-def]

    assert _infer_payload_model(handler) is None


# ── _register ────────────────────────────────────────────────────────────────


def test_register_stores_handler() -> None:
    async def handler(ctx: JobContext, payload: AddPayload) -> None: ...

    _register("add", handler)
    assert "add" in _HANDLERS
    assert _HANDLERS["add"].payload_model is AddPayload


def test_register_dict_payload() -> None:
    from typing import Any

    async def handler(ctx: JobContext, payload: dict[str, Any]) -> None: ...

    _register("raw", handler)
    assert _HANDLERS["raw"].payload_model is None


def test_register_stores_result_model() -> None:
    class SumResult(BaseModel):
        total: int

    async def handler(ctx: JobContext, payload: AddPayload) -> SumResult:
        return SumResult(total=payload.a + payload.b)

    _register("sum", handler)
    assert _HANDLERS["sum"].result_model is SumResult


def test_register_none_result_model_when_no_annotation() -> None:
    async def handler(ctx: JobContext, payload: AddPayload) -> None: ...

    _register("void", handler)
    assert _HANDLERS["void"].result_model is None


# ── dispatch ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_dispatch_with_pydantic() -> None:
    received = {}

    async def handler(ctx: JobContext, payload: AddPayload) -> None:
        received["a"] = payload.a
        received["b"] = payload.b

    _register("add", handler)
    await dispatch(_make_ctx("add"), {"a": 1, "b": 2})
    assert received == {"a": 1, "b": 2}


@pytest.mark.asyncio
async def test_dispatch_with_dict() -> None:
    received = {}

    async def handler(ctx: JobContext, payload: dict) -> None:  # type: ignore[type-arg]
        received.update(payload)

    _register("raw", handler)
    await dispatch(_make_ctx("raw"), {"x": 99})
    assert received == {"x": 99}


@pytest.mark.asyncio
async def test_dispatch_validation_error_is_terminal() -> None:
    async def handler(ctx: JobContext, payload: AddPayload) -> None: ...

    _register("add", handler)
    with pytest.raises(TerminalError, match="Payload validation failed"):
        await dispatch(_make_ctx("add"), {"a": "not_a_number", "b": "oops"})


@pytest.mark.asyncio
async def test_dispatch_unknown_job_type() -> None:
    with pytest.raises(TerminalError, match="Unknown job type"):
        await dispatch(_make_ctx("nope"), {})


# ── infer_result_model ───────────────────────────────────────────────────────


class SumResult(BaseModel):
    total: int


def testinfer_result_model_from_return_annotation() -> None:
    async def handler(ctx: JobContext, payload: AddPayload) -> SumResult: ...

    assert infer_result_model(handler) is SumResult


def testinfer_result_model_optional_return() -> None:
    async def handler(ctx: JobContext, payload: AddPayload) -> SumResult | None: ...

    assert infer_result_model(handler) is SumResult


def testinfer_result_model_none_return() -> None:
    async def handler(ctx: JobContext, payload: AddPayload) -> None: ...

    assert infer_result_model(handler) is None


def testinfer_result_model_dict_return() -> None:
    from typing import Any

    async def handler(ctx: JobContext, payload: AddPayload) -> dict[str, Any]: ...

    assert infer_result_model(handler) is None


def testinfer_result_model_no_annotation() -> None:
    async def handler(ctx, payload) -> None: ...  # type: ignore[no-untyped-def]

    assert infer_result_model(handler) is None


# ── dispatch() returns the handler return value ───────────────────────────────


@pytest.mark.asyncio
async def test_dispatch_returns_handler_value() -> None:
    async def handler(ctx: JobContext, payload: AddPayload) -> SumResult:
        return SumResult(total=payload.a + payload.b)

    _register("sum", handler)
    result = await dispatch(_make_ctx("sum"), {"a": 3, "b": 4})
    assert isinstance(result, SumResult)
    assert result.total == 7


@pytest.mark.asyncio
async def test_dispatch_returns_none_for_void_handler() -> None:
    async def handler(ctx: JobContext, payload: AddPayload) -> None:
        pass

    _register("void", handler)
    result = await dispatch(_make_ctx("void"), {"a": 1, "b": 2})
    assert result is None


# ── JobContext.queued_at and options ──────────────────────────────────────


def test_job_context_has_queued_at_and_options() -> None:
    from datetime import datetime, timezone

    from polyqueue.enqueue_options import EnqueueOptions

    opts = EnqueueOptions(max_run_seconds=10)
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ctx = JobContext.make(
        job_id="j1",
        job_type="add",
        attempt=1,
        max_attempts=3,
        session_factory=None,
        queued_at=now,
        options=opts,
    )
    assert ctx.queued_at == now
    assert ctx.options is opts
    assert ctx.options.max_run_seconds == 10


def test_job_context_queued_at_and_options_default_none() -> None:
    ctx = JobContext.make(
        job_id="j1",
        job_type="add",
        attempt=1,
        max_attempts=3,
        session_factory=None,
    )
    assert ctx.queued_at is None
    assert ctx.options is None


# ── job_type_for_handler ──────────────────────────────────────────────────────


def test_job_type_for_handler_registered() -> None:
    async def handle_demo(ctx: JobContext, payload: AddPayload) -> None:
        pass

    _register("demo_type", handle_demo)
    assert job_type_for_handler(handle_demo) == "demo_type"


def test_job_type_for_handler_unregistered() -> None:
    async def handle_orphan(ctx: JobContext, payload: AddPayload) -> None:
        pass

    with pytest.raises(KeyError, match="not registered"):
        job_type_for_handler(handle_orphan)
