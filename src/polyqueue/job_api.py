"""Typed job API — single entry point for enqueue, status, and result fetch.

``make_job`` and ``job_spec`` are the two factory functions:

``make_job`` (worker side or simple single-process apps):
    Registers the handler for dispatch AND returns a typed ``JobCommands``
    in one step. Use this when handler code is in scope::

        # handlers.py
        add = make_job("add", handle_add)   # JobCommands[AddPayload, AddResult]

``job_spec`` (enqueue side / web servers / CLIs):
    Returns a typed ``JobCommands`` without registering any handler.
    Use this when you only need to enqueue jobs or query their status —
    no handler code required, so no heavy worker dependencies are pulled in::

        # job_defs.py
        add = job_spec("add", AddPayload, AddResult)   # JobCommands[AddPayload, AddResult]
        greet = job_spec("greet", GreetPayload)        # JobCommands[GreetPayload, None]

Both produce a ``JobCommands[P, R]`` with the same interface::

    result = await add.enqueue(queue, AddPayload(a=1, b=2))
    status = await add.get_status(session, table=..., job_id=result.job_id)
    value  = await add.get_result(session, table=..., job_id=result.job_id)
    # value: AddResult | None
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, cast, overload

from pydantic import BaseModel

from polyqueue.enqueue_options import EnqueueOptions, EnqueueResult  # noqa: TC001
from polyqueue.jobs.context import JobContext  # noqa: TC001
from polyqueue.jobs.dispatcher import _register, infer_result_model
from polyqueue.queue.interface import JobQueue  # noqa: TC001
from polyqueue.results import (
    JobStatus,
    get_job_result,
    get_job_result_typed,
    get_job_status,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class JobCommands[P: BaseModel, R]:
    """Typed accessor for a registered job handler.

    Created by ``make_job`` or ``job_spec``; provides ``enqueue``,
    ``get_status``, and ``get_result`` — all typed from the payload
    and result annotations.

    Enqueues by job type string, so it does not need the handler function
    to be importable in the enqueuing process.
    """

    _job_type: str
    _result_model: type[BaseModel] | None

    def __init__(
        self,
        job_type: str,
        result_model: type[BaseModel] | None = None,
    ) -> None:
        self._job_type = job_type
        self._result_model = result_model

    async def enqueue(
        self,
        queue: JobQueue,
        payload: P,
        *,
        options: EnqueueOptions | None = None,
    ) -> EnqueueResult:
        """Enqueue a job and return an EnqueueResult with the job_id."""
        return await queue.enqueue(self._job_type, payload, options=options)

    async def get_status(
        self,
        session: AsyncSession,
        *,
        table: str,
        job_id: str,
    ) -> JobStatus:
        """Return a rich status snapshot for a job.

        Raises:
            JobNotFoundError: if the job does not exist.
        """
        return await get_job_status(session, table=table, job_id=job_id)

    async def get_result(
        self,
        session: AsyncSession,
        *,
        table: str,
        job_id: str,
    ) -> R | None:
        """Fetch the result of a succeeded job.

        If the handler declared a Pydantic return type, the result is
        deserialised via ``model_validate`` and returned as that type.
        Otherwise the raw JSON value is returned.

        Returns ``None`` if the stored result is ``NULL``.

        Raises:
            JobNotFoundError: if the job does not exist.
            JobNotSucceededError: if the job has not yet succeeded.
        """
        if self._result_model is not None:
            raw = await get_job_result_typed(
                session, table=table, job_id=job_id, result_model=self._result_model
            )
        else:
            raw = await get_job_result(session, table=table, job_id=job_id)
        return cast("R | None", raw)


def make_job[P: BaseModel, R](
    job_type: str,
    handler: Callable[[JobContext, P], Awaitable[R]],
) -> JobCommands[P, R]:
    """Register a handler and return a typed ``JobCommands``.

    Registers the handler under ``job_type`` for worker dispatch and returns
    a command object with typed ``enqueue``, ``get_status``, and ``get_result``.
    Both ``P`` and ``R`` are inferred from the handler's annotations::

        add = make_job("add", handle_add)   # JobCommands[AddPayload, AddResult]

    Use this on the **worker side** (or in single-process apps) where the
    handler implementation is available. For the enqueue side only, prefer
    ``job_spec`` to avoid pulling in worker dependencies.
    """
    _register(job_type, handler)
    return JobCommands(job_type, infer_result_model(handler))


@overload
def job_spec[P: BaseModel, R: BaseModel](
    job_type: str,
    payload_type: type[P],
    result_type: type[R],
) -> JobCommands[P, R]: ...


@overload
def job_spec[P: BaseModel](
    job_type: str,
    payload_type: type[P],
    result_type: None = ...,
) -> JobCommands[P, None]: ...


def job_spec(
    job_type: str,
    payload_type: type[Any],
    result_type: type[BaseModel] | None = None,
) -> JobCommands[Any, Any]:
    """Create a typed ``JobCommands`` without registering a handler.

    Use this on the **enqueue side** (web servers, CLIs, scripts) where you
    only need to push jobs and query their status — no handler code is
    required, so no worker dependencies are imported::

        # job_defs.py — safe to import anywhere
        add   = job_spec("add",   AddPayload, AddResult)
        greet = job_spec("greet", GreetPayload)

    The job type string must match the one used in ``make_job`` on the
    worker side — ``job_defs.py`` is the canonical source of those strings.

    ``P`` is inferred from ``payload_type`` and ``R`` from ``result_type``
    (``None`` when omitted).
    """
    if result_type is not None and not issubclass(result_type, BaseModel):
        raise TypeError(
            "job_spec result_type must be a pydantic BaseModel subclass or None"
        )
    return JobCommands(job_type, result_type)


def register_handler[P: BaseModel, R](
    commands: JobCommands[P, R],
) -> Callable[
    [Callable[[JobContext, P], Awaitable[R]]], Callable[[JobContext, P], Awaitable[R]]
]:
    """Decorator that registers a handler against an existing ``JobCommands``.

    Use this on the **worker side** when ``job_spec`` is the canonical source
    of job type strings.  The handler is registered under the same job type
    as the ``JobCommands`` object, eliminating the need to repeat the string::

        # job_defs.py
        add = job_spec("add", AddPayload, AddResult)

        # handlers.py
        @register_handler(add)
        async def _handle_add(ctx: JobContext, payload: AddPayload) -> AddResult:
            ...

    The decorated function is returned unchanged so it can still be called
    directly in tests.
    """

    def decorator(
        fn: Callable[[JobContext, P], Awaitable[R]],
    ) -> Callable[[JobContext, P], Awaitable[R]]:
        _register(commands._job_type, fn)
        return fn

    return decorator
