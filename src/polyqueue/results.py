"""Result fetch API — retrieve stored job results from Postgres.

Usage::

    from polyqueue.results import get_job_result, get_job_result_typed, JobNotFoundError

    async with session_factory() as session:
        raw = await get_job_result(session, table="public.polyqueue_jobs", job_id=job_id)
        typed = await get_job_result_typed(session, table=..., job_id=..., result_model=MyModel)
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, ConfigDict
from sqlalchemy import select

from polyqueue.tables import make_jobs_table, parse_qualified

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class JobStatus(BaseModel):
    """Rich status snapshot for a job row.

    Returned by :func:`get_job_status` and :meth:`JobCommands.get_status`.
    """

    model_config = ConfigDict(frozen=True)

    job_id: str
    job_type: str
    status: str  # queued | processing | succeeded | failed
    attempt: int
    max_attempts: int
    queued_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    worker_id: str | None = None
    worker_hostname: str | None = None
    worker_pid: int | None = None
    error_code: str | None = None
    last_error: str | None = None


class JobResultError(Exception):
    """Base class for result fetch errors."""


class JobNotFoundError(JobResultError):
    """Raised when the job_id does not exist in the jobs table."""


class JobNotSucceededError(JobResultError):
    """Raised when the job exists but has not reached status='succeeded'.

    Args:
        status: The current job status (e.g. 'queued', 'processing', 'failed').
    """

    job_id: str
    status: str

    def __init__(self, job_id: str, status: str) -> None:
        self.job_id = job_id
        self.status = status
        super().__init__(
            f"Job {job_id!r} has not succeeded yet (current status: {status!r})."
        )


async def get_job_status(
    session: AsyncSession,
    *,
    table: str,
    job_id: str,
) -> JobStatus:
    """Fetch a rich status snapshot for a job.

    Returns:
        A :class:`JobStatus` with status, worker info, timing, and error fields.

    Raises:
        JobNotFoundError: if the job_id does not exist.
    """
    schema, name = parse_qualified(table)
    t = make_jobs_table(name=name, schema=schema)
    stmt = select(
        t.c.id,
        t.c.job_type,
        t.c.status,
        t.c.attempt_count,
        t.c.max_attempts,
        t.c.created_at,
        t.c.started_at,
        t.c.finished_at,
        t.c.claimed_by_worker_id,
        t.c.claimed_by_hostname,
        t.c.claimed_by_pid,
        t.c.error_code,
        t.c.last_error,
    ).where(t.c.id == job_id)
    row = await session.execute(stmt)
    record = row.mappings().one_or_none()
    if record is None:
        raise JobNotFoundError(f"Job {job_id!r} not found in {table!r}")
    return JobStatus(
        job_id=str(record["id"]),
        job_type=str(record["job_type"]),
        status=str(record["status"]),
        attempt=int(record["attempt_count"]),
        max_attempts=int(record["max_attempts"]),
        queued_at=record["created_at"],
        started_at=record["started_at"],
        finished_at=record["finished_at"],
        worker_id=record["claimed_by_worker_id"],
        worker_hostname=record["claimed_by_hostname"],
        worker_pid=record["claimed_by_pid"],
        error_code=record["error_code"],
        last_error=record["last_error"],
    )


async def get_job_result(
    session: AsyncSession,
    *,
    table: str,
    job_id: str,
) -> Any:
    """Fetch the raw result value for a succeeded job.

    Args:
        table: Fully-qualified table reference, e.g. ``"public.polyqueue_jobs"``.
        job_id: The job identifier.

    Returns:
        The value stored in ``jobs.result`` — a dict, list, primitive, or None.

    Raises:
        JobNotFoundError: if the job_id does not exist.
        JobNotSucceededError: if the job exists but status != 'succeeded'.
    """
    schema, name = parse_qualified(table)
    t = make_jobs_table(name=name, schema=schema)
    stmt = select(t.c.status, t.c.result).where(t.c.id == job_id)
    row = await session.execute(stmt)
    record = row.mappings().one_or_none()

    if record is None:
        raise JobNotFoundError(f"Job {job_id!r} not found in {table!r}")

    if record["status"] != "succeeded":
        raise JobNotSucceededError(job_id, str(record["status"]))

    return record["result"]


async def get_job_result_typed[T: BaseModel](
    session: AsyncSession,
    *,
    table: str,
    job_id: str,
    result_model: type[T],
) -> T | None:
    """Fetch and deserialise the result for a succeeded job.

    Returns None if the stored result is NULL. Otherwise calls
    ``result_model.model_validate(raw)`` and returns the model instance.

    Raises:
        JobNotFoundError: if the job_id does not exist.
        JobNotSucceededError: if the job has not succeeded.
        pydantic.ValidationError: if the stored result doesn't match the model.
    """
    raw = await get_job_result(session, table=table, job_id=job_id)
    if raw is None:
        return None
    return result_model.model_validate(raw)


async def get_job_result_typed_via_registry(
    session: AsyncSession,
    *,
    table: str,
    job_id: str,
) -> Any:
    """Fetch and deserialise a result using the registered handler's result_model.

    Fetches ``job_type`` from the DB row (no need to pass it from the caller).
    If the handler declared a Pydantic return type, deserialises via that model.
    Otherwise returns the raw value.

    Note:
        This relies on handler registrations in the *current process*. If the
        caller (e.g. a web app) has not imported the handler modules, the registry
        will be empty and raw JSON is returned — not an error. Import handlers
        before calling this if typed results are required.

    Raises:
        JobNotFoundError: if the job_id does not exist.
        JobNotSucceededError: if the job has not succeeded.
        pydantic.ValidationError: if result doesn't match the registered model.
    """
    from polyqueue.jobs.dispatcher import _HANDLERS

    schema, name = parse_qualified(table)
    t = make_jobs_table(name=name, schema=schema)
    stmt = select(t.c.status, t.c.result, t.c.job_type).where(t.c.id == job_id)
    row = await session.execute(stmt)
    record = row.mappings().one_or_none()

    if record is None:
        raise JobNotFoundError(f"Job {job_id!r} not found in {table!r}")

    if record["status"] != "succeeded":
        raise JobNotSucceededError(job_id, str(record["status"]))

    raw = record["result"]
    if raw is None:
        return None

    job_type: str = record["job_type"]
    handler = _HANDLERS.get(job_type)
    if handler is None or handler.result_model is None:
        return raw

    return handler.result_model.model_validate(raw)
