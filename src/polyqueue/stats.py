"""Queue statistics — snapshot queries for autoscaling and monitoring.

All functions accept a live ``AsyncSession`` and table names as strings so they
can be used against any schema without requiring a settings object.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from pydantic import BaseModel
from sqlalchemy import extract, func, select

from polyqueue.tables import make_jobs_table, make_workers_table, parse_qualified

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class JobTypeCount(BaseModel):
    job_type: str
    count: int


class QueueSnapshot(BaseModel):
    queued_jobs: int = 0
    processing_jobs: int = 0
    succeeded_jobs: int = 0
    failed_jobs: int = 0
    oldest_queued_age_seconds: float | None = None
    timed_out_processing_jobs: int = 0
    stale_progress_jobs: int = 0
    healthy_workers: int = 0
    running_workers: int = 0


async def get_job_type_counts(
    session: AsyncSession,
    *,
    jobs_table: str,
    status: str,
) -> list[JobTypeCount]:
    """Count jobs grouped by job_type for a given status.

    Returns:
        List ordered by count desc.
    """
    schema, name = parse_qualified(jobs_table)
    j = make_jobs_table(name=name, schema=schema)
    stmt = (
        select(j.c.job_type, func.count().label("count"))
        .where(j.c.status == status)
        .group_by(j.c.job_type)
        .order_by(func.count().desc())
    )
    result = await session.execute(stmt)
    return [
        JobTypeCount(job_type=row["job_type"], count=row["count"])
        for row in result.mappings()
    ]


async def get_queue_snapshot(
    session: AsyncSession,
    *,
    jobs_table: str,
    workers_table: str,
    stale_progress_seconds: int = 120,
    worker_stale_seconds: int = 60,
) -> QueueSnapshot:
    """Compute a full queue snapshot in a single DB round-trip.

    Args:
        jobs_table: Fully-qualified jobs table, e.g. ``"public.polyqueue_jobs"``.
        workers_table: Fully-qualified workers table, e.g. ``"public.polyqueue_workers"``.
        stale_progress_seconds: A processing job is considered stale if its
            ``progress_heartbeat_at`` has not been updated within this many seconds.
        worker_stale_seconds: A worker is considered unhealthy if its
            ``last_heartbeat_at`` is older than this many seconds.
    """
    jschema, jname = parse_qualified(jobs_table)
    wschema, wname = parse_qualified(workers_table)
    j = make_jobs_table(name=jname, schema=jschema)
    w = make_workers_table(name=wname, schema=wschema)

    now = func.now()
    stale_progress_delta = timedelta(seconds=stale_progress_seconds)
    worker_stale_delta = timedelta(seconds=worker_stale_seconds)

    queued_count = select(func.count()).where(j.c.status == "queued").scalar_subquery()
    processing_count = (
        select(func.count()).where(j.c.status == "processing").scalar_subquery()
    )
    succeeded_count = (
        select(func.count()).where(j.c.status == "succeeded").scalar_subquery()
    )
    failed_count = select(func.count()).where(j.c.status == "failed").scalar_subquery()
    oldest_queued_age = (
        select(extract("epoch", now - func.min(j.c.created_at)))
        .where(j.c.status == "queued")
        .scalar_subquery()
    )
    timed_out_count = (
        select(func.count())
        .where(
            j.c.status == "processing",
            j.c.timeout_at.is_not(None),
            j.c.timeout_at < now,
        )
        .scalar_subquery()
    )
    stale_progress_count = (
        select(func.count())
        .where(
            j.c.status == "processing",
            j.c.progress_heartbeat_at < now - stale_progress_delta,
        )
        .scalar_subquery()
    )
    healthy_workers_count = (
        select(func.count())
        .where(
            w.c.status == "running",
            w.c.last_heartbeat_at >= now - worker_stale_delta,
        )
        .scalar_subquery()
    )
    running_workers_count = (
        select(func.count()).where(w.c.status == "running").scalar_subquery()
    )

    stmt = select(
        queued_count.label("queued_jobs"),
        processing_count.label("processing_jobs"),
        succeeded_count.label("succeeded_jobs"),
        failed_count.label("failed_jobs"),
        oldest_queued_age.label("oldest_queued_age_seconds"),
        timed_out_count.label("timed_out_processing_jobs"),
        stale_progress_count.label("stale_progress_jobs"),
        healthy_workers_count.label("healthy_workers"),
        running_workers_count.label("running_workers"),
    )
    data = (await session.execute(stmt)).mappings().one()
    return QueueSnapshot(
        queued_jobs=data["queued_jobs"] or 0,
        processing_jobs=data["processing_jobs"] or 0,
        succeeded_jobs=data["succeeded_jobs"] or 0,
        failed_jobs=data["failed_jobs"] or 0,
        oldest_queued_age_seconds=data["oldest_queued_age_seconds"],
        timed_out_processing_jobs=data["timed_out_processing_jobs"] or 0,
        stale_progress_jobs=data["stale_progress_jobs"] or 0,
        healthy_workers=data["healthy_workers"] or 0,
        running_workers=data["running_workers"] or 0,
    )
