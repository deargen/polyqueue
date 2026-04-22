"""Worker registry DB helpers — upsert, heartbeat, deregister, reap, list.

All functions accept a ``table`` keyword argument (fully-qualified table
reference, e.g. ``"public.polyqueue_workers"``) and operate within a caller-
supplied session so the caller controls transaction boundaries.
"""

from __future__ import annotations

import json
from datetime import (
    datetime,  # noqa: TC003 — needed by Pydantic for WorkerRow field resolution
)
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel
from sqlalchemy import select, text

from polyqueue.tables import make_workers_table, parse_qualified

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

    from polyqueue.worker.identity import WorkerInfo


class WorkerRow(BaseModel):
    """Typed result returned by list_workers()."""

    worker_id: str
    hostname: str
    pid: int
    started_at: datetime
    last_heartbeat_at: datetime
    backend: str
    worker_name: str
    status: str
    current_job_id: str | None
    current_job_started_at: datetime | None
    version: str | None
    metadata: dict[str, Any] | None


async def register_worker(
    session: AsyncSession,
    *,
    table: str,
    worker_info: WorkerInfo,
    backend: str = "",
    worker_name: str = "",
    version: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Upsert a worker row on startup.

    On conflict (same worker_id), refreshes heartbeat and resets status to
    'running' so a restarted worker resurfaces cleanly.
    """
    await session.execute(
        text(
            f"""
            INSERT INTO {table}
                (worker_id, hostname, pid, started_at, last_heartbeat_at,
                 backend, worker_name, status, version, metadata)
            VALUES
                (:worker_id, :hostname, :pid, :started_at, now(),
                 :backend, :worker_name, 'running', :version,
                 CAST(:metadata AS jsonb))
            ON CONFLICT (worker_id) DO UPDATE
                SET hostname          = EXCLUDED.hostname,
                    pid               = EXCLUDED.pid,
                    started_at        = EXCLUDED.started_at,
                    last_heartbeat_at = now(),
                    backend           = EXCLUDED.backend,
                    worker_name       = EXCLUDED.worker_name,
                    status            = 'running',
                    version           = EXCLUDED.version,
                    metadata          = EXCLUDED.metadata
            """
        ),
        {
            "worker_id": worker_info.worker_id,
            "hostname": worker_info.hostname,
            "pid": worker_info.pid,
            "started_at": worker_info.started_at,
            "backend": backend,
            "worker_name": worker_name,
            "version": version,
            "metadata": json.dumps(metadata) if metadata is not None else None,
        },
    )


async def heartbeat_worker(
    session: AsyncSession,
    *,
    table: str,
    worker_id: str,
    current_job_id: str | None = None,
    current_job_started_at: datetime | None = None,
) -> bool:
    """Update last_heartbeat_at and current job info for a running worker.

    Returns:
        False if no row matched (worker not registered or already stopped/dead).
    """
    result = await session.execute(
        text(
            f"""
            UPDATE {table}
            SET last_heartbeat_at      = now(),
                current_job_id         = :current_job_id,
                current_job_started_at = :current_job_started_at
            WHERE worker_id = :worker_id
              AND status = 'running'
            """
        ),
        {
            "worker_id": worker_id,
            "current_job_id": current_job_id,
            "current_job_started_at": current_job_started_at,
        },
    )
    return result.rowcount > 0  # type: ignore[union-attr]


async def deregister_worker(
    session: AsyncSession,
    *,
    table: str,
    worker_id: str,
) -> None:
    """Mark worker status='stopped' and clear current_job fields on clean shutdown."""
    await session.execute(
        text(
            f"""
            UPDATE {table}
            SET status                 = 'stopped',
                current_job_id         = NULL,
                current_job_started_at = NULL,
                last_heartbeat_at      = now()
            WHERE worker_id = :worker_id
            """
        ),
        {"worker_id": worker_id},
    )


async def reap_stale_workers(
    session: AsyncSession,
    *,
    table: str,
    stale_threshold_seconds: int,
) -> int:
    """Mark workers 'dead' if their heartbeat is stale and they are still 'running'.

    Returns:
        Number of worker rows transitioned to 'dead'.
    """
    result = await session.execute(
        text(
            f"""
            UPDATE {table}
            SET status                 = 'dead',
                current_job_id         = NULL,
                current_job_started_at = NULL
            WHERE status = 'running'
              AND last_heartbeat_at < now() - make_interval(secs => :threshold)
            """
        ),
        {"threshold": stale_threshold_seconds},
    )
    return result.rowcount  # type: ignore[return-value]


async def list_workers(
    session: AsyncSession,
    *,
    table: str,
    status: str | None = None,
) -> list[WorkerRow]:
    """List workers ordered by last_heartbeat_at DESC, optionally filtered by status."""
    schema, name = parse_qualified(table)
    t = make_workers_table(name=name, schema=schema)
    stmt = select(t).order_by(t.c.last_heartbeat_at.desc())
    if status is not None:
        stmt = stmt.where(t.c.status == status)
    result = await session.execute(stmt)
    return [WorkerRow.model_validate(dict(row)) for row in result.mappings()]
