"""polyqueue — admin CLI.

Usage:
    polyqueue inspect stats
    polyqueue inspect workers [--status STATUS]
    polyqueue inspect jobs [--status STATUS] [--stale-progress] [--timed-out] [--limit N]
    polyqueue admin requeue JOB_ID
    polyqueue admin fail JOB_ID [--error MSG]
"""

from __future__ import annotations

import asyncio

import cyclopts
from rich.console import Console
from rich.table import Table

console = Console()

app = cyclopts.App(name="polyqueue", help="Polyqueue admin CLI")
inspect_app = cyclopts.App(name="inspect", help="Inspect queue and worker state")
admin_app = cyclopts.App(name="admin", help="Admin operations on jobs")
app.command(inspect_app)
app.command(admin_app)


def _get_engine_and_settings():
    from sqlalchemy.ext.asyncio import create_async_engine

    from polyqueue.config import PolyqueueSettings

    settings = PolyqueueSettings()
    if not settings.db_url:
        console.print("[red]POLYQUEUE_DB_URL is required[/red]")
        raise SystemExit(1)
    engine = create_async_engine(settings.db_url, echo=False)
    return engine, settings


@inspect_app.command
def stats() -> None:
    """Show a QueueSnapshot table."""

    async def _run() -> None:
        from sqlalchemy.ext.asyncio import AsyncSession

        from polyqueue.stats import get_queue_snapshot

        engine, settings = _get_engine_and_settings()
        try:
            async with AsyncSession(engine) as session:
                snapshot = await get_queue_snapshot(
                    session,
                    jobs_table=settings.qualified_table(),
                    workers_table=settings.qualified_workers_table(),
                    stale_progress_seconds=settings.stale_progress_threshold_seconds,
                    worker_stale_seconds=settings.worker_stale_threshold_seconds,
                )
        finally:
            await engine.dispose()

        table = Table(
            title="Queue Snapshot", show_header=True, header_style="bold cyan"
        )
        table.add_column("Metric", style="bold")
        table.add_column("Value", justify="right")

        table.add_row("Queued jobs", str(snapshot.queued_jobs))
        table.add_row("Processing jobs", str(snapshot.processing_jobs))
        table.add_row("Succeeded jobs", str(snapshot.succeeded_jobs))
        table.add_row("Failed jobs", str(snapshot.failed_jobs))
        table.add_row(
            "Oldest queued age (s)",
            f"{snapshot.oldest_queued_age_seconds:.1f}"
            if snapshot.oldest_queued_age_seconds is not None
            else "—",
        )
        table.add_row(
            "Timed-out processing jobs", str(snapshot.timed_out_processing_jobs)
        )
        table.add_row("Stale-progress jobs", str(snapshot.stale_progress_jobs))
        table.add_row("Healthy workers", str(snapshot.healthy_workers))
        table.add_row("Running workers", str(snapshot.running_workers))

        console.print(table)

    asyncio.run(_run())


@inspect_app.command
def workers(*, status: str | None = None) -> None:
    """List workers from the registry.

    Args:
        status: Optional status filter (e.g. running, stopped, dead).
    """

    async def _run() -> None:
        from sqlalchemy.ext.asyncio import AsyncSession

        from polyqueue.worker.registry import list_workers

        engine, settings = _get_engine_and_settings()
        try:
            async with AsyncSession(engine) as session:
                rows = await list_workers(
                    session,
                    table=settings.qualified_workers_table(),
                    status=status,
                )
        finally:
            await engine.dispose()

        title = f"Workers (status={status})" if status else "Workers (all)"
        table = Table(title=title, show_header=True, header_style="bold cyan")
        table.add_column("worker_id")
        table.add_column("hostname")
        table.add_column("pid", justify="right")
        table.add_column("status")
        table.add_column("backend")
        table.add_column("worker_name")
        table.add_column("current_job_id")
        table.add_column("last_heartbeat_at")

        for row in rows:
            table.add_row(
                str(row.worker_id),
                str(row.hostname or ""),
                str(row.pid or ""),
                str(row.status or ""),
                str(row.backend or ""),
                str(row.worker_name or ""),
                str(row.current_job_id or ""),
                str(row.last_heartbeat_at or ""),
            )

        console.print(table)
        console.print(f"[dim]{len(rows)} row(s)[/dim]")

    asyncio.run(_run())


@inspect_app.command
def jobs(
    *,
    status: str | None = None,
    stale_progress: bool = False,
    timed_out: bool = False,
    limit: int = 50,
) -> None:
    """List jobs with optional filters.

    Args:
        status: Filter by job status (queued, processing, succeeded, failed).
        stale_progress: Show processing jobs with a stale progress heartbeat
            (threshold from POLYQUEUE_STALE_PROGRESS_THRESHOLD, default 120s).
        timed_out: Show processing jobs whose timeout_at has passed.
        limit: Maximum number of rows to return (default 50).
    """

    async def _run() -> None:
        from datetime import timedelta

        from sqlalchemy import func, select
        from sqlalchemy.ext.asyncio import AsyncSession

        from polyqueue.tables import make_jobs_table, parse_qualified

        engine, settings = _get_engine_and_settings()
        schema, name = parse_qualified(settings.qualified_table())
        t = make_jobs_table(name=name, schema=schema)

        stmt = (
            select(
                t.c.id,
                t.c.job_type,
                t.c.status,
                t.c.attempt_count,
                t.c.max_attempts,
                t.c.error_code,
                t.c.last_error,
                t.c.created_at,
                t.c.updated_at,
                t.c.claimed_by_worker_id,
                t.c.timeout_at,
                t.c.progress_heartbeat_at,
            )
            .order_by(t.c.created_at.desc())
            .limit(limit)
        )

        if status is not None:
            stmt = stmt.where(t.c.status == status)

        if stale_progress:
            stmt = stmt.where(
                t.c.status == "processing",
                t.c.progress_heartbeat_at
                < func.now()
                - timedelta(seconds=settings.stale_progress_threshold_seconds),
            )

        if timed_out:
            stmt = stmt.where(
                t.c.status == "processing",
                t.c.timeout_at.is_not(None),
                t.c.timeout_at < func.now(),
            )

        try:
            async with AsyncSession(engine) as session:
                result = await session.execute(stmt)
                rows = list(result.mappings().all())
        finally:
            await engine.dispose()

        filters = []
        if status:
            filters.append(f"status={status}")
        if stale_progress:
            filters.append("stale-progress")
        if timed_out:
            filters.append("timed-out")
        title = "Jobs" + (f" ({', '.join(filters)})" if filters else " (all)")

        table = Table(title=title, show_header=True, header_style="bold cyan")
        table.add_column("id")
        table.add_column("job_type")
        table.add_column("status")
        table.add_column("attempts", justify="right")
        table.add_column("error_code")
        table.add_column("claimed_by_worker_id")
        table.add_column("timeout_at")
        table.add_column("progress_heartbeat_at")
        table.add_column("created_at")

        for row in rows:
            attempts = f"{row['attempt_count']}/{row['max_attempts']}"
            table.add_row(
                str(row["id"]),
                str(row["job_type"] or ""),
                str(row["status"] or ""),
                attempts,
                str(row["error_code"] or ""),
                str(row["claimed_by_worker_id"] or ""),
                str(row["timeout_at"] or ""),
                str(row["progress_heartbeat_at"] or ""),
                str(row["created_at"] or ""),
            )

        console.print(table)
        console.print(f"[dim]{len(rows)} row(s)[/dim]")

    asyncio.run(_run())


@admin_app.command
def requeue(job_id: str) -> None:
    """Reset a failed or stuck job back to queued (DB-only).

    Only transitions from status IN ('failed', 'processing').
    This resets DB state but does NOT republish to the broker.
    For Redis, orphan reconciliation will eventually re-push it.
    For SQS/Azure, you may need to re-enqueue via the application.

    Args:
        job_id: The job ID to requeue.
    """

    async def _run() -> None:
        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import AsyncSession

        from polyqueue.queue.db import reset_for_retry

        engine, settings = _get_engine_and_settings()
        jobs_table = settings.qualified_table()
        attempts_table = settings.qualified_attempts_table()

        did_reset = False
        try:
            async with AsyncSession(engine) as session:
                # Path 1: processing → queued via reset_for_retry (race-safe,
                # emits an 'abandoned' event with finalized_by='admin').
                row = await session.execute(
                    text(
                        f"SELECT lease_token FROM {jobs_table} "
                        "WHERE id = :id AND status = 'processing'"
                    ),
                    {"id": job_id},
                )
                rec = row.mappings().one_or_none()
                if rec is not None and rec["lease_token"] is not None:
                    attempt = await reset_for_retry(
                        session,
                        table=jobs_table,
                        attempts_table=attempts_table,
                        job_id=job_id,
                        lease_token=rec["lease_token"],
                        error_code="admin_requeue",
                        last_error="Requeued via polyqueue admin CLI",
                        finalized_by="admin",
                    )
                    did_reset = attempt is not None

                if not did_reset:
                    # Path 2: failed → queued. No attempt was in flight, so
                    # no attempt event is needed. Just flip the status.
                    result = await session.execute(
                        text(
                            f"""
                            UPDATE {jobs_table}
                            SET status     = 'queued',
                                error_code = NULL,
                                last_error = NULL,
                                updated_at = now()
                            WHERE id = :id AND status = 'failed'
                            RETURNING id
                            """
                        ),
                        {"id": job_id},
                    )
                    did_reset = result.mappings().one_or_none() is not None

                await session.commit()
        finally:
            await engine.dispose()

        if did_reset:
            console.print(
                f"[green]Job {job_id} reset to queued state (DB only).[/green]"
            )
        else:
            console.print(
                f"[yellow]No update: job {job_id} not found or not in failed/processing state.[/yellow]"
            )

    asyncio.run(_run())


@admin_app.command
def fail(job_id: str, *, error: str = "admin_forced_fail") -> None:
    """Mark a processing job as permanently failed.

    Uses mark_failed_terminal — only transitions from processing → failed.

    Args:
        job_id: The job ID to fail.
        error: Error message to record (default: admin_forced_fail).
    """

    async def _run() -> None:
        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import AsyncSession

        from polyqueue.queue.db import mark_failed_terminal

        engine, settings = _get_engine_and_settings()
        jobs_table = settings.qualified_table()
        attempts_table = settings.qualified_attempts_table()

        try:
            async with AsyncSession(engine) as session:
                row = await session.execute(
                    text(
                        f"SELECT lease_token FROM {jobs_table} "
                        "WHERE id = :id AND status = 'processing'"
                    ),
                    {"id": job_id},
                )
                rec = row.mappings().one_or_none()
                if rec is None or rec["lease_token"] is None:
                    ok = False
                else:
                    ok = await mark_failed_terminal(
                        session,
                        table=jobs_table,
                        attempts_table=attempts_table,
                        job_id=job_id,
                        lease_token=rec["lease_token"],
                        error_code="admin_forced_fail",
                        last_error=error,
                        finalized_by="admin",
                    )
                await session.commit()
        finally:
            await engine.dispose()

        if ok:
            console.print(f"[green]Job {job_id} marked as failed.[/green]")
        else:
            console.print(
                f"[yellow]No update: job {job_id} not found or not in processing state.[/yellow]"
            )

    asyncio.run(_run())
