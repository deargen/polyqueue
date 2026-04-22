"""SQLAlchemy Core Table definitions — single source of truth for column names.

Read-only query helpers (stats, results, registry list) import from here to
reference columns by object rather than string. Mutation helpers in db.py
intentionally keep raw text() SQL for explicit state-transition semantics.
"""

from __future__ import annotations

from sqlalchemy import BigInteger, Column, DateTime, Integer, MetaData, Table, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID


def make_jobs_table(name: str = "polyqueue_jobs", schema: str = "public") -> Table:
    """Construct a Core Table for the jobs table."""
    return Table(
        name,
        MetaData(schema=schema),
        Column("id", Text, primary_key=True),
        Column("job_type", Text, nullable=False),
        Column("status", Text, nullable=False),
        Column("payload", JSONB, nullable=False),
        Column("result", JSONB),
        Column("error_code", Text),
        Column("last_error", Text),
        Column("attempt_count", Integer, nullable=False),
        Column("max_attempts", Integer, nullable=False),
        Column("created_at", DateTime(timezone=True), nullable=False),
        Column("updated_at", DateTime(timezone=True), nullable=False),
        Column("started_at", DateTime(timezone=True)),
        Column("finished_at", DateTime(timezone=True)),
        Column("claimed_by_worker_id", Text),
        Column("claimed_by_hostname", Text),
        Column("claimed_by_pid", Integer),
        Column("claimed_at", DateTime(timezone=True)),
        Column("lease_expires_at", DateTime(timezone=True)),
        Column("lease_token", UUID(as_uuid=True)),
        Column("progress_heartbeat_at", DateTime(timezone=True)),
        Column("last_heartbeat_at", DateTime(timezone=True)),
        Column("max_run_seconds", Integer),
        Column("timeout_strategy", Text),
        Column("timeout_at", DateTime(timezone=True)),
    )


def make_attempts_table(
    name: str = "polyqueue_jobs_attempts", schema: str = "public"
) -> Table:
    """Construct a Core Table for the attempts (audit log) table."""
    return Table(
        name,
        MetaData(schema=schema),
        Column("event_id", BigInteger, primary_key=True),
        Column("job_id", Text, nullable=False),
        Column("attempt_number", Integer, nullable=False),
        Column("event_type", Text, nullable=False),
        Column("event_at", DateTime(timezone=True), nullable=False),
        Column("worker_id", Text),
        Column("worker_hostname", Text),
        Column("worker_pid", Integer),
        Column("lease_token", UUID(as_uuid=True)),
        Column("finalized_by", Text),
        Column("duration_ms", BigInteger),
        Column("error_code", Text),
        Column("error_message", Text),
        Column("stack_trace", Text),
        Column("job_type", Text),
        Column("queue_name", Text),
        Column("metadata", JSONB),
    )


def make_workers_table(
    name: str = "polyqueue_workers", schema: str = "public"
) -> Table:
    """Construct a Core Table for the workers table."""
    return Table(
        name,
        MetaData(schema=schema),
        Column("worker_id", Text, primary_key=True),
        Column("hostname", Text, nullable=False),
        Column("pid", Integer, nullable=False),
        Column("started_at", DateTime(timezone=True), nullable=False),
        Column("last_heartbeat_at", DateTime(timezone=True), nullable=False),
        Column("backend", Text, nullable=False),
        Column("worker_name", Text, nullable=False),
        Column("status", Text, nullable=False),
        Column("current_job_id", Text),
        Column("current_job_started_at", DateTime(timezone=True)),
        Column("version", Text),
        Column("metadata", JSONB),
    )


def parse_qualified(qualified: str) -> tuple[str, str]:
    """Split ``'schema.name'`` into ``(schema, name)``. Defaults schema to ``'public'``."""
    if "." in qualified:
        schema, name = qualified.split(".", 1)
        return schema, name
    return "public", qualified
