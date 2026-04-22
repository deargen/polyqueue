"""PolyqueueSettings — standalone configuration for the polyqueue worker and adapters.

Usage (direct construction):
    from polyqueue.config import PolyqueueSettings
    settings = PolyqueueSettings(db_url="postgresql+asyncpg://...", redis_url="redis://...")

Usage (from environment — reads POLYQUEUE_* env vars automatically):
    settings = PolyqueueSettings()

Environment variables (POLYQUEUE_ prefix):
    POLYQUEUE_DB_URL                        required (not used by the 'none' backend)
    POLYQUEUE_BACKEND                       redis | sqs | azure_service_bus | pgmq | none  (default: redis)
                                            'none' runs jobs inline as asyncio tasks — no broker
                                            or DB required; useful for development and testing.
    POLYQUEUE_TABLE_PREFIX                  default: polyqueue
                                            Used as prefix for all polyqueue tables:
                                            {prefix}_jobs, {prefix}_workers, {prefix}_jobs_attempts
    POLYQUEUE_TABLE_SCHEMA                  default: public
    POLYQUEUE_REDIS_URL                     default: redis://localhost:6379
    POLYQUEUE_SQS_QUEUE_URL
    POLYQUEUE_SQS_REGION                    default: us-east-1
    POLYQUEUE_SQS_VISIBILITY_TIMEOUT        seconds, default: 660
    POLYQUEUE_PGMQ_QUEUE_NAME               default: polyqueue
    POLYQUEUE_PGMQ_VISIBILITY_TIMEOUT       seconds, default: 660
    POLYQUEUE_PGMQ_POLL_SECONDS             seconds, default: 5
    POLYQUEUE_PGMQ_POLL_INTERVAL_MS         milliseconds, default: 100
    POLYQUEUE_PGMQ_CREATE_QUEUE_IF_MISSING  bool, default: true
    POLYQUEUE_PGMQ_USE_UNLOGGED_QUEUE       bool, default: false
    POLYQUEUE_PGMQ_ARCHIVE_ON_SUCCESS       bool, default: false
    POLYQUEUE_PGMQ_ARCHIVE_ON_TERMINAL_FAIL bool, default: false
    POLYQUEUE_AZURE_SB_CONNECTION_STRING
    POLYQUEUE_AZURE_SB_QUEUE_NAME           default: polyqueue
    POLYQUEUE_AZURE_SB_LOCK_RENEWAL         seconds, default: 3600
    POLYQUEUE_MAX_ATTEMPTS                  default: 3
    POLYQUEUE_RETRY_BACKOFF_SECONDS         comma-separated ints, default: 10,60,300
    POLYQUEUE_LEASE_DURATION                seconds (Redis), default: 600
    POLYQUEUE_HEARTBEAT_INTERVAL            seconds, default: 120
    POLYQUEUE_REAPER_INTERVAL               seconds, default: 60
    POLYQUEUE_RETRY_POLL_INTERVAL           seconds (Redis), default: 1
                                            How often the worker polls for due retries.
                                            Set lower than the shortest backoff value.
    POLYQUEUE_WORKER_NAME                   label for worker_id; empty = use hostname
    POLYQUEUE_DEFAULT_MAX_RUN_SECONDS       int or unset; unset = no time limit
    POLYQUEUE_TIMEOUT_STRATEGY              retry | fail | ignore  (default: retry)
    POLYQUEUE_PROGRESS_HEARTBEAT_INTERVAL   seconds, default: 30
    POLYQUEUE_STALE_PROGRESS_THRESHOLD      seconds, default: 120; a processing job is stale if
                                            progress_heartbeat_at has not been updated within this
                                            many seconds; used by stats snapshot and CLI
"""

from __future__ import annotations

import re
from typing import Literal

from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _env(field_name: str, *alt_env_names: str) -> AliasChoices:
    """Build an AliasChoices that maps both the field name and env var names.

    pydantic-settings uses ``validation_alias`` for env var lookup (no prefix
    applied).  This helper keeps the mapping explicit and DRY.
    """
    return AliasChoices(field_name, *alt_env_names)


class PolyqueueSettings(BaseSettings):
    """All settings needed to construct a queue adapter and run the worker.

    Reads from ``POLYQUEUE_*`` environment variables automatically on
    construction (via pydantic-settings).  Pass values directly to
    override.
    """

    model_config = SettingsConfigDict(populate_by_name=True, extra="ignore")

    # ── Database ──────────────────────────────────────────────────────────────
    db_url: str = Field(
        default="",
        validation_alias=_env("db_url", "POLYQUEUE_DB_URL"),
    )

    # ── Tables ────────────────────────────────────────────────────────────────
    # A single prefix + schema controls all three polyqueue tables.
    # Tables are always named {prefix}_jobs, {prefix}_workers, {prefix}_jobs_attempts.
    table_prefix: str = Field(
        default="polyqueue",
        validation_alias=_env("table_prefix", "POLYQUEUE_TABLE_PREFIX"),
    )
    table_schema: str = Field(
        default="polyqueue",
        validation_alias=_env("table_schema", "POLYQUEUE_TABLE_SCHEMA"),
    )

    # ── Queue backend ─────────────────────────────────────────────────────────
    queue_backend: Literal["redis", "sqs", "azure_service_bus", "pgmq", "none"] = Field(
        default="redis",
        validation_alias=_env("queue_backend", "POLYQUEUE_BACKEND"),
    )

    # ── Redis ─────────────────────────────────────────────────────────────────
    redis_url: str = Field(
        default="redis://localhost:6379",
        validation_alias=_env("redis_url", "POLYQUEUE_REDIS_URL"),
    )

    # ── SQS ───────────────────────────────────────────────────────────────────
    sqs_queue_url: str = Field(
        default="",
        validation_alias=_env("sqs_queue_url", "POLYQUEUE_SQS_QUEUE_URL"),
    )
    sqs_region: str = Field(
        default="us-east-1",
        validation_alias=_env("sqs_region", "POLYQUEUE_SQS_REGION"),
    )
    sqs_visibility_timeout_seconds: int = Field(
        default=660,
        validation_alias=_env(
            "sqs_visibility_timeout_seconds", "POLYQUEUE_SQS_VISIBILITY_TIMEOUT"
        ),
    )

    # ── PGMQ ──────────────────────────────────────────────────────────────────
    pgmq_queue_name: str = Field(
        default="polyqueue",
        validation_alias=_env("pgmq_queue_name", "POLYQUEUE_PGMQ_QUEUE_NAME"),
    )
    pgmq_visibility_timeout_seconds: int = Field(
        default=660,
        validation_alias=_env(
            "pgmq_visibility_timeout_seconds", "POLYQUEUE_PGMQ_VISIBILITY_TIMEOUT"
        ),
    )
    pgmq_poll_seconds: int = Field(
        default=5,
        validation_alias=_env("pgmq_poll_seconds", "POLYQUEUE_PGMQ_POLL_SECONDS"),
    )
    pgmq_poll_interval_ms: int = Field(
        default=100,
        validation_alias=_env(
            "pgmq_poll_interval_ms", "POLYQUEUE_PGMQ_POLL_INTERVAL_MS"
        ),
    )
    pgmq_create_queue_if_missing: bool = Field(
        default=True,
        validation_alias=_env(
            "pgmq_create_queue_if_missing", "POLYQUEUE_PGMQ_CREATE_QUEUE_IF_MISSING"
        ),
    )
    pgmq_use_unlogged_queue: bool = Field(
        default=False,
        validation_alias=_env(
            "pgmq_use_unlogged_queue", "POLYQUEUE_PGMQ_USE_UNLOGGED_QUEUE"
        ),
    )
    pgmq_archive_on_success: bool = Field(
        default=False,
        validation_alias=_env(
            "pgmq_archive_on_success", "POLYQUEUE_PGMQ_ARCHIVE_ON_SUCCESS"
        ),
    )
    pgmq_archive_on_terminal_fail: bool = Field(
        default=False,
        validation_alias=_env(
            "pgmq_archive_on_terminal_fail",
            "POLYQUEUE_PGMQ_ARCHIVE_ON_TERMINAL_FAIL",
        ),
    )

    # ── Azure Service Bus ─────────────────────────────────────────────────────
    azure_sb_connection_string: str = Field(
        default="",
        validation_alias=_env(
            "azure_sb_connection_string", "POLYQUEUE_AZURE_SB_CONNECTION_STRING"
        ),
    )
    azure_sb_queue_name: str = Field(
        default="polyqueue",
        validation_alias=_env("azure_sb_queue_name", "POLYQUEUE_AZURE_SB_QUEUE_NAME"),
    )
    azure_sb_max_lock_renewal_duration_seconds: int = Field(
        default=3600,
        validation_alias=_env(
            "azure_sb_max_lock_renewal_duration_seconds",
            "POLYQUEUE_AZURE_SB_LOCK_RENEWAL",
        ),
    )

    # ── Worker / shared ───────────────────────────────────────────────────────
    max_attempts: int = Field(
        default=3,
        validation_alias=_env("max_attempts", "POLYQUEUE_MAX_ATTEMPTS"),
    )
    retry_backoff_seconds: list[int] = Field(
        default_factory=lambda: [10, 60, 300],
        validation_alias=_env(
            "retry_backoff_seconds", "POLYQUEUE_RETRY_BACKOFF_SECONDS"
        ),
    )
    lease_duration_seconds: int = Field(
        default=600,
        validation_alias=_env("lease_duration_seconds", "POLYQUEUE_LEASE_DURATION"),
    )
    heartbeat_interval_seconds: int = Field(
        default=120,
        validation_alias=_env(
            "heartbeat_interval_seconds", "POLYQUEUE_HEARTBEAT_INTERVAL"
        ),
    )
    reaper_interval_seconds: int = Field(
        default=60,
        validation_alias=_env("reaper_interval_seconds", "POLYQUEUE_REAPER_INTERVAL"),
    )
    retry_poll_interval_seconds: int = Field(
        default=1,
        validation_alias=_env(
            "retry_poll_interval_seconds", "POLYQUEUE_RETRY_POLL_INTERVAL"
        ),
    )

    # ── Worker identity ───────────────────────────────────────────────────────
    worker_name: str = Field(
        default="",
        validation_alias=_env("worker_name", "POLYQUEUE_WORKER_NAME"),
    )

    # ── Time limits ───────────────────────────────────────────────────────────
    default_max_run_seconds: int | None = Field(
        default=None,
        validation_alias=_env(
            "default_max_run_seconds", "POLYQUEUE_DEFAULT_MAX_RUN_SECONDS"
        ),
    )
    timeout_strategy: Literal["retry", "fail", "ignore"] = Field(
        default="retry",
        validation_alias=_env("timeout_strategy", "POLYQUEUE_TIMEOUT_STRATEGY"),
    )
    progress_heartbeat_interval_seconds: int = Field(
        default=30,
        validation_alias=_env(
            "progress_heartbeat_interval_seconds",
            "POLYQUEUE_PROGRESS_HEARTBEAT_INTERVAL",
        ),
    )

    # ── Worker registry ───────────────────────────────────────────────────
    worker_heartbeat_interval_seconds: int = Field(
        default=15,
        validation_alias=_env(
            "worker_heartbeat_interval_seconds", "POLYQUEUE_WORKER_HEARTBEAT_INTERVAL"
        ),
    )
    worker_stale_threshold_seconds: int = Field(
        default=60,
        validation_alias=_env(
            "worker_stale_threshold_seconds", "POLYQUEUE_WORKER_STALE_THRESHOLD"
        ),
    )
    stale_progress_threshold_seconds: int = Field(
        default=120,
        validation_alias=_env(
            "stale_progress_threshold_seconds", "POLYQUEUE_STALE_PROGRESS_THRESHOLD"
        ),
    )

    # ──────────────────────────────────────────────────────────────────────────

    @field_validator("retry_backoff_seconds", mode="before")
    @classmethod
    def _parse_retry_backoff(cls, v: object) -> list[int] | object:
        """Accept comma-separated ints from env vars (e.g. ``10,60,300``)."""
        if isinstance(v, str):
            return [int(x.strip()) for x in v.split(",") if x.strip()]
        return v

    @model_validator(mode="after")
    def _validate(self) -> PolyqueueSettings:
        if not self.db_url and self.queue_backend != "none":
            raise ValueError("POLYQUEUE_DB_URL is required when backend is not 'none'")
        if self.queue_backend == "redis":
            if self.heartbeat_interval_seconds >= self.lease_duration_seconds:
                raise ValueError(
                    f"heartbeat_interval_seconds ({self.heartbeat_interval_seconds}) "
                    f"must be less than lease_duration_seconds ({self.lease_duration_seconds})"
                )
        if self.queue_backend == "sqs":
            if self.heartbeat_interval_seconds >= self.sqs_visibility_timeout_seconds:
                raise ValueError(
                    f"heartbeat_interval_seconds ({self.heartbeat_interval_seconds}) "
                    f"must be less than sqs_visibility_timeout_seconds ({self.sqs_visibility_timeout_seconds})"
                )
        if self.queue_backend == "pgmq":
            if self.heartbeat_interval_seconds >= self.pgmq_visibility_timeout_seconds:
                raise ValueError(
                    f"heartbeat_interval_seconds ({self.heartbeat_interval_seconds}) "
                    f"must be less than pgmq_visibility_timeout_seconds ({self.pgmq_visibility_timeout_seconds})"
                )
            if not re.fullmatch(r"[a-z0-9_-]+", self.pgmq_queue_name):
                raise ValueError(
                    "pgmq_queue_name must match [a-z0-9_-]+ "
                    "(lowercase letters, digits, hyphen, underscore)"
                )
        return self

    def qualified_table(self) -> str:
        """Fully-qualified jobs table, e.g. ``polyqueue.polyqueue_jobs``."""
        return f"{self.table_schema}.{self.table_prefix}_jobs"

    def qualified_workers_table(self) -> str:
        """Fully-qualified workers table, e.g. ``polyqueue.polyqueue_workers``."""
        return f"{self.table_schema}.{self.table_prefix}_workers"

    def qualified_attempts_table(self) -> str:
        """Fully-qualified attempts (audit log) table, e.g. ``polyqueue.polyqueue_jobs_attempts``."""
        return f"{self.table_schema}.{self.table_prefix}_jobs_attempts"
