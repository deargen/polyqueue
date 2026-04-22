"""Queue factory — builds the right JobQueue adapter from PolyqueueSettings.

Install the extra for your chosen backend:
    pip install polyqueue[redis]               # Redis (local / default)
    pip install polyqueue[sqs]                 # AWS SQS
    pip install polyqueue[azure-service-bus]   # Azure Service Bus
    # PGMQ backend uses SQL calls via SQLAlchemy/asyncpg (no extra needed)
    pip install polyqueue[all-queues]          # all external broker extras

The 'none' backend (InProcessQueue) requires no extras — jobs run as local
asyncio tasks in the same process. No DB or broker needed.

Usage:
    from polyqueue.config import PolyqueueSettings
    from polyqueue.queue.factory import get_queue

    # From environment (POLYQUEUE_* vars):
    queue, session_factory, engine = get_queue()

    # From explicit settings:
    settings = PolyqueueSettings(db_url="postgresql+asyncpg://...", redis_url="redis://...")
    queue, session_factory, engine = get_queue(settings)

    # No-worker / no-broker fallback (db_url not required):
    settings = PolyqueueSettings(db_url="", queue_backend="none")
    queue, session_factory, engine = get_queue(settings)
    # engine will be None; session_factory will be None
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

    from polyqueue.metrics import MetricsHook
    from polyqueue.queue.azure_service_bus_queue import AzureServiceBusJobQueue
    from polyqueue.queue.inprocess_queue import InProcessQueue
    from polyqueue.queue.pgmq_queue import PgmqJobQueue
    from polyqueue.queue.redis_queue import RedisJobQueue
    from polyqueue.queue.sqs_queue import SqsJobQueue

    AnyJobQueue = (
        RedisJobQueue
        | SqsJobQueue
        | AzureServiceBusJobQueue
        | PgmqJobQueue
        | InProcessQueue
    )
else:
    # At runtime the concrete classes are not imported unconditionally
    # (they have optional dependencies). Use Any so the name is valid.
    AnyJobQueue = Any

logger = logging.getLogger(__name__)


def get_queue(
    settings: Any | None = None,
    metrics: MetricsHook | None = None,
) -> tuple[AnyJobQueue, async_sessionmaker[AsyncSession] | None, AsyncEngine | None]:
    """Build and return (queue_adapter, session_factory, engine).

    Args:
        settings: A PolyqueueSettings instance. If None, reads from POLYQUEUE_*
            environment variables via PolyqueueSettings().
        metrics: Optional metrics hook. Passed to brokered backends (Redis, SQS,
            Azure Service Bus) which emit ``job_enqueued`` and other events.
            The ``none`` (InProcessQueue) backend is dev-only and does not
            participate in metrics — pass metrics to ``main()`` for worker-mode
            lifecycle events instead.

    For the 'none' backend, session_factory and engine are both None — no DB
    connection is created. Callers must handle None before using them.

    For all other backends, callers must call ``await engine.dispose()`` on shutdown.

    Raises:
        ImportError: if the required backend package is not installed.
        ValueError: if the queue_backend is unknown or required config is missing.
    """
    from polyqueue.config import PolyqueueSettings

    if settings is None:
        settings = PolyqueueSettings()

    backend: str = settings.queue_backend

    if backend == "none":
        from polyqueue.queue.inprocess_queue import InProcessQueue

        logger.info("queue: using InProcessQueue (no-worker / no-broker fallback)")
        return (
            InProcessQueue(
                session_factory=None,
                max_attempts=settings.max_attempts,
                retry_backoff_seconds=settings.retry_backoff_seconds,
            ),
            None,
            None,
        )

    engine = create_async_engine(settings.db_url, echo=False)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    if backend == "redis":
        try:
            from redis.asyncio import Redis
        except ImportError as exc:
            raise ImportError(
                "Redis backend requires the 'redis' extra: pip install polyqueue[redis]"
            ) from exc

        from polyqueue.queue.redis_queue import RedisJobQueue

        redis = Redis.from_url(settings.redis_url, decode_responses=False)
        logger.info("queue: using RedisJobQueue (%s)", settings.redis_url)
        return (
            RedisJobQueue(
                redis,
                session_factory,
                table_name=f"{settings.table_prefix}_jobs",
                table_schema=settings.table_schema,
                lease_duration_seconds=settings.lease_duration_seconds,
                heartbeat_interval_seconds=settings.heartbeat_interval_seconds,
                max_attempts=settings.max_attempts,
                retry_backoff_seconds=settings.retry_backoff_seconds,
                default_max_run_seconds=settings.default_max_run_seconds,
                default_timeout_strategy=settings.timeout_strategy,
                metrics=metrics,
            ),
            session_factory,
            engine,
        )

    if backend == "sqs":
        try:
            import boto3  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "SQS backend requires the 'sqs' extra: pip install polyqueue[sqs]"
            ) from exc

        from polyqueue.queue.sqs_queue import SqsJobQueue

        if not settings.sqs_queue_url:
            raise ValueError(
                "sqs_queue_url must be set when using the SQS backend "
                "(env: POLYQUEUE_SQS_QUEUE_URL)"
            )
        logger.info("queue: using SqsJobQueue (%s)", settings.sqs_queue_url)
        return (
            SqsJobQueue(
                settings.sqs_queue_url,
                session_factory,
                table_name=f"{settings.table_prefix}_jobs",
                table_schema=settings.table_schema,
                region=settings.sqs_region,
                visibility_timeout_seconds=settings.sqs_visibility_timeout_seconds,
                heartbeat_interval_seconds=settings.heartbeat_interval_seconds,
                max_attempts=settings.max_attempts,
                retry_backoff_seconds=settings.retry_backoff_seconds,
                default_max_run_seconds=settings.default_max_run_seconds,
                default_timeout_strategy=settings.timeout_strategy,
                metrics=metrics,
            ),
            session_factory,
            engine,
        )

    if backend == "azure_service_bus":
        try:
            import azure.servicebus  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "Azure Service Bus backend requires the 'azure-service-bus' extra: "
                "pip install polyqueue[azure-service-bus]"
            ) from exc

        from polyqueue.queue.azure_service_bus_queue import AzureServiceBusJobQueue

        if not settings.azure_sb_connection_string:
            raise ValueError(
                "azure_sb_connection_string must be set when using the Azure Service Bus backend "
                "(env: POLYQUEUE_AZURE_SB_CONNECTION_STRING)"
            )
        logger.info(
            "queue: using AzureServiceBusJobQueue (%s)", settings.azure_sb_queue_name
        )
        return (
            AzureServiceBusJobQueue(
                settings.azure_sb_connection_string,
                settings.azure_sb_queue_name,
                session_factory,
                table_name=f"{settings.table_prefix}_jobs",
                table_schema=settings.table_schema,
                max_attempts=settings.max_attempts,
                retry_backoff_seconds=settings.retry_backoff_seconds,
                max_lock_renewal_duration_seconds=settings.azure_sb_max_lock_renewal_duration_seconds,
                default_max_run_seconds=settings.default_max_run_seconds,
                default_timeout_strategy=settings.timeout_strategy,
                metrics=metrics,
            ),
            session_factory,
            engine,
        )

    if backend == "pgmq":
        from polyqueue.queue.pgmq_queue import PgmqJobQueue

        logger.info("queue: using PgmqJobQueue (%s)", settings.pgmq_queue_name)
        return (
            PgmqJobQueue(
                session_factory,
                table_name=f"{settings.table_prefix}_jobs",
                table_schema=settings.table_schema,
                queue_name=settings.pgmq_queue_name,
                visibility_timeout_seconds=settings.pgmq_visibility_timeout_seconds,
                heartbeat_interval_seconds=settings.heartbeat_interval_seconds,
                poll_seconds=settings.pgmq_poll_seconds,
                poll_interval_ms=settings.pgmq_poll_interval_ms,
                create_queue_if_missing=settings.pgmq_create_queue_if_missing,
                use_unlogged_queue=settings.pgmq_use_unlogged_queue,
                archive_on_success=settings.pgmq_archive_on_success,
                archive_on_terminal_fail=settings.pgmq_archive_on_terminal_fail,
                max_attempts=settings.max_attempts,
                retry_backoff_seconds=settings.retry_backoff_seconds,
                default_max_run_seconds=settings.default_max_run_seconds,
                default_timeout_strategy=settings.timeout_strategy,
                metrics=metrics,
            ),
            session_factory,
            engine,
        )

    raise ValueError(
        f"Unknown queue backend {backend!r}. "
        "Valid values: 'redis', 'sqs', 'azure_service_bus', 'pgmq', 'none'"
    )
