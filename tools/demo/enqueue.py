# /// script
# requires-python = ">=3.12"
# dependencies = ["polyqueue[all-queues]", "rich"]
#
# [tool.uv.sources]
# polyqueue = { path = "../.." }
# ///
r"""Minimal polyqueue enqueue demo — pushes a handful of sample jobs into the queue.

Usage
-----
Redis backend (after worker.py is running):

    cd python/packages/polyqueue
    POLYQUEUE_BACKEND=redis \\
    POLYQUEUE_DB_URL=postgresql+asyncpg://pq:pq@localhost/pq \\
    uv run tools/demo/enqueue.py

In-process backend (no Redis/Postgres needed — runs jobs inline):

    cd python/packages/polyqueue
    POLYQUEUE_BACKEND=none \\
    uv run tools/demo/enqueue.py
"""

import asyncio
import logging
import sys
from pathlib import Path

from rich.logging import RichHandler

sys.path.insert(0, str(Path(__file__).parent))

from job_defs import AddPayload, GreetPayload, add, greet

logging.basicConfig(
    level=logging.INFO,
    format="%(name)s  %(message)s",
    handlers=[RichHandler(rich_tracebacks=True, show_path=False)],
)
logger = logging.getLogger(__name__)


async def _run() -> None:
    from polyqueue.config import PolyqueueSettings
    from polyqueue.queue.factory import get_queue

    settings = PolyqueueSettings()
    if settings.queue_backend == "none":
        # In-process queue dispatches in this same process, so handlers must be
        # imported here to register job types before enqueueing.
        import handlers  # noqa: F401

        logger.info(
            "In-process backend: imported handlers to register job types locally"
        )

    queue, _, engine = get_queue(settings)

    if engine is not None:
        from polyqueue.schema import ensure_schema

        await ensure_schema(engine, settings)
        logger.info("Jobs table ready: %s", settings.qualified_table())

    # Enqueue a tracked job so we can fetch its typed result afterwards
    tracked = await add.enqueue(queue, AddPayload(a=10, b=20))
    logger.info("Enqueued tracked add job  job_id=%s", tracked.job_id)

    # Remaining sample jobs
    await add.enqueue(queue, AddPayload(a=1, b=2))
    logger.info("Enqueued  add    a=1 b=2")

    await greet.enqueue(queue, GreetPayload(name="Alice"))
    logger.info("Enqueued  greet  name=Alice")

    await greet.enqueue(queue, GreetPayload(name="Bob"))
    logger.info("Enqueued  greet  name=Bob")

    await greet.enqueue(queue, GreetPayload(name="Charlie", fail=True))
    logger.info("Enqueued  greet  name=Charlie fail=True  (demonstrates TerminalError)")

    if settings.queue_backend == "none":
        logger.info("Waiting for in-process jobs to finish…")
        await asyncio.sleep(1)

    # Fetch the typed result of the tracked job (Redis/Postgres backends only)
    if engine is not None:
        from sqlalchemy.ext.asyncio import async_sessionmaker

        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        async with session_factory() as session:
            job_status = await add.get_status(
                session, table=settings.qualified_table(), job_id=tracked.job_id
            )
            logger.info(
                "Job %s  status=%s  attempt=%d/%d  worker=%s",
                tracked.job_id,
                job_status.status,
                job_status.attempt,
                job_status.max_attempts,
                job_status.worker_hostname or "—",
            )

            if job_status.status == "succeeded":
                result = await add.get_result(
                    session, table=settings.qualified_table(), job_id=tracked.job_id
                )
                # result is typed as AddResult | None
                logger.info("Result: %s", result)
            elif job_status.status == "failed":
                logger.error("Job failed: %s", job_status.last_error)
            else:
                logger.info(
                    "Job not yet succeeded — run worker.py first, then re-run this script"
                )

    await queue.close()
    if engine is not None:
        await engine.dispose()
    logger.info("Done.")


if __name__ == "__main__":
    asyncio.run(_run())
