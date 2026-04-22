# /// script
# requires-python = ">=3.12"
# dependencies = ["polyqueue[all-queues]", "rich"]
#
# [tool.uv.sources]
# polyqueue = { path = "../.." }
# ///
r"""Multi-worker demo — enqueues a mix of slow and fast jobs.

Run two workers in separate terminals, then enqueue these jobs.
The slow jobs (5s each) block one worker while the other picks up fast jobs.

Usage
-----
    cd python/packages/polyqueue

    # terminal 1 — worker A
    POLYQUEUE_BACKEND=redis \
    POLYQUEUE_DB_URL=postgresql+asyncpg://pq:pq@localhost/pq \
    uv run tools/demo/worker.py

    # terminal 2 — worker B
    POLYQUEUE_BACKEND=redis \
    POLYQUEUE_DB_URL=postgresql+asyncpg://pq:pq@localhost/pq \
    uv run tools/demo/worker.py

    # terminal 3 — enqueue
    POLYQUEUE_BACKEND=redis \
    POLYQUEUE_DB_URL=postgresql+asyncpg://pq:pq@localhost/pq \
    uv run tools/demo/enqueue_multiworker.py
"""

import asyncio
import logging
import sys
from pathlib import Path

from rich.logging import RichHandler

sys.path.insert(0, str(Path(__file__).parent))

from job_defs import AddPayload, SleepPayload, add, sleep

from polyqueue.enqueue_options import EnqueueOptions

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
    queue, _, engine = get_queue(settings)

    if engine is not None:
        from polyqueue.schema import ensure_schema

        await ensure_schema(engine, settings)
        logger.info("Jobs table ready: %s", settings.qualified_table())

    await sleep.enqueue(queue, SleepPayload(seconds=5, label="slow-1"))
    logger.info("Enqueued  sleep  label=slow-1 (5s)")

    await sleep.enqueue(queue, SleepPayload(seconds=5, label="slow-2"))
    logger.info("Enqueued  sleep  label=slow-2 (5s)")

    await sleep.enqueue(
        queue,
        SleepPayload(seconds=30, label="will-timeout"),
        options=EnqueueOptions(max_run_seconds=3),
    )
    logger.info("Enqueued  sleep  label=will-timeout (30s, max_run_seconds=3)")

    await add.enqueue(queue, AddPayload(a=1, b=2))
    await add.enqueue(queue, AddPayload(a=3, b=4))
    await add.enqueue(queue, AddPayload(a=5, b=6))
    await add.enqueue(queue, AddPayload(a=7, b=8))
    logger.info("Enqueued  add x4")

    await queue.close()
    if engine is not None:
        await engine.dispose()
    logger.info("Done — watch the two workers claim jobs.")


if __name__ == "__main__":
    asyncio.run(_run())
