# /// script
# requires-python = ">=3.12"
# dependencies = ["polyqueue[all-queues]", "rich"]
#
# [tool.uv.sources]
# polyqueue = { path = "../.." }
# ///
r"""Minimal polyqueue worker demo — registers handlers and runs the worker loop.

Usage
-----
Redis backend (terminal 1):

    cd python/packages/polyqueue
    POLYQUEUE_BACKEND=redis \\
    POLYQUEUE_DB_URL=postgresql+asyncpg://pq:pq@localhost/pq \\
    uv run tools/demo/worker.py

Then in terminal 2, run tools/demo/enqueue.py to push jobs.
Press Ctrl-C to stop the worker.
"""

import asyncio
import logging
import sys
from pathlib import Path

from rich.logging import RichHandler

sys.path.insert(0, str(Path(__file__).parent))

import handlers  # noqa: F401 — registers all handlers as a side effect

from polyqueue.config import PolyqueueSettings
from polyqueue.schema import ensure_schema
from polyqueue.worker.main import main

logging.basicConfig(
    level=logging.INFO,
    format="%(name)s  %(message)s",
    handlers=[RichHandler(rich_tracebacks=True, show_path=False)],
)


async def _run() -> None:
    settings = PolyqueueSettings()
    if settings.db_url:
        from sqlalchemy.ext.asyncio import create_async_engine

        engine = create_async_engine(settings.db_url, echo=False)
        await ensure_schema(engine, settings)
        await engine.dispose()
        logging.getLogger(__name__).info(
            "Jobs/workers tables ready: %s / %s",
            settings.qualified_table(),
            settings.qualified_workers_table(),
        )
    await main(settings)


if __name__ == "__main__":
    asyncio.run(_run())
