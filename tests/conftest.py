"""Shared fixtures for polyqueue tests.

Start all containers:

    podman compose -f tests/docker-compose.yml up -d
"""

from __future__ import annotations

import os

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from polyqueue.schema import attempts_table_ddl, jobs_table_ddl, workers_table_ddl

POSTGRES_URL = os.environ.get(
    "TEST_POSTGRES_URL",
    "postgresql+asyncpg://test:test@localhost:15432/polyqueue_test",
)
REDIS_URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:16379")
SQS_ENDPOINT = os.environ.get("TEST_SQS_ENDPOINT", "http://localhost:19324")
AZURE_SB_CONN = os.environ.get(
    "TEST_AZURE_SB_CONNECTION_STRING",
    "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=SAS_KEY_VALUE=;UseDevelopmentEmulator=true;",
)


@pytest_asyncio.fixture
async def session_factory() -> async_sessionmaker[AsyncSession]:
    engine = create_async_engine(POSTGRES_URL, echo=False)
    try:
        async with engine.begin() as conn:
            for stmt_batch in (
                jobs_table_ddl("jobs"),
                workers_table_ddl("polyqueue_workers"),
                attempts_table_ddl("jobs_attempts"),
            ):
                for stmt in (s.strip() for s in stmt_batch.split(";")):
                    if stmt:
                        await conn.execute(text(stmt))
            await conn.execute(text("TRUNCATE TABLE jobs_attempts"))
            await conn.execute(text("TRUNCATE TABLE jobs"))
            await conn.execute(text("TRUNCATE TABLE polyqueue_workers"))
    except Exception as exc:
        await engine.dispose()
        pytest.skip(f"Postgres not reachable at {POSTGRES_URL}: {exc}")
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE jobs_attempts"))
        await conn.execute(text("TRUNCATE TABLE jobs"))
        await conn.execute(text("TRUNCATE TABLE polyqueue_workers"))
    await engine.dispose()
