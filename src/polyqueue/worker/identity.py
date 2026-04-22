"""Worker identity — created once at startup, passed to backends for claim tracking."""

from __future__ import annotations

import os
import platform
import uuid
from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict


class WorkerInfo(BaseModel):
    model_config = ConfigDict(frozen=True)

    worker_id: str
    hostname: str
    pid: int
    started_at: datetime


def make_worker_info(worker_name: str = "") -> WorkerInfo:
    """Build a WorkerInfo with a unique worker_id.

    Format: ``{label}:{pid}:{uuid}`` where label is worker_name or hostname.
    """
    hostname = platform.node()
    label = worker_name or hostname
    pid = os.getpid()
    worker_id = f"{label}:{pid}:{uuid.uuid4()}"
    return WorkerInfo(
        worker_id=worker_id,
        hostname=hostname,
        pid=pid,
        started_at=datetime.now(UTC),
    )
