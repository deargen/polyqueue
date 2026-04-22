from __future__ import annotations

from datetime import datetime  # noqa: TC003 — Pydantic needs this at runtime
from typing import TYPE_CHECKING, Any, Protocol, overload
from uuid import UUID  # noqa: TC003 — Pydantic needs this at runtime

from pydantic import BaseModel, ConfigDict

from polyqueue.enqueue_options import EnqueueOptions, EnqueueResult  # noqa: TC001

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from polyqueue.worker.identity import WorkerInfo


class ClaimedJob(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    receipt: str
    job_id: str
    job_type: str
    payload: dict[str, Any]
    attempt: int
    max_attempts: int
    # worker tracking
    worker_id: str = ""
    worker_hostname: str = ""
    worker_pid: int = 0
    # time limits
    claimed_at: datetime | None = None
    queued_at: datetime | None = None
    max_run_seconds: int | None = None
    timeout_strategy: str | None = None
    timeout_at: datetime | None = None
    # race-safety token set by claim_job(); required by all terminal DB updates
    lease_token: UUID | None = None


class JobQueue(Protocol):
    @overload
    async def enqueue[P: BaseModel](
        self,
        target: Callable[[Any, P], Awaitable[Any]],
        payload: P,
        *,
        options: EnqueueOptions | None = None,
    ) -> EnqueueResult: ...

    @overload
    async def enqueue(
        self,
        target: str,
        payload: dict[str, Any] | BaseModel,
        *,
        options: EnqueueOptions | None = None,
    ) -> EnqueueResult: ...

    async def enqueue(
        self,
        target: Any,
        payload: Any,
        *,
        options: EnqueueOptions | None = None,
    ) -> EnqueueResult: ...

    def set_worker_info(self, worker_info: WorkerInfo) -> None: ...
    async def claim(self) -> ClaimedJob | None: ...
    async def ack(self, receipt: str) -> None: ...
    async def fail(
        self, receipt: str, error: str, *, retryable: bool = True
    ) -> None: ...
    async def reap_abandoned_leases(self) -> None: ...
    async def move_due_retries(self) -> None: ...
    async def acquire_maintenance_lock(self, ttl_seconds: int = 90) -> bool: ...
    async def release_maintenance_lock(self) -> None: ...
    async def close(self) -> None: ...
