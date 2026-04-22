from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict


class EnqueueOptions(BaseModel):
    model_config = ConfigDict(frozen=True)

    job_id: str | None = None
    max_run_seconds: int | None = None
    timeout_strategy: Literal["retry", "fail", "ignore"] | None = None


class EnqueueResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    job_id: str
