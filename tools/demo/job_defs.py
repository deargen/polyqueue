"""Shared job contracts — safe to import from anywhere.

This module defines the payload and result models for every job type, plus
typed ``JobCommands`` objects via ``job_spec``.  It is imported by BOTH:

  - The **enqueue side** (``enqueue.py``, web servers, CLIs) — to push jobs
    and query their status/results with full type safety.
  - The **worker side** (``handlers.py``) — to annotate handler signatures
    with the correct payload and result types.

Because ``job_spec`` does NOT import or register any handler code, this
module stays lightweight: no asyncio, no heavy ML/DB/IO dependencies.
Any import that belongs to handler implementations lives in ``handlers.py``
instead.

The job-type strings defined here (``"add"``, ``"greet"``, …) are the
canonical names. ``handlers.py`` registers handlers against these commands via
``@register_handler(...)``.
"""

from pydantic import BaseModel

from polyqueue.job_api import job_spec

# ── Payload models ────────────────────────────────────────────────────────────


class AddPayload(BaseModel):
    a: int
    b: int


class GreetPayload(BaseModel):
    name: str = "world"
    fail: bool = False


class SleepPayload(BaseModel):
    seconds: float
    label: str = ""


# ── Result models ─────────────────────────────────────────────────────────────


class AddResult(BaseModel):
    sum: int


# ── Typed job commands (no handler, no heavy deps) ────────────────────────────

add = job_spec("add", AddPayload, AddResult)  # JobCommands[AddPayload, AddResult]
greet = job_spec("greet", GreetPayload)  # JobCommands[GreetPayload, None]
sleep = job_spec("sleep", SleepPayload)  # JobCommands[SleepPayload, None]
slow_with_progress = job_spec(
    "slow_with_progress", SleepPayload
)  # JobCommands[SleepPayload, None]
