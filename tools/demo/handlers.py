"""Worker-side handler implementations — import ONLY in worker processes.

This module imports ``job_defs`` for the typed ``JobCommands`` objects and
uses ``@register_handler(cmd)`` to bind each handler to its job type.
The job type string is owned by ``job_defs.py`` — it never appears here.

Do NOT import this module from the enqueue side (``enqueue.py``, web servers,
CLIs).  Worker dependencies — asyncio-heavy libs, ML models, DB clients, etc.
— belong here, not in ``job_defs.py``.
"""

import asyncio

from job_defs import (
    AddPayload,
    AddResult,
    GreetPayload,
    SleepPayload,
    add,
    greet,
    sleep,
    slow_with_progress,
)

from polyqueue.job_api import register_handler
from polyqueue.jobs.context import JobContext
from polyqueue.jobs.dispatcher import TerminalError

# ── Handler implementations ───────────────────────────────────────────────────


@register_handler(add)
async def _handle_add(ctx: JobContext, payload: AddPayload) -> AddResult:
    result = payload.a + payload.b
    ctx.logger.info(
        "add(%s, %s) = %s  [attempt %d/%d]",
        payload.a,
        payload.b,
        result,
        ctx.attempt,
        ctx.max_attempts,
    )
    return AddResult(sum=result)


@register_handler(greet)
async def _handle_greet(ctx: JobContext, payload: GreetPayload) -> None:
    ctx.logger.info(
        "Hello, %s!  [attempt %d/%d]", payload.name, ctx.attempt, ctx.max_attempts
    )
    if payload.fail:
        raise TerminalError("instructed to fail permanently")


@register_handler(sleep)
async def _handle_sleep(ctx: JobContext, payload: SleepPayload) -> None:
    tag = f" ({payload.label})" if payload.label else ""
    ctx.logger.info(
        "sleeping %gs%s  [attempt %d/%d]",
        payload.seconds,
        tag,
        ctx.attempt,
        ctx.max_attempts,
    )
    await asyncio.sleep(payload.seconds)
    ctx.logger.info("woke up%s  [attempt %d/%d]", tag, ctx.attempt, ctx.max_attempts)


@register_handler(slow_with_progress)
async def _handle_slow_with_progress(ctx: JobContext, payload: SleepPayload) -> None:
    tag = f" ({payload.label})" if payload.label else ""
    for i in range(int(payload.seconds)):
        ctx.logger.info("working%s step %d/%d", tag, i + 1, int(payload.seconds))
        await asyncio.sleep(1)
        await ctx.heartbeat_progress()
    ctx.logger.info("done%s", tag)
