"""Job dispatcher — registry-based routing from job_type to handler functions.

Register handlers via ``make_job`` (preferred) before starting the worker:

    from polyqueue.job_api import make_job
    from polyqueue.jobs.context import JobContext
    from pydantic import BaseModel

    class AddPayload(BaseModel):
        a: int
        b: int

    add = make_job("add", handle_add)  # registers and returns typed JobCommands

Handler contract:
- Handlers may return None or a typed value (Pydantic BaseModel, dict, primitive).
  Return values are captured by dispatch() and persisted by the worker.
- Handlers own all Postgres persistence beyond result storage (reading payload, etc).
- Raise TerminalError to mark the job permanently failed (no retry).
- Any other exception triggers a retry up to ctx.max_attempts, then marks failed.
- Unknown job types raise TerminalError automatically.
- Pydantic ValidationError on payload coercion raises TerminalError (bad payloads
  must not retry).

App-facing stable surface:
    from polyqueue.jobs.dispatcher import TerminalError
    from polyqueue.jobs.context import JobContext
    from polyqueue.job_api import make_job
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import Callable  # noqa: TC003 — Pydantic needs this at runtime
from typing import TYPE_CHECKING, Any, get_args, get_origin, get_type_hints

from pydantic import BaseModel, ConfigDict, ValidationError

if TYPE_CHECKING:
    from polyqueue.jobs.context import JobContext

logger = logging.getLogger(__name__)


class TerminalError(Exception):
    """Raised by handlers (or dispatch) to mark a job as permanently failed — no retry."""


class RegisteredHandler(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    fn: Callable[..., Any]
    payload_model: type[BaseModel] | None
    result_model: type[BaseModel] | None


_HANDLERS: dict[str, RegisteredHandler] = {}
_HANDLERS_BY_FN: dict[int, str] = {}  # id(fn) -> job_type


def _unwrap_optional(annotation: Any) -> Any:
    """Unwrap ``X | None`` / ``Optional[X]`` to ``X``."""
    origin = get_origin(annotation)
    if origin is None:
        return annotation
    args = [a for a in get_args(annotation) if a is not type(None)]
    if len(args) == 1:
        return args[0]
    return annotation


def _is_pydantic_model_type(tp: object) -> bool:
    try:
        return isinstance(tp, type) and issubclass(tp, BaseModel)
    except TypeError:
        return False


def _infer_payload_model(
    handler: Callable[..., Any],
) -> type[BaseModel] | None:
    """Inspect the handler's second parameter annotation.

    Returns the annotation if it is a BaseModel subclass, otherwise None.
    Uses ``get_type_hints`` so string annotations (``from __future__ import
    annotations``) are resolved to their actual types.
    """
    sig = inspect.signature(handler)
    params = list(sig.parameters.keys())
    if len(params) < 2:
        return None

    try:
        hints = get_type_hints(handler)
    except Exception:
        return None

    annotation = hints.get(params[1])
    if annotation is None:
        return None

    unwrapped = _unwrap_optional(annotation)
    if _is_pydantic_model_type(unwrapped):
        return unwrapped  # type: ignore[return-value]
    return None


def infer_result_model(
    handler: Callable[..., Any],
) -> type[BaseModel] | None:
    """Inspect the handler's return annotation.

    Returns the annotation if it is a BaseModel subclass, otherwise None.
    Uses ``get_type_hints`` so string annotations (``from __future__ import
    annotations``) are resolved to their actual types.
    """
    try:
        hints = get_type_hints(handler)
    except Exception:
        return None

    annotation = hints.get("return")
    if annotation is None:
        return None

    unwrapped = _unwrap_optional(annotation)
    if _is_pydantic_model_type(unwrapped):
        return unwrapped  # type: ignore[return-value]
    return None


def _register(job_type: str, handler: Callable[..., Any]) -> None:
    """Internal: register a handler for a job type.

    Use ``make_job(job_type, handler)`` as the public API.
    """
    payload_model = _infer_payload_model(handler)
    result_model = infer_result_model(handler)
    _HANDLERS[job_type] = RegisteredHandler(
        fn=handler, payload_model=payload_model, result_model=result_model
    )
    _HANDLERS_BY_FN[id(handler)] = job_type
    model_name = payload_model.__name__ if payload_model else "dict"
    result_name = result_model.__name__ if result_model else "Any"
    logger.info(
        "registered job_type=%s payload_model=%s result_model=%s",
        job_type,
        model_name,
        result_name,
    )


def job_type_for_handler(fn: Callable[..., Any]) -> str:
    """Return the registered job type for a handler function.

    Raises:
        KeyError: if the handler has not been registered.
    """
    job_type = _HANDLERS_BY_FN.get(id(fn))
    if job_type is None:
        raise KeyError(
            f"Handler {fn.__qualname__!r} is not registered. "
            "Use make_job(job_type, handler) to register it."
        )
    return job_type


def registered_job_types() -> list[str]:
    """Return a sorted list of currently registered job types."""
    return sorted(_HANDLERS)


async def dispatch(ctx: JobContext, payload: dict[str, Any]) -> Any:
    registered = _HANDLERS.get(ctx.job_type)
    if registered is None:
        raise TerminalError(
            f"Unknown job type: {ctx.job_type!r}. "
            f"Registered types: {registered_job_types()}"
        )

    parsed: Any = payload
    if registered.payload_model is not None:
        try:
            parsed = registered.payload_model.model_validate(payload)
        except ValidationError as exc:
            raise TerminalError(
                f"Payload validation failed for job_type={ctx.job_type!r}: {exc}"
            ) from exc

    return await registered.fn(ctx, parsed)
