"""Shared payload normalisation for all queue backends."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


def normalize_payload(payload: dict[str, Any] | BaseModel) -> dict[str, Any]:
    """Convert a Pydantic model to a JSON-safe dict, or pass a dict through.

    Called by every backend's ``enqueue()`` so the wire format is always a plain dict.

    Raises:
        TypeError: if payload is neither a dict nor a BaseModel.
    """
    if isinstance(payload, BaseModel):
        data = payload.model_dump(mode="json")
        if not isinstance(data, dict):
            raise TypeError(
                f"Pydantic payload must serialise to a JSON object, got {type(data).__name__}"
            )
        return data
    if isinstance(payload, dict):
        return payload
    raise TypeError(
        f"payload must be a dict or a Pydantic BaseModel, got {type(payload).__name__}"
    )
