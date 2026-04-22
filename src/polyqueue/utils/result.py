"""Result normalisation — converts handler return values to JSON-safe objects."""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel


def normalize_result(value: Any) -> Any:
    """Convert a handler return value to a JSON-serialisable form.

    - None → None (stored as SQL NULL)
    - BaseModel → dict via model_dump(mode="json")
    - Everything else (dict, list, int, str, …) → passed through unchanged

    Raises:
        TypeError: if value is not JSON-serialisable (e.g. contains datetime or UUID).
            Raised early so the worker can surface this as a terminal error before
            any DB writes, rather than failing inside mark_succeeded_with_result().
    """
    if value is None:
        return None
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    _ = json.dumps(value)  # raises TypeError early if not JSON-serialisable
    return value
