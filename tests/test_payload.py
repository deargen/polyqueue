from pydantic import BaseModel

from polyqueue.utils.payload import normalize_payload


class AddPayload(BaseModel):
    a: int
    b: int


def test_dict_passthrough() -> None:
    d = {"a": 1, "b": 2}
    assert normalize_payload(d) is d


def test_basemodel_serialised() -> None:
    p = AddPayload(a=1, b=2)
    result = normalize_payload(p)
    assert result == {"a": 1, "b": 2}
    assert isinstance(result, dict)


def test_basemodel_mode_json() -> None:
    """model_dump(mode='json') ensures nested types serialise to JSON primitives."""
    from datetime import UTC, datetime

    class WithDate(BaseModel):
        ts: datetime

    now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    result = normalize_payload(WithDate(ts=now))
    assert isinstance(result["ts"], str)


def test_rejects_non_dict_non_model() -> None:
    import pytest

    with pytest.raises(TypeError, match="dict or a Pydantic BaseModel"):
        normalize_payload("not a payload")  # type: ignore[arg-type]
