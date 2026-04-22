"""Tests for normalize_result()."""

from pydantic import BaseModel

from polyqueue.utils.result import normalize_result


class Point(BaseModel):
    x: int
    y: int


def test_none_returns_none():
    assert normalize_result(None) is None


def test_pydantic_model_serialised_to_dict():
    result = normalize_result(Point(x=1, y=2))
    assert result == {"x": 1, "y": 2}


def test_dict_passed_through():
    data = {"key": "value", "n": 42}
    assert normalize_result(data) == data


def test_primitive_int_passed_through():
    assert normalize_result(42) == 42


def test_primitive_str_passed_through():
    assert normalize_result("hello") == "hello"


def test_list_passed_through():
    assert normalize_result([1, 2, 3]) == [1, 2, 3]


def test_non_serialisable_dict_raises_type_error():
    import datetime

    with __import__("pytest").raises(TypeError):
        normalize_result({"ts": datetime.datetime.now(tz=datetime.timezone.utc)})
