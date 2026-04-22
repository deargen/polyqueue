import os
import re

from polyqueue.worker.identity import make_worker_info


def test_worker_info_fields() -> None:
    info = make_worker_info()
    assert info.hostname != ""
    assert info.pid == os.getpid()
    assert info.started_at is not None
    assert re.match(r".+:\d+:[0-9a-f-]+", info.worker_id)


def test_worker_info_with_name() -> None:
    info = make_worker_info(worker_name="worker-a")
    assert info.worker_id.startswith("worker-a:")
    assert info.hostname != ""


def test_worker_info_frozen() -> None:
    import pytest
    from pydantic import ValidationError

    info = make_worker_info()
    with pytest.raises(ValidationError, match="frozen_instance"):
        info.worker_id = "changed"  # type: ignore[misc]
