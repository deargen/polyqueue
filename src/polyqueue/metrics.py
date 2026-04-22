"""Metrics hook interface — pluggable event emission for monitoring.

Default is no-op. Users can provide a Prometheus, CloudWatch, or
OpenTelemetry implementation.

Usage::

    from polyqueue.metrics import MetricsHook, NoOpMetrics

    class PrometheusMetrics(MetricsHook):
        def job_enqueued(self, job_type: str) -> None:
            JOBS_ENQUEUED.labels(job_type=job_type).inc()
        ...
"""

from __future__ import annotations

from typing import Protocol


class MetricsHook(Protocol):
    """Protocol for polyqueue metrics emission."""

    def job_enqueued(self, job_type: str) -> None: ...
    def job_started(self, job_type: str, worker_id: str) -> None: ...
    def job_succeeded(self, job_type: str, duration_ms: int) -> None: ...
    def job_failed(self, job_type: str, error_code: str, duration_ms: int) -> None: ...
    def job_timed_out(self, job_type: str, strategy: str, duration_ms: int) -> None: ...
    def job_retried(self, job_type: str, attempt: int) -> None: ...
    def worker_heartbeat(self, worker_id: str) -> None: ...
    def worker_started(self, worker_id: str) -> None: ...
    def worker_stopped(self, worker_id: str) -> None: ...


class NoOpMetrics:
    """Default no-op metrics — all methods are silent."""

    def job_enqueued(self, job_type: str) -> None:
        pass

    def job_started(self, job_type: str, worker_id: str) -> None:
        pass

    def job_succeeded(self, job_type: str, duration_ms: int) -> None:
        pass

    def job_failed(self, job_type: str, error_code: str, duration_ms: int) -> None:
        pass

    def job_timed_out(self, job_type: str, strategy: str, duration_ms: int) -> None:
        pass

    def job_retried(self, job_type: str, attempt: int) -> None:
        pass

    def worker_heartbeat(self, worker_id: str) -> None:
        pass

    def worker_started(self, worker_id: str) -> None:
        pass

    def worker_stopped(self, worker_id: str) -> None:
        pass
