# Polyqueue

Async-first Python job queue that keeps **Postgres as the authoritative job state** while supporting Redis, SQS, Azure Service Bus, PGMQ, and an in-process broker. Handlers are typed via Pydantic. Return values are persisted to the same Postgres DB — **no separate result backend required**.

---

| Dimension | Polyqueue | Celery | Taskiq |
|---|---|---|---|
| Primary model | Small queue runtime with **Postgres as source of truth** plus pluggable broker adapters | Mature distributed task queue with broad ecosystem/tooling | Async-first distributed task runner with modular brokers/extensions |
| Async support | **Yes, async-first**. Worker, handlers, DB access, and adapters are designed around asyncio | Not primarily async-first in overall design | **Yes, async-first** |
| Built-in broker/backend support | **Redis, SQS, Azure Service Bus, PGMQ, none (in-process)** | Commonly used with Redis/RabbitMQ and other broker/result-backend combinations | Broker-based, typically via pluggable integrations |
| Redis support | **Yes** | Yes | Yes, depending on broker plugin/config |
| SQS support | **Yes** | Yes | Possible via broker integration/plugin, but not the default mental model |
| Azure Service Bus support | **Yes** | Not a standard first-class default choice in the same way as Redis/RabbitMQ | Possible via ecosystem extension/custom broker, but not a core default pairing |
| No-broker / in-process mode | **Yes** via `queue_backend="none"` | Not the usual model | Not the main model |
| Source of truth for job state | **Postgres** | Broker/backend-centric task system, optionally paired with result backends | Broker/message-centric |
| Pydantic support | **Yes** — `enqueue()` accepts `BaseModel`, `dispatch()` validates annotated payload models via `model_validate()` | Available via explicit task-side support | Works naturally with typed task signatures and model parsing patterns |
| Pydantic behavior | **Strict**: model serialised to dict on enqueue, `model_validate()` on dispatch, `ValidationError` => `TerminalError` (no retry) | Usually task-side validation/conversion | More ergonomic/best-effort style than strict contract-first behavior |
| Result persistence | **Postgres `jobs.result` JSONB** — same DB already tracking job state, no extra infrastructure | Requires a separate result backend (Redis, DB, RPC, etc.) configured explicitly; off by default | Requires a separate result backend plugin (e.g. `taskiq-redis`); pluggable but extra infrastructure |
| Multi-worker distribution | **Yes**. Workers compete on claim; one worker owns the in-flight delivery | **Yes** | **Yes** |
| Worker identity tracking | **Yes** — `polyqueue_workers` registry: worker_id, hostname, pid, current job, heartbeat, status (running/stopped/dead); stale workers reaped by maintenance loop | Stronger built-in worker identity/inspection model | Workers exist, but less built-in cluster observability than Celery |
| In-flight ownership model | **Redis lease / SQS receipt handle + visibility timeout / Azure SB lock token + lock renewal** | Broker/worker-managed | Broker/worker-managed |
| Recovery when worker crashes | **Yes**, via lease expiry / visibility timeout / lock expiry and redelivery/reclaim | Yes | Yes |
| Detecting "alive but wedged" jobs | **Basic** — `progress_heartbeat_at` + `timeout_at` in DB; maintenance backstop reconciles timed-out jobs | Better operational story; mature controls exist | Typically needs app/broker-level design |
| Time limits | **Yes** — per-job `max_run_seconds`, cooperative timeout via `asyncio.wait_for`, DB backstop, three strategies (retry/fail/ignore) | More mature built-in support | Less central than in Celery's ops model |
| Operational maturity | Focused, understandable, smaller surface area | **Most mature** | Lighter-weight, async-friendly |
| Best fit | Internal service jobs where you want simplicity, explicit semantics, and control | Large-scale general background job platform | Async Python apps that want typed/ergonomic task execution |

## Summary

- **Brokers**: Redis, SQS, Azure Service Bus, PGMQ, or in-process (`none`). Postgres always holds the authoritative job state regardless of broker.
- **Pydantic-first**: handlers declare typed payloads via `@register`; `ValidationError` on dispatch becomes `TerminalError` (no retry).
- **Result persistence in Postgres**: handler return values (`BaseModel`, dict, or primitive) are saved to `jobs.result` JSONB — no separate result backend needed. Fetch via `get_job_result`, `get_job_result_typed`, or `get_job_result_typed_via_registry`. Not available with the `none`/in-process backend.
- **Time limits**: per-job `max_run_seconds` enforced via `asyncio.wait_for`, with three strategies (`retry` / `fail` / `ignore`) and a maintenance-loop DB backstop.
- **Worker registry**: `polyqueue_workers` table tracks id, hostname, pid, current job, and heartbeat. Stale workers are reaped to `dead` by the maintenance loop.
- **Metrics hook**: pluggable `MetricsHook` for Prometheus, CloudWatch, or OpenTelemetry. Pass to `main(metrics=...)`.
- **Admin CLI**: `polyqueue inspect stats|workers|jobs` and `polyqueue admin requeue|fail`.
- Smaller surface area than Celery — no admin UI or auto-scaling built-in.
