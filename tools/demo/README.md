# polyqueue demo

Two scripts that show the full enqueue → process lifecycle.

All commands are run from the **`python/packages/polyqueue`** directory.

---

## Quick start — in-process (no services needed)

`enqueue.py` handles both sides when `POLYQUEUE_BACKEND=none`: it imports
worker `handlers.py` so job types are registered in-process, then runs jobs
as asyncio tasks in the same process.

Important: `InProcessQueue` does not have a separate worker process. The
same process that enqueues jobs must also import modules that register
handlers (`make_job(...)` or `@register_handler(...)`), otherwise dispatch
fails with `Unknown job type`.

```bash
POLYQUEUE_BACKEND=none uv run --reinstall enqueue.py
```

---

## Redis backend (two terminals)

### Start throwaway containers (Podman)

```bash
# Postgres
podman run -d --name pq-demo-pg \
  -e POSTGRES_USER=pq -e POSTGRES_PASSWORD=pq -e POSTGRES_DB=pq \
  -p 5432:5432 \
  postgres:16-alpine

# Redis
podman run -d --name pq-demo-redis \
  -p 6379:6379 \
  redis:7-alpine
```

Stop and remove when done:

```bash
podman rm -f pq-demo-pg pq-demo-redis
```

### Run the demo

**Terminal 1 — worker** (blocks, Ctrl-C to stop):

```bash
POLYQUEUE_BACKEND=redis \
POLYQUEUE_DB_URL=postgresql+asyncpg://pq:pq@localhost/pq \
POLYQUEUE_REDIS_URL=redis://localhost:6379 \
uv run --reinstall worker.py
```

**Terminal 2 — enqueue jobs**:

```bash
POLYQUEUE_BACKEND=redis \
POLYQUEUE_DB_URL=postgresql+asyncpg://pq:pq@localhost/pq \
POLYQUEUE_REDIS_URL=redis://localhost:6379 \
uv run --reinstall enqueue.py
```

`enqueue.py` creates the jobs table automatically on a fresh database.

---

## PGMQ backend (two terminals)

### Start Postgres with PGMQ preinstalled (Podman)

```bash
podman run -d --name pq-demo-pgmq \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  ghcr.io/pgmq/pg18-pgmq:v1.10.0
```

Enable the extension once:

```bash
podman exec -it pq-demo-pgmq \
  psql -U postgres -d postgres \
  -c "CREATE EXTENSION IF NOT EXISTS pgmq;"
```

Stop and remove when done:

```bash
podman rm -f pq-demo-pgmq
```

### Run the demo

`PgmqJobQueue` creates the queue automatically by default (`POLYQUEUE_PGMQ_CREATE_QUEUE_IF_MISSING=true`).

**Terminal 1 — worker** (blocks, Ctrl-C to stop):

```bash
POLYQUEUE_BACKEND=pgmq \
POLYQUEUE_DB_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/postgres \
POLYQUEUE_PGMQ_QUEUE_NAME=polyqueue \
uv run --reinstall worker.py
```

**Terminal 2 — enqueue jobs**:

```bash
POLYQUEUE_BACKEND=pgmq \
POLYQUEUE_DB_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/postgres \
POLYQUEUE_PGMQ_QUEUE_NAME=polyqueue \
uv run --reinstall enqueue.py
```

---

## Multi-worker demo (three terminals)

Shows that two workers claim different jobs concurrently. Two slow jobs (5s each)
block one worker each, while fast jobs get picked up immediately by whichever
worker finishes first.

**Terminal 1 — worker A:**

```bash
POLYQUEUE_BACKEND=redis \
POLYQUEUE_DB_URL=postgresql+asyncpg://pq:pq@localhost/pq \
uv run --reinstall worker.py
```

**Terminal 2 — worker B:**

```bash
POLYQUEUE_BACKEND=redis \
POLYQUEUE_DB_URL=postgresql+asyncpg://pq:pq@localhost/pq \
uv run --reinstall worker.py
```

**Terminal 3 — enqueue:**

```bash
POLYQUEUE_BACKEND=redis \
POLYQUEUE_DB_URL=postgresql+asyncpg://pq:pq@localhost/pq \
uv run --reinstall enqueue_multiworker.py
```

You should see each worker claim one slow job, then race to pick up the fast
`add` jobs as they finish.

The `will-timeout` job (30s sleep with `max_run_seconds=3`) demonstrates timeout
enforcement — the worker cancels it after 3 seconds and retries or fails it.

---

## Sample jobs

| job_type | payload | expected outcome |
|----------|---------|-----------------|
| `add` | `{"a": 1, "b": 2}` | logs `add(1, 2) = 3` |
| `add` | `{"a": 10, "b": 20}` | logs `add(10, 20) = 30` |
| `greet` | `{"name": "Alice"}` | logs `Hello, Alice!` |
| `greet` | `{"name": "Bob"}` | logs `Hello, Bob!` |
| `greet` | `{"name": "Charlie", "fail": true}` | raises `TerminalError` → permanent failure, no retry |
| `sleep` | `{"seconds": 5, "label": "slow-1"}` | sleeps 5s — used in multi-worker demo |
| `sleep` | `{"seconds": 30, "label": "will-timeout"}` + `max_run_seconds=3` | cancelled after 3s — demonstrates timeout enforcement |
| `slow_with_progress` | `{"seconds": N, "label": "..."}` | reports `heartbeat_progress()` each second |

---

## Typed enqueue API

`enqueue.py` and `enqueue_multiworker.py` use `job_spec(...)` commands from
`job_defs.py` on the enqueue side:

```python
from job_defs import AddPayload, SleepPayload, add, sleep
from polyqueue.enqueue_options import EnqueueOptions

# Enqueue with typed payload
await add.enqueue(queue, AddPayload(a=1, b=2))

# Enqueue with options (e.g. per-job timeout)
await sleep.enqueue(
    queue,
    SleepPayload(seconds=30, label="will-timeout"),
    options=EnqueueOptions(max_run_seconds=3),
)
```

Result typing note:
- `result_type` in `job_spec(job_type, payload_type, result_type)` must be a
  Pydantic model class (or omitted for `None` result). This keeps result
  deserialization stable and backend-agnostic.

The string-first API still works for backward compatibility:

```python
await queue.enqueue("add", AddPayload(a=1, b=2))
await queue.enqueue("add", {"a": 1, "b": 2}, options=EnqueueOptions(job_id="my-id"))
```

---

## JobContext fields available to handlers

Inside a handler, `ctx` exposes:

| Field | Type | Description |
|-------|------|-------------|
| `ctx.job_id` | `str` | Unique job ID |
| `ctx.job_type` | `str` | Registered job type string |
| `ctx.attempt` | `int` | Current attempt number (1-based) |
| `ctx.max_attempts` | `int` | Max retries configured for this job |
| `ctx.queued_at` | `datetime \| None` | When the job was originally enqueued |
| `ctx.claimed_at` | `datetime \| None` | When this worker claimed the job |
| `ctx.timeout_at` | `datetime \| None` | Absolute deadline (if `max_run_seconds` set) |
| `ctx.max_run_seconds` | `int \| None` | Per-job timeout in seconds |
| `ctx.options` | `EnqueueOptions \| None` | Effective enqueue options |
| `ctx.options.max_run_seconds` | `int \| None` | Timeout applied at enqueue time |
| `ctx.options.timeout_strategy` | `str \| None` | `"retry"` \| `"fail"` \| `"ignore"` |

---

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POLYQUEUE_BACKEND` | `redis` | `redis` \| `sqs` \| `azure_service_bus` \| `pgmq` \| `none` |
| `POLYQUEUE_DB_URL` | — | PostgreSQL async URL (required for non-`none` backends) |
| `POLYQUEUE_REDIS_URL` | `redis://localhost:6379` | Redis URL |
| `POLYQUEUE_PGMQ_QUEUE_NAME` | `polyqueue` | Queue name for the `pgmq` backend |
| `POLYQUEUE_PGMQ_VISIBILITY_TIMEOUT` | `660` | Visibility timeout (seconds) for PGMQ messages |
| `POLYQUEUE_PGMQ_POLL_SECONDS` | `5` | Max seconds to poll in `pgmq.read_with_poll(...)` |
| `POLYQUEUE_PGMQ_POLL_INTERVAL_MS` | `100` | Poll interval (milliseconds) for `read_with_poll` |
| `POLYQUEUE_MAX_ATTEMPTS` | `3` | Max retry attempts per job |
| `POLYQUEUE_DEFAULT_MAX_RUN_SECONDS` | — | Default per-job timeout (unset = no limit) |
| `POLYQUEUE_TIMEOUT_STRATEGY` | `retry` | `retry` \| `fail` \| `ignore` |
| `POLYQUEUE_PROGRESS_HEARTBEAT_INTERVAL` | `30` | Seconds between progress heartbeat DB updates |
| `POLYQUEUE_WORKER_NAME` | — | Label for worker_id (default: hostname) |
