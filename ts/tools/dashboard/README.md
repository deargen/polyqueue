# Polyqueue Dashboard

Live admin dashboard for a running Polyqueue deployment. Shows queue snapshot metrics, worker status, job list with filters, job detail, and admin requeue/fail actions.

All commands are run from the **`ts/tools/dashboard`** directory unless stated otherwise.

---

## Quick start (dev mode — three terminals)

### 1. Start throwaway containers (Podman)

```bash
# Postgres
podman run -d --name pq-demo-pg \
  -e POSTGRES_USER=pq -e POSTGRES_PASSWORD=pq -e POSTGRES_DB=pq \
  -p 5432:5432 \
  postgres:16-alpine

# Redis (only needed if you are also running the polyqueue worker demo)
podman run -d --name pq-demo-redis \
  -p 6379:6379 \
  redis:7-alpine
```

Stop and remove when done:

```bash
podman rm -f pq-demo-pg pq-demo-redis
```

### 2. Install dependencies (once)

From the repo root `ts/` directory:

```bash
bun install
```

### 3. Terminal 1 — Bun API server

```bash
POLYQUEUE_DB_URL=postgresql://pq:pq@localhost/pq \
bun run dev:server
```

Expected output:

```
Schema ready.
Polyqueue dashboard server listening on http://localhost:3030
```

The server creates the `polyqueue_jobs` and `polyqueue_workers` tables automatically on first start.

### 4. Terminal 2 — Vite UI

```bash
bun run dev:ui
```

Open **http://localhost:5183** in your browser.

### 5. Terminal 3 — (optional) polyqueue worker demo

To have live jobs flowing through the queue, run the polyqueue Python demo alongside the dashboard.

See [`python/packages/polyqueue/tools/demo/README.md`](../../../../python/packages/polyqueue/tools/demo/README.md) for the full setup — use the same DB URL (`postgresql+asyncpg://pq:pq@localhost/pq`) and Redis URL.

---

## Production mode (single process)

Build the UI once, then start the Bun server — it serves both the API and the built frontend:

```bash
bun run build

POLYQUEUE_DB_URL=postgresql://pq:pq@localhost/pq \
bun run start
```

Open **http://localhost:3030** in your browser.

---

## What the dashboard shows

| Panel | Description |
|-------|-------------|
| **Snapshot cards** | 9 live metrics: queued, processing, succeeded, failed, timed-out, stale-progress, healthy workers, running workers, oldest queued age |
| **Workers table** | All registered workers with status, backend, hostname, PID, current job, and last heartbeat. Stale running workers are highlighted in yellow. |
| **Jobs table** | Most recent jobs with status filter, timed-out/stale-progress checkboxes, free-text search, and row limit. Timed-out rows are red; stale-progress rows are yellow. |
| **Job detail panel** | Click any job row to open a side drawer: full job fields, payload/result JSON, error details, and admin Requeue/Fail buttons. |

All panels refresh automatically every 1–2 seconds. Use the **Pause / Resume** button to freeze polling.

---

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `POLYQUEUE_DB_URL` | — | Postgres connection URL (**required**). Use `postgresql://` (sync) for the dashboard — not `postgresql+asyncpg://`. |
| `POLYQUEUE_TABLE_SCHEMA` | `public` | Schema containing the jobs and workers tables |
| `POLYQUEUE_TABLE_NAME` | `polyqueue_jobs` | Jobs table name |
| `POLYQUEUE_WORKERS_TABLE_NAME` | `polyqueue_workers` | Workers table name |
| `POLYQUEUE_STALE_PROGRESS_THRESHOLD` | `120` | Seconds before a processing job's heartbeat is considered stale |
| `POLYQUEUE_WORKER_STALE_THRESHOLD` | `60` | Seconds before a running worker's heartbeat is considered stale |
| `PORT` | `3030` | Port for the Bun API/static server |
