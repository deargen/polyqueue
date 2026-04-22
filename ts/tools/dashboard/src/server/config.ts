/** Reads POLYQUEUE_* env vars and builds safe, double-quoted table identifiers.
 *
 * Tables are always named {prefix}_jobs, {prefix}_workers, {prefix}_jobs_attempts.
 * Configure via POLYQUEUE_TABLE_PREFIX (default "polyqueue") and
 * POLYQUEUE_TABLE_SCHEMA (default "polyqueue").
 */

function parsePositiveInt(value: string | undefined, fallback: number): number {
  const n = Number(value)
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : fallback
}

function q(name: string): string {
  return `"${name.replace(/"/g, '""')}"`
}

function buildTableIdent(schema: string, table: string): string {
  return `${q(schema)}.${q(table)}`
}

export interface ServerConfig {
  jobsTable: string
  workersTable: string
  attemptsTable: string
  staleProgressSeconds: number
  workerStaleSeconds: number
  port: number
}

let _config: ServerConfig | null = null

export function getConfig(): ServerConfig {
  if (_config) return _config

  const schema = process.env.POLYQUEUE_TABLE_SCHEMA ?? "polyqueue"
  const prefix = process.env.POLYQUEUE_TABLE_PREFIX ?? "polyqueue"

  _config = {
    jobsTable: buildTableIdent(schema, `${prefix}_jobs`),
    workersTable: buildTableIdent(schema, `${prefix}_workers`),
    attemptsTable: buildTableIdent(schema, `${prefix}_jobs_attempts`),
    staleProgressSeconds: parsePositiveInt(process.env.POLYQUEUE_STALE_PROGRESS_THRESHOLD, 120),
    workerStaleSeconds: parsePositiveInt(process.env.POLYQUEUE_WORKER_STALE_THRESHOLD, 60),
    port: parsePositiveInt(process.env.PORT, 3030),
  }
  return _config
}
