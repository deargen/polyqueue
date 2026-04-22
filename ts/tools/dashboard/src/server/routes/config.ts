import { getConfig } from "../config.ts"
import type { ServerConfigResponse } from "../../shared/types.ts"

export function handleConfig(_req: Request): Response {
  const { staleProgressSeconds, workerStaleSeconds } = getConfig()
  const data: ServerConfigResponse = {
    stale_progress_seconds: staleProgressSeconds,
    worker_stale_seconds: workerStaleSeconds,
  }
  return new Response(JSON.stringify(data), {
    headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
  })
}
