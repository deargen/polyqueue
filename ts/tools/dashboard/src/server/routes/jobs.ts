import { queryJobs } from "../queries/jobs.ts"
import type { JobsFilter } from "../../shared/types.ts"

export async function handleJobs(req: Request): Promise<Response> {
  const url = new URL(req.url)
  const filter: JobsFilter = {
    status: url.searchParams.get("status") ?? undefined,
    timedOut: url.searchParams.get("timedOut") === "true",
    staleProgress: url.searchParams.get("staleProgress") === "true",
    limit: (() => {
      const raw = Number(url.searchParams.get("limit"))
      return Number.isFinite(raw) && raw > 0 ? Math.min(raw, 200) : 50
    })(),
    q: url.searchParams.get("q") ?? undefined,
  }
  const data = await queryJobs(filter)
  return new Response(JSON.stringify(data), {
    headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
  })
}
