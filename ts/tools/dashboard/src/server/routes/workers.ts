import { queryWorkers } from "../queries/workers.ts"

export async function handleWorkers(req: Request): Promise<Response> {
  const url = new URL(req.url)
  const status = url.searchParams.get("status") ?? undefined
  const data = await queryWorkers(status)
  return new Response(JSON.stringify(data), {
    headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
  })
}
