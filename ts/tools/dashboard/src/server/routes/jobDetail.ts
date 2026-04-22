import { queryJobDetail } from "../queries/jobDetail.ts"
import { queryJobAttempts } from "../queries/jobAttempts.ts"

export async function handleJobDetail(_req: Request, jobId: string): Promise<Response> {
  const [job, attempts] = await Promise.all([
    queryJobDetail(jobId),
    queryJobAttempts(jobId),
  ])
  if (!job) {
    return new Response(JSON.stringify({ error: "Not found" }), {
      status: 404,
      headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
    })
  }
  return new Response(JSON.stringify({ job, attempts }), {
    headers: { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" },
  })
}
