import { existsSync } from "node:fs"
import { join } from "node:path"
import { getConfig } from "./config.ts"
import { handleSnapshot } from "./routes/snapshot.ts"
import { handleWorkers } from "./routes/workers.ts"
import { handleJobs } from "./routes/jobs.ts"
import { handleJobDetail } from "./routes/jobDetail.ts"
import { handleAdminRequeue, handleAdminFail } from "./routes/admin.ts"
import { handleConfig } from "./routes/config.ts"
import { ensureSchema } from "./schema.ts"

const { port } = getConfig()

await ensureSchema()
console.log("Schema ready.")

const DIST_DIR = join(import.meta.dir, "../../dist")

function serveStatic(pathname: string): Response | null {
  if (!existsSync(DIST_DIR)) return null

  // Strip leading slash, default to index.html
  const relative = pathname === "/" || !pathname.includes(".") ? "index.html" : pathname.slice(1)
  const filePath = join(DIST_DIR, relative)

  // Guard against path traversal attacks
  if (!filePath.startsWith(DIST_DIR + "/") && filePath !== DIST_DIR) {
    return new Response("Forbidden", { status: 403 })
  }

  if (!existsSync(filePath)) {
    // SPA fallback
    const indexPath = join(DIST_DIR, "index.html")
    if (existsSync(indexPath)) {
      return new Response(Bun.file(indexPath))
    }
    return null
  }

  return new Response(Bun.file(filePath))
}

function corsHeaders(): HeadersInit {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
  }
}

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json", ...corsHeaders() },
  })
}

Bun.serve({
  port,
  async fetch(req) {
    if (req.method === "OPTIONS") {
      return new Response(null, { headers: corsHeaders() })
    }

    const url = new URL(req.url)
    const { pathname } = url

    try {
      if (pathname === "/api/health") {
        return json({ ok: true })
      }

      if (pathname === "/api/config" && req.method === "GET") {
        return handleConfig(req)
      }

      if (pathname === "/api/snapshot" && req.method === "GET") {
        return handleSnapshot(req)
      }

      if (pathname === "/api/workers" && req.method === "GET") {
        return handleWorkers(req)
      }
      if (pathname === "/api/jobs" && req.method === "GET") {
        return handleJobs(req)
      }
      const jobDetailMatch = pathname.match(/^\/api\/jobs\/([^/]+)$/)
      if (jobDetailMatch && req.method === "GET") {
        return handleJobDetail(req, decodeURIComponent(jobDetailMatch[1]))
      }

      const requeueMatch = pathname.match(/^\/api\/jobs\/([^/]+)\/requeue$/)
      if (requeueMatch && req.method === "POST") {
        return handleAdminRequeue(req, decodeURIComponent(requeueMatch[1]))
      }
      const failMatch = pathname.match(/^\/api\/jobs\/([^/]+)\/fail$/)
      if (failMatch && req.method === "POST") {
        return handleAdminFail(req, decodeURIComponent(failMatch[1]))
      }

      // Block unknown /api/* from falling through to static serving
      if (pathname.startsWith("/api/")) {
        return new Response("Not Found", { status: 404, headers: corsHeaders() })
      }

      // Static file serving (production mode)
      const staticResponse = serveStatic(pathname)
      if (staticResponse) return staticResponse

      return new Response("Not Found", { status: 404, headers: corsHeaders() })
    } catch (err) {
      console.error(err)
      return json({ error: String(err) }, 500)
    }
  },
})

console.log(`Polyqueue dashboard server listening on http://localhost:${port}`)
