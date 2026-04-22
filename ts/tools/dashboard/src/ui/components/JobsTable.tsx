import { useQuery } from "@tanstack/react-query"
import { clsx } from "clsx"
import { useState } from "react"
import { fetchJobs } from "../api.ts"
import { StatusBadge } from "./StatusBadge.tsx"
import type { JobRow, JobsFilter } from "../../shared/types.ts"

function isStaleProgress(progressHeartbeat: string | null, staleSeconds = 120): boolean {
  if (!progressHeartbeat) return false
  return Date.now() - new Date(progressHeartbeat).getTime() > staleSeconds * 1000
}

function isTimedOut(timeoutAt: string | null): boolean {
  if (!timeoutAt) return false
  return new Date(timeoutAt).getTime() < Date.now()
}

interface JobsTableProps {
  paused: boolean
  selectedJobId: string | null
  onSelectJob: (jobId: string) => void
  staleProgressSeconds: number
}

export function JobsTable({ paused, selectedJobId, onSelectJob, staleProgressSeconds }: JobsTableProps) {
  const [filter, setFilter] = useState<JobsFilter>({ limit: 50 })

  const jobs = useQuery({
    queryKey: ["jobs", filter],
    queryFn: () => fetchJobs(filter),
    refetchInterval: paused ? false : 2000,
  })

  function updateFilter(patch: Partial<JobsFilter>) {
    setFilter((f) => ({ ...f, ...patch }))
  }

  return (
    <div className="flex flex-col gap-2">
      {/* Filter bar */}
      <div className="flex flex-wrap items-center gap-2">
        <span className="text-sm font-semibold text-gray-300">Jobs</span>
        <select
          className="rounded border border-gray-700 bg-gray-900 px-2 py-1 text-xs text-gray-300"
          value={filter.status ?? ""}
          onChange={(e) => {
            const nextStatus = e.target.value || undefined
            const compatible = !nextStatus || nextStatus === "processing"
            updateFilter({
              status: nextStatus,
              timedOut: compatible ? filter.timedOut : false,
              staleProgress: compatible ? filter.staleProgress : false,
            })
          }}
        >
          <option value="">All statuses</option>
          <option value="queued">Queued</option>
          <option value="processing">Processing</option>
          <option value="succeeded">Succeeded</option>
          <option value="failed">Failed</option>
        </select>
        <label className={`flex items-center gap-1 text-xs ${!filter.status || filter.status === "processing" ? "text-gray-400" : "text-gray-600 cursor-not-allowed"}`}>
          <input
            type="checkbox"
            checked={filter.timedOut ?? false}
            disabled={!(!filter.status || filter.status === "processing")}
            onChange={(e) => updateFilter({ timedOut: e.target.checked })}
          />
          Timed out
        </label>
        <label className={`flex items-center gap-1 text-xs ${!filter.status || filter.status === "processing" ? "text-gray-400" : "text-gray-600 cursor-not-allowed"}`}>
          <input
            type="checkbox"
            checked={filter.staleProgress ?? false}
            disabled={!(!filter.status || filter.status === "processing")}
            onChange={(e) => updateFilter({ staleProgress: e.target.checked })}
          />
          Stale progress
        </label>
        <input
          type="search"
          placeholder="Search id / type…"
          className="rounded border border-gray-700 bg-gray-900 px-2 py-1 text-xs text-gray-300 placeholder-gray-600"
          value={filter.q ?? ""}
          onChange={(e) => updateFilter({ q: e.target.value || undefined })}
        />
        <select
          className="ml-auto rounded border border-gray-700 bg-gray-900 px-2 py-1 text-xs text-gray-300"
          value={filter.limit ?? 50}
          onChange={(e) => updateFilter({ limit: Number(e.target.value) })}
        >
          <option value={20}>20</option>
          <option value={50}>50</option>
          <option value={100}>100</option>
          <option value={200}>200</option>
        </select>
      </div>

      {jobs.isLoading && !jobs.data && (
        <div className="text-gray-500 text-xs">Loading jobs…</div>
      )}
      {jobs.isError && (
        <div className="text-red-400 text-xs">Error: {String(jobs.error)}</div>
      )}

      <div className="overflow-x-auto">
        <table className="w-full text-xs text-gray-300">
          <thead>
            <tr className="border-b border-gray-800 text-left text-gray-500">
              <th className="pb-1 pr-3">ID</th>
              <th className="pb-1 pr-3">Type</th>
              <th className="pb-1 pr-3">Status</th>
              <th className="pb-1 pr-3">Attempts</th>
              <th className="pb-1 pr-3">Worker</th>
              <th className="pb-1 pr-3">Created</th>
              <th className="pb-1 pr-3">Started</th>
              <th className="pb-1 pr-3">Finished</th>
              <th className="pb-1">Error</th>
            </tr>
          </thead>
          <tbody>
            {jobs.data?.length === 0 && (
              <tr>
                <td colSpan={9} className="py-4 text-center text-gray-600">
                  No jobs match the current filter
                </td>
              </tr>
            )}
            {jobs.data?.map((job: JobRow) => {
              const stale = job.status === "processing" && isStaleProgress(job.progress_heartbeat_at, staleProgressSeconds)
              const timedOut = job.status === "processing" && isTimedOut(job.timeout_at)
              return (
                <tr
                  key={job.id}
                  className={clsx(
                    "border-b border-gray-900 cursor-pointer hover:bg-gray-800",
                    selectedJobId === job.id && "bg-blue-950/40",
                    timedOut && "bg-red-950/30",
                    stale && !timedOut && "bg-yellow-950/20",
                  )}
                  onClick={() => onSelectJob(job.id)}
                >
                  <td className="py-1 pr-3 font-mono text-blue-400" title={job.id}>
                    {job.id.slice(-12)}
                  </td>
                  <td className="py-1 pr-3 text-gray-300">{job.job_type}</td>
                  <td className="py-1 pr-3">
                    <StatusBadge status={job.status} />
                  </td>
                  <td className="py-1 pr-3 font-mono text-gray-400">
                    {job.attempt_count}/{job.max_attempts}
                  </td>
                  <td className="py-1 pr-3 font-mono text-gray-500" title={job.claimed_by_worker_id ?? ""}>
                    {job.claimed_by_worker_id?.slice(-12) ?? "—"}
                  </td>
                  <td className="py-1 pr-3 font-mono text-gray-400">
                    {new Date(job.created_at).toLocaleTimeString()}
                  </td>
                  <td className="py-1 pr-3 font-mono text-gray-400">
                    {job.started_at ? new Date(job.started_at).toLocaleTimeString() : "—"}
                  </td>
                  <td className="py-1 pr-3 font-mono text-gray-400">
                    {job.finished_at ? new Date(job.finished_at).toLocaleTimeString() : "—"}
                  </td>
                  <td className="py-1 font-mono text-red-400">
                    {job.error_code ?? ""}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
      <div className="text-right text-xs text-gray-600">
        {jobs.data?.length ?? 0} row(s)
      </div>
    </div>
  )
}
