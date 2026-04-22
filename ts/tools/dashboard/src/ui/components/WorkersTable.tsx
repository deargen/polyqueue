import { useQuery } from "@tanstack/react-query"
import { clsx } from "clsx"
import { useState } from "react"
import { fetchWorkers } from "../api.ts"
import { StatusBadge } from "./StatusBadge.tsx"
import type { WorkerRow } from "../../shared/types.ts"

function isStaleHeartbeat(lastHeartbeat: string, staleSeconds = 60): boolean {
  return Date.now() - new Date(lastHeartbeat).getTime() > staleSeconds * 1000
}

interface WorkersTableProps {
  paused: boolean
  onSelectJob: (jobId: string) => void
  workerStaleSeconds: number
}

export function WorkersTable({ paused, onSelectJob, workerStaleSeconds }: WorkersTableProps) {
  const [statusFilter, setStatusFilter] = useState<string | undefined>(undefined)

  const workers = useQuery({
    queryKey: ["workers", statusFilter],
    queryFn: () => fetchWorkers(statusFilter),
    refetchInterval: paused ? false : 2000,
  })

  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center gap-2">
        <span className="text-sm font-semibold text-gray-300">Workers</span>
        <select
          className="ml-auto rounded border border-gray-700 bg-gray-900 px-2 py-1 text-sm text-gray-300"
          value={statusFilter ?? ""}
          onChange={(e) => setStatusFilter(e.target.value || undefined)}
        >
          <option value="">All statuses</option>
          <option value="running">Running</option>
          <option value="stopped">Stopped</option>
          <option value="dead">Dead</option>
        </select>
      </div>
      {workers.isLoading && !workers.data && (
        <div className="text-gray-500 text-xs">Loading workers…</div>
      )}
      {workers.isError && (
        <div className="text-red-400 text-xs">Error: {String(workers.error)}</div>
      )}
      <div className="overflow-x-auto">
        <table className="w-full text-xs text-gray-300">
          <thead>
            <tr className="border-b border-gray-800 text-left text-gray-500">
              <th className="pb-1 pr-3">Worker ID</th>
              <th className="pb-1 pr-3">Status</th>
              <th className="pb-1 pr-3">Backend</th>
              <th className="pb-1 pr-3">Host</th>
              <th className="pb-1 pr-3">PID</th>
              <th className="pb-1 pr-3">Current Job</th>
              <th className="pb-1">Last Heartbeat</th>
            </tr>
          </thead>
          <tbody>
            {workers.data?.length === 0 && (
              <tr>
                <td colSpan={7} className="py-4 text-center text-gray-600">
                  No workers
                </td>
              </tr>
            )}
            {workers.data?.map((w: WorkerRow) => (
              <tr
                key={w.worker_id}
                className={clsx(
                  "border-b border-gray-900 hover:bg-gray-900",
                  isStaleHeartbeat(w.last_heartbeat_at, workerStaleSeconds) && w.status === "running" && "bg-yellow-950/30",
                )}
              >
                <td className="py-1 pr-3 font-mono text-gray-400" title={w.worker_id}>
                  {w.worker_id.slice(-12)}
                </td>
                <td className="py-1 pr-3">
                  <StatusBadge status={w.status} />
                </td>
                <td className="py-1 pr-3 text-gray-400">{w.backend}</td>
                <td className="py-1 pr-3 text-gray-400">{w.hostname}</td>
                <td className="py-1 pr-3 font-mono text-gray-400">{w.pid}</td>
                <td className="py-1 pr-3">
                  {w.current_job_id ? (
                    <button
                      className="font-mono text-blue-400 hover:underline"
                      onClick={() => onSelectJob(w.current_job_id!)}
                    >
                      {w.current_job_id.slice(-12)}
                    </button>
                  ) : (
                    <span className="text-gray-600">—</span>
                  )}
                </td>
                <td
                  className={clsx(
                    "py-1 font-mono",
                    isStaleHeartbeat(w.last_heartbeat_at, workerStaleSeconds) && w.status === "running"
                      ? "text-yellow-400"
                      : "text-gray-400",
                  )}
                >
                  {new Date(w.last_heartbeat_at).toLocaleTimeString()}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
