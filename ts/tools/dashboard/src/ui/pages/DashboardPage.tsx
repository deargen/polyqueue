import { useQuery } from "@tanstack/react-query"
import { useState } from "react"
import { clsx } from "clsx"
import { fetchSnapshot, fetchConfig } from "../api.ts"
import { JobDetailPanel } from "../components/JobDetailPanel.tsx"
import { JobsTable } from "../components/JobsTable.tsx"
import { RefreshControl } from "../components/RefreshControl.tsx"
import { SnapshotCards } from "../components/SnapshotCards.tsx"
import { WorkersTable } from "../components/WorkersTable.tsx"

export function DashboardPage() {
  const [paused, setPaused] = useState(false)
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null)

  const snapshot = useQuery({
    queryKey: ["snapshot"],
    queryFn: fetchSnapshot,
    refetchInterval: paused ? false : 1000,
  })

  const config = useQuery({
    queryKey: ["config"],
    queryFn: fetchConfig,
    staleTime: 60_000,
  })

  return (
    <div className="min-h-screen bg-gray-950">
      {/* Main content */}
      <div
        className={clsx(
          "flex flex-col gap-4 p-4 transition-all duration-200",
          selectedJobId ? "mr-96" : "",
        )}
      >
        <div className="flex items-center justify-between">
          <h1 className="text-xl font-bold text-gray-100">Polyqueue Dashboard</h1>
          <RefreshControl paused={paused} onToggle={() => setPaused((p) => !p)} />
        </div>

        <SnapshotCards snapshot={snapshot.data} isLoading={snapshot.isLoading} />

        {snapshot.isError && (
          <div className="text-red-400 text-sm">
            Snapshot error: {String(snapshot.error)}
          </div>
        )}

        <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
          <div className="rounded-lg border border-gray-800 bg-gray-900 p-4 lg:col-span-1">
            <WorkersTable paused={paused} onSelectJob={setSelectedJobId} workerStaleSeconds={config.data?.worker_stale_seconds ?? 60} />
          </div>
          <div className="rounded-lg border border-gray-800 bg-gray-900 p-4 lg:col-span-2">
            <JobsTable
              paused={paused}
              selectedJobId={selectedJobId}
              onSelectJob={setSelectedJobId}
              staleProgressSeconds={config.data?.stale_progress_seconds ?? 120}
            />
          </div>
        </div>
      </div>

      {/* Job detail drawer */}
      {selectedJobId && (
        <div className="fixed inset-y-0 right-0 w-96 border-l border-gray-800 bg-gray-950 shadow-2xl overflow-y-auto p-4">
          <JobDetailPanel
            jobId={selectedJobId}
            paused={paused}
            onClose={() => setSelectedJobId(null)}
          />
        </div>
      )}
    </div>
  )
}
