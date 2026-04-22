import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import type { ReactNode } from "react"
import { fetchJobDetail, postFail, postRequeue } from "../api.ts"
import type { JobAttemptEvent } from "../../shared/types.ts"
import { StatusBadge } from "./StatusBadge.tsx"

function JsonBlock({ value }: { value: unknown }) {
  if (value === null || value === undefined) {
    return <span className="text-gray-600 italic">null</span>
  }
  return (
    <pre className="overflow-x-auto rounded bg-gray-950 p-3 text-xs text-gray-300 font-mono max-h-48">
      {JSON.stringify(value, null, 2)}
    </pre>
  )
}

function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="space-y-1">
      <div className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
        {label}
      </div>
      <div>{children}</div>
    </div>
  )
}

/** Group events by attempt_number for display. */
function groupAttempts(events: JobAttemptEvent[]): Array<{
  number: number
  events: JobAttemptEvent[]
  finalOutcome: string
  workerId: string | null
  durationMs: number | null
}> {
  const byAttempt = new Map<number, JobAttemptEvent[]>()
  for (const ev of events) {
    const existing = byAttempt.get(ev.attempt_number)
    if (existing) {
      existing.push(ev)
    } else {
      byAttempt.set(ev.attempt_number, [ev])
    }
  }
  const groups: ReturnType<typeof groupAttempts> = []
  for (const [number, evs] of Array.from(byAttempt.entries()).sort((a, b) => a[0] - b[0])) {
    const terminal = evs.filter((e) => e.event_type !== "claimed")
    // Latest terminal event wins. The atomic lease_token predicate in db.py
    // makes it impossible for more than one terminal event per attempt to
    // land (except when an admin action overrides after a worker already
    // finalized — in which case admin's event is most recent and should win).
    const latest = terminal[terminal.length - 1]
    const finalOutcome = latest?.event_type ?? "in-flight"
    const claimed = evs.find((e) => e.event_type === "claimed")
    const workerId = claimed?.worker_id ?? evs[0].worker_id
    const durationMs = terminal.find((e) => e.duration_ms != null)?.duration_ms ?? null
    groups.push({ number, events: evs, finalOutcome, workerId, durationMs })
  }
  return groups
}

function AttemptOutcomeBadge({ outcome }: { outcome: string }) {
  const color =
    outcome === "succeeded"
      ? "bg-emerald-900 text-emerald-200 border-emerald-700"
      : outcome === "failed"
        ? "bg-red-900 text-red-200 border-red-700"
        : outcome === "abandoned"
          ? "bg-amber-900 text-amber-200 border-amber-700"
          : "bg-blue-900 text-blue-200 border-blue-700"
  return (
    <span className={`inline-block rounded border px-2 py-0.5 text-xs ${color}`}>
      {outcome}
    </span>
  )
}

interface JobDetailPanelProps {
  jobId: string
  paused: boolean
  onClose: () => void
}

export function JobDetailPanel({
  jobId,
  paused,
  onClose,
}: JobDetailPanelProps) {
  const queryClient = useQueryClient()

  const detail = useQuery({
    queryKey: ["job", jobId],
    queryFn: () => fetchJobDetail(jobId),
    refetchInterval: paused ? false : 2000,
  })

  const requeue = useMutation({
    mutationFn: () => postRequeue(jobId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["jobs"] })
      void queryClient.invalidateQueries({ queryKey: ["job", jobId] })
      void queryClient.invalidateQueries({ queryKey: ["snapshot"] })
    },
  })

  const fail = useMutation({
    mutationFn: () => postFail(jobId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["jobs"] })
      void queryClient.invalidateQueries({ queryKey: ["job", jobId] })
      void queryClient.invalidateQueries({ queryKey: ["snapshot"] })
    },
  })

  const job = detail.data?.job
  const attempts = detail.data?.attempts ?? []
  const grouped = groupAttempts(attempts)

  return (
    <div className="flex h-full flex-col">
      {/* Header */}
      <div className="flex items-center justify-between border-b border-gray-800 pb-3 mb-4">
        <div className="space-y-1">
          <div className="text-sm font-bold text-gray-100">Job Detail</div>
          <div className="font-mono text-xs text-gray-400" title={jobId}>
            {jobId}
          </div>
        </div>
        <button
          onClick={onClose}
          className="text-gray-500 hover:text-gray-100 text-lg leading-none"
          aria-label="Close"
        >
          ✕
        </button>
      </div>

      {detail.isLoading && !detail.data && (
        <div className="text-gray-500 text-sm">Loading…</div>
      )}
      {detail.isError && (
        <div className="text-red-400 text-sm">Error: {String(detail.error)}</div>
      )}

      {job && (
        <div className="flex-1 overflow-y-auto space-y-4">
          {/* Meta row */}
          <div className="flex flex-wrap gap-4">
            <Field label="Status">
              <StatusBadge status={job.status} />
            </Field>
            <Field label="Type">
              <span className="font-mono text-sm text-gray-200">
                {job.job_type}
              </span>
            </Field>
            <Field label="Attempts">
              <span className="font-mono text-sm text-gray-200">
                {job.attempt_count} / {job.max_attempts}
              </span>
            </Field>
            {job.timeout_strategy && (
              <Field label="Timeout Strategy">
                <span className="font-mono text-sm text-gray-200">
                  {job.timeout_strategy}
                </span>
              </Field>
            )}
          </div>

          {/* Timing */}
          <div className="grid grid-cols-2 gap-3 text-xs">
            {[
              ["Created", job.created_at],
              ["Started", job.started_at],
              ["Finished", job.finished_at],
              ["Claimed At", job.claimed_at],
              ["Timeout At", job.timeout_at],
              ["Progress Heartbeat", job.progress_heartbeat_at],
            ].map(([label, val]) => (
              <div key={label}>
                <div className="text-gray-500 uppercase tracking-wide text-xs">
                  {label}
                </div>
                <div className="font-mono text-gray-300">
                  {val ? new Date(val as string).toLocaleString() : "—"}
                </div>
              </div>
            ))}
          </div>

          {/* Worker (current claim, if in-flight) */}
          {job.claimed_by_worker_id && (
            <Field label="Worker (current)">
              <div className="font-mono text-xs text-gray-400">
                {job.claimed_by_worker_id} ({job.claimed_by_hostname}:
                {job.claimed_by_pid})
              </div>
            </Field>
          )}

          {/* Attempts history */}
          {grouped.length > 0 && (
            <Field label={`Attempts (${grouped.length})`}>
              <div className="space-y-2">
                {grouped.map((g) => (
                  <div
                    key={g.number}
                    className="rounded border border-gray-800 bg-gray-900/40 p-2"
                  >
                    <div className="flex items-center gap-2 text-xs">
                      <span className="font-mono text-gray-400">#{g.number}</span>
                      <AttemptOutcomeBadge outcome={g.finalOutcome} />
                      {g.durationMs != null && (
                        <span className="text-gray-500">{g.durationMs} ms</span>
                      )}
                    </div>
                    <div className="mt-1 font-mono text-[11px] text-gray-400 break-all">
                      {g.workerId ?? "—"}
                    </div>
                    <ul className="mt-1 space-y-0.5 text-[11px] text-gray-500">
                      {g.events.map((e) => (
                        <li key={e.event_id} className="flex gap-2">
                          <span className="w-20">{e.event_type}</span>
                          <span className="w-20">{e.finalized_by ?? "—"}</span>
                          <span className="flex-1 font-mono">
                            {new Date(e.event_at).toLocaleString()}
                          </span>
                          {e.error_message && (
                            <span className="text-red-400 truncate max-w-xs" title={e.error_message}>
                              {e.error_message}
                            </span>
                          )}
                        </li>
                      ))}
                    </ul>
                  </div>
                ))}
              </div>
            </Field>
          )}

          {/* Payload */}
          <Field label="Payload">
            <JsonBlock value={job.payload} />
          </Field>

          {/* Result */}
          <Field label="Result">
            <JsonBlock value={job.result} />
          </Field>

          {/* Error */}
          {(job.error_code || job.last_error) && (
            <Field label="Error">
              {job.error_code && (
                <div className="font-mono text-xs text-red-400 mb-1">
                  {job.error_code}
                </div>
              )}
              {job.last_error && (
                <pre className="rounded bg-gray-950 p-3 text-xs text-red-300 font-mono overflow-x-auto max-h-32">
                  {job.last_error}
                </pre>
              )}
            </Field>
          )}

          {/* Admin actions */}
          <div className="border-t border-gray-800 pt-4 space-y-2">
            <div className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
              Admin Actions
            </div>
            {requeue.isError && (
              <div className="text-red-400 text-xs">
                {String(requeue.error)}
              </div>
            )}
            {fail.isError && (
              <div className="text-red-400 text-xs">{String(fail.error)}</div>
            )}
            <div className="flex gap-2">
              <button
                onClick={() => requeue.mutate()}
                disabled={requeue.isPending}
                className="rounded bg-blue-800 px-3 py-1.5 text-xs text-white hover:bg-blue-700 disabled:opacity-50 transition-colors"
              >
                {requeue.isPending ? "Requeueing…" : "Requeue"}
              </button>
              <button
                onClick={() => fail.mutate()}
                disabled={fail.isPending}
                className="rounded bg-red-900 px-3 py-1.5 text-xs text-white hover:bg-red-800 disabled:opacity-50 transition-colors"
              >
                {fail.isPending ? "Failing…" : "Fail"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
