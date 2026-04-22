import type { QueueSnapshot } from "../../shared/types.ts"

interface CardProps {
  label: string
  value: string | number | null
  highlight?: "warn" | "danger" | "ok"
}

function Card({ label, value, highlight }: CardProps) {
  const colorClass =
    highlight === "danger"
      ? "border-red-800 text-red-300"
      : highlight === "warn"
        ? "border-yellow-800 text-yellow-300"
        : highlight === "ok"
          ? "border-green-800 text-green-300"
          : "border-gray-800 text-gray-100"

  return (
    <div className={`rounded-lg border bg-gray-900 p-4 ${colorClass}`}>
      <div className="text-xs text-gray-400 mb-1">{label}</div>
      <div className="text-2xl font-mono font-bold">
        {value === null ? "—" : value}
      </div>
    </div>
  )
}

interface SnapshotCardsProps {
  snapshot: QueueSnapshot | undefined
  isLoading: boolean
}

export function SnapshotCards({ snapshot, isLoading }: SnapshotCardsProps) {
  if (isLoading && !snapshot) {
    return <div className="text-gray-500 text-sm">Loading snapshot…</div>
  }
  if (!snapshot) return null

  const ageSeconds = snapshot.oldest_queued_age_seconds
  const ageLabel =
    ageSeconds === null
      ? null
      : ageSeconds < 60
        ? `${Math.round(ageSeconds)}s`
        : ageSeconds < 3600
          ? `${Math.round(ageSeconds / 60)}m`
          : `${Math.round(ageSeconds / 3600)}h`

  return (
    <div className="grid grid-cols-3 gap-3 sm:grid-cols-5 lg:grid-cols-9">
      <Card label="Queued" value={snapshot.queued_jobs} />
      <Card label="Processing" value={snapshot.processing_jobs} />
      <Card label="Succeeded" value={snapshot.succeeded_jobs} highlight="ok" />
      <Card label="Failed" value={snapshot.failed_jobs} highlight={snapshot.failed_jobs > 0 ? "danger" : undefined} />
      <Card label="Timed Out" value={snapshot.timed_out_processing_jobs} highlight={snapshot.timed_out_processing_jobs > 0 ? "warn" : undefined} />
      <Card label="Stale Progress" value={snapshot.stale_progress_jobs} highlight={snapshot.stale_progress_jobs > 0 ? "warn" : undefined} />
      <Card label="Healthy Workers" value={snapshot.healthy_workers} highlight={snapshot.healthy_workers > 0 ? "ok" : undefined} />
      <Card label="Running Workers" value={snapshot.running_workers} />
      <Card label="Oldest Queued" value={ageLabel} />
    </div>
  )
}
