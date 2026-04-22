import { clsx } from "clsx"

const STATUS_COLORS: Record<string, string> = {
  queued: "bg-gray-700 text-gray-200",
  processing: "bg-blue-900 text-blue-200",
  succeeded: "bg-green-900 text-green-200",
  failed: "bg-red-900 text-red-200",
  running: "bg-green-900 text-green-200",
  stopped: "bg-gray-700 text-gray-400",
  dead: "bg-red-900 text-red-400",
}

export function StatusBadge({ status }: { status: string }) {
  return (
    <span
      className={clsx(
        "inline-block rounded px-2 py-0.5 text-xs font-mono font-semibold",
        STATUS_COLORS[status] ?? "bg-gray-700 text-gray-200",
      )}
    >
      {status}
    </span>
  )
}
