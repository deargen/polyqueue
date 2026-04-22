interface RefreshControlProps {
  paused: boolean
  onToggle: () => void
}

export function RefreshControl({ paused, onToggle }: RefreshControlProps) {
  return (
    <button
      onClick={onToggle}
      className="rounded border border-gray-700 px-3 py-1 text-sm text-gray-400 hover:text-gray-100 transition-colors"
    >
      {paused ? "▶ Resume" : "⏸ Pause"} auto-refresh
    </button>
  )
}
