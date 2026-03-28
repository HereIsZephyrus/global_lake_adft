export const STATE_LABELS: Record<string, string> = {
  download: 'downloading',
}

export function formatStateLabel(state: string) {
  return STATE_LABELS[state] ?? state
}
