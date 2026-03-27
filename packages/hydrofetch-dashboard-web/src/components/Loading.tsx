export default function Loading({ label = '加载中…' }: { label?: string }) {
  return (
    <div style={{ color: '#6b7280', fontSize: 13, padding: '24px 0' }}>{label}</div>
  )
}
