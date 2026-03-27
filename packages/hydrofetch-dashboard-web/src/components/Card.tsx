interface Props {
  title: string
  value: string | number
  sub?: string
  color?: string
}

export default function Card({ title, value, sub, color = '#3b82f6' }: Props) {
  return (
    <div style={{
      background: '#1e2130',
      borderRadius: 10,
      padding: '16px 20px',
      borderLeft: `4px solid ${color}`,
      minWidth: 150,
    }}>
      <div style={{ fontSize: 12, color: '#9ca3af', marginBottom: 4 }}>{title}</div>
      <div style={{ fontSize: 28, fontWeight: 700, color: '#f3f4f6' }}>{value}</div>
      {sub && <div style={{ fontSize: 11, color: '#6b7280', marginTop: 2 }}>{sub}</div>}
    </div>
  )
}
