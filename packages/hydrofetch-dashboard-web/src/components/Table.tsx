interface Props {
  columns: { key: string; label: string; width?: number }[]
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  rows: Record<string, any>[]
  emptyText?: string
  maxHeight?: number
}

export default function Table({ columns, rows, emptyText = '暂无数据', maxHeight = 400 }: Props) {
  if (!rows.length) return <div style={{ color: '#6b7280', fontSize: 13, padding: 12 }}>{emptyText}</div>
  return (
    <div style={{ overflowX: 'auto', overflowY: 'auto', maxHeight, borderRadius: 8, border: '1px solid #2d3148' }}>
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 12 }}>
        <thead>
          <tr style={{ background: '#1a1d2e', position: 'sticky', top: 0 }}>
            {columns.map(c => (
              <th key={c.key} style={{
                padding: '8px 12px', textAlign: 'left', color: '#9ca3af',
                fontWeight: 600, whiteSpace: 'nowrap',
                width: c.width ?? 'auto',
              }}>{c.label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} style={{ borderTop: '1px solid #1e2130', background: i % 2 ? '#161827' : 'transparent' }}>
              {columns.map(c => (
                <td key={c.key} style={{
                  padding: '6px 12px', color: '#d1d5db',
                  whiteSpace: 'nowrap', overflow: 'hidden',
                  maxWidth: c.width ?? 200, textOverflow: 'ellipsis',
                }} title={String(row[c.key] ?? '')}>
                  {row[c.key] == null ? <span style={{ color: '#4b5563' }}>—</span> : String(row[c.key])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
