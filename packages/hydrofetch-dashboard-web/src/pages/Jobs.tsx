import { useState, useCallback } from 'react'
import useSWR from 'swr'
import ErrorBox from '../components/ErrorBox'
import Loading from '../components/Loading'
import Table from '../components/Table'
import { api } from '../api/client'
import { formatStateLabel } from '../utils/stateLabels'

const STATE_ORDER = ['', 'hold', 'export', 'download', 'sample', 'write', 'cleanup', 'completed', 'failed']

const COLS = [
  { key: 'state', label: '状态', width: 90 },
  { key: 'tile_id', label: 'Tile', width: 70 },
  { key: 'date_iso', label: '日期', width: 100 },
  { key: 'attempt', label: '次数', width: 50 },
  { key: 'job_id', label: 'Job ID', width: 200 },
  { key: 'task_id', label: 'Task ID', width: 160 },
  { key: 'updated_at', label: '更新时间', width: 160 },
  { key: 'last_error', label: '最后错误', width: 260 },
]

const inputStyle = {
  background: '#1a1d2e', border: '1px solid #2d3148', color: '#d1d5db',
  borderRadius: 6, padding: '5px 10px', fontSize: 12,
}
const btnStyle = {
  ...inputStyle,
  cursor: 'pointer', background: '#3b4168',
  padding: '5px 14px', marginLeft: 8,
}

interface Props {
  projectId: string
}

export default function Jobs({ projectId }: Props) {
  const [state, setState] = useState('')
  const [tileId, setTileId] = useState('')
  const [minAttempt, setMinAttempt] = useState(0)
  const [offset, setOffset] = useState(0)
  const limit = 200

  const [filter, setFilter] = useState({ state: '', tileId: '', minAttempt: 0 })

  const { data, error, isLoading } = useSWR(
    ['jobs', projectId, filter, offset],
    () => api.jobs(projectId, {
      state: filter.state || undefined,
      tile_id: filter.tileId || undefined,
      min_attempt: filter.minAttempt,
      limit,
      offset,
    }),
    { revalidateOnFocus: false },
  )

  const apply = useCallback(() => {
    setFilter({ state, tileId, minAttempt })
    setOffset(0)
  }, [state, tileId, minAttempt])

  return (
    <div>
      <h2 style={{ color: '#f3f4f6', marginBottom: 16, fontSize: 18 }}>任务明细</h2>

      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginBottom: 16, alignItems: 'center' }}>
        <select style={inputStyle} value={state} onChange={e => setState(e.target.value)}>
          {STATE_ORDER.map(s => <option key={s} value={s}>{s ? formatStateLabel(s) : '全部状态'}</option>)}
        </select>
        <input style={inputStyle} placeholder="Tile ID" value={tileId} onChange={e => setTileId(e.target.value)} />
        <input
          style={{ ...inputStyle, width: 80 }}
          type="number" min={0} placeholder="最少尝试"
          value={minAttempt}
          onChange={e => setMinAttempt(Number(e.target.value))}
        />
        <button style={btnStyle} onClick={apply}>查询</button>
      </div>

      {error ? <ErrorBox msg={`任务加载失败: ${error.message}`} /> :
        isLoading ? <Loading /> : data ? (
          <>
            <div style={{ fontSize: 12, color: '#6b7280', marginBottom: 8 }}>
              共 {data.total} 条，当前 {offset + 1}–{Math.min(offset + limit, data.total)}
            </div>
            <Table columns={COLS} rows={data.items} maxHeight={520} />
            <div style={{ marginTop: 10, display: 'flex', gap: 8 }}>
              <button style={btnStyle} disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - limit))}>上一页</button>
              <button style={btnStyle} disabled={offset + limit >= data.total} onClick={() => setOffset(offset + limit)}>下一页</button>
            </div>
          </>
        ) : null}
    </div>
  )
}
