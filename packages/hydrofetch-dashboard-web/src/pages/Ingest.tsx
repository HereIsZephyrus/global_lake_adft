import ReactECharts from 'echarts-for-react'
import ErrorBox from '../components/ErrorBox'
import Loading from '../components/Loading'
import Table from '../components/Table'
import { useIngest } from '../hooks/useApi'

const RECENT_COLS = [
  { key: 'hylak_id', label: 'HyLak ID', width: 120 },
  { key: 'date', label: '日期', width: 100 },
  { key: 'ingested_at', label: '入库时间', width: 180 },
]

export default function Ingest() {
  const { data, error } = useIngest()

  if (error) return <ErrorBox msg={`入库数据加载失败: ${error.message}`} />
  if (!data) return <Loading />

  if (!data.available) {
    return (
      <div>
        <h2 style={{ color: '#f3f4f6', marginBottom: 12, fontSize: 18 }}>入库进度</h2>
        <div style={{ background: '#2a1f1f', border: '1px solid #78350f', borderRadius: 8, padding: 16, color: '#fbbf24', fontSize: 13 }}>
          {data.message}
        </div>
      </div>
    )
  }

  const sortedDaily = [...data.daily_counts].sort((a, b) => a.date < b.date ? -1 : 1)
  const dailyOption = sortedDaily.length ? {
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis' },
    grid: { left: 60, right: 20, top: 20, bottom: 40 },
    xAxis: {
      type: 'category',
      data: sortedDaily.map(d => d.date),
      axisLabel: { color: '#9ca3af', fontSize: 10, rotate: 45 },
      axisLine: { lineStyle: { color: '#2d3148' } },
    },
    yAxis: { type: 'value', axisLabel: { color: '#9ca3af', fontSize: 11 }, splitLine: { lineStyle: { color: '#1e2130' } } },
    series: [{
      type: 'bar',
      data: sortedDaily.map(d => d.row_count),
      itemStyle: { color: '#10b981' },
    }],
  } : null

  return (
    <div>
      <h2 style={{ color: '#f3f4f6', marginBottom: 16, fontSize: 18 }}>入库进度</h2>

      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 12, marginBottom: 20 }}>
        {[
          { label: '总行数', value: data.total_rows.toLocaleString(), color: '#3b82f6' },
          { label: '最早日期', value: data.min_date ?? '—', color: '#6b7280' },
          { label: '最新日期', value: data.max_date ?? '—', color: '#10b981' },
          { label: '最近入库', value: data.latest_ingested_at ? data.latest_ingested_at.slice(0, 19) : '—', color: '#a78bfa' },
        ].map(item => (
          <div key={item.label} style={{
            background: '#1e2130', borderRadius: 10, padding: '14px 18px',
            borderLeft: `4px solid ${item.color}`,
          }}>
            <div style={{ fontSize: 11, color: '#9ca3af' }}>{item.label}</div>
            <div style={{ fontSize: 20, fontWeight: 700, color: '#f3f4f6', marginTop: 2 }}>{item.value}</div>
          </div>
        ))}
      </div>

      {dailyOption && (
        <div style={{ background: '#1e2130', borderRadius: 10, padding: 16, marginBottom: 20 }}>
          <div style={{ color: '#9ca3af', fontSize: 12, marginBottom: 8 }}>近 30 天每日入库量</div>
          <ReactECharts option={dailyOption} style={{ height: 240 }} theme="dark" />
        </div>
      )}

      <div style={{ background: '#1e2130', borderRadius: 10, padding: 16 }}>
        <div style={{ color: '#9ca3af', fontSize: 12, marginBottom: 8 }}>最近入库记录</div>
        <Table columns={RECENT_COLS} rows={data.recent_rows} maxHeight={300} />
      </div>
    </div>
  )
}
