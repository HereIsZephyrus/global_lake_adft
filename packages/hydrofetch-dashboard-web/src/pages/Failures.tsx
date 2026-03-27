import ReactECharts from 'echarts-for-react'
import ErrorBox from '../components/ErrorBox'
import Loading from '../components/Loading'
import Table from '../components/Table'
import { useFailures } from '../hooks/useApi'

const DETAIL_COLS = [
  { key: 'job_id', label: 'Job ID', width: 220 },
  { key: 'tile_id', label: 'Tile', width: 80 },
  { key: 'date_iso', label: '日期', width: 100 },
  { key: 'attempt', label: '尝试', width: 50 },
  { key: 'error_group', label: '错误摘要', width: 300 },
  { key: 'updated_at', label: '更新时间', width: 160 },
]

interface Props {
  projectId: string
}

export default function Failures({ projectId }: Props) {
  const { data, error } = useFailures(projectId)

  if (error) return <ErrorBox msg={`失败数据加载失败: ${error.message}`} />
  if (!data) return <Loading />

  const { items, summary } = data

  const barOption = summary.length ? {
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis' },
    grid: { left: 50, right: 20, top: 20, bottom: 40 },
    xAxis: {
      type: 'value',
      axisLabel: { color: '#9ca3af', fontSize: 11 },
      splitLine: { lineStyle: { color: '#1e2130' } },
    },
    yAxis: {
      type: 'category',
      data: summary.slice(0, 15).map(s => s.error_group.slice(0, 60)),
      axisLabel: { color: '#9ca3af', fontSize: 10, overflow: 'truncate', width: 300 },
    },
    series: [{
      type: 'bar',
      data: summary.slice(0, 15).map(s => s.count),
      itemStyle: { color: '#ef4444' },
    }],
  } : null

  return (
    <div>
      <h2 style={{ color: '#f3f4f6', marginBottom: 16, fontSize: 18 }}>失败诊断</h2>
      <div style={{ color: '#9ca3af', fontSize: 13, marginBottom: 16 }}>
        共 <strong style={{ color: '#ef4444' }}>{items.length}</strong> 个失败任务，{summary.length} 种错误类型
      </div>

      {barOption && (
        <div style={{ background: '#1e2130', borderRadius: 10, padding: 16, marginBottom: 20 }}>
          <div style={{ color: '#9ca3af', fontSize: 12, marginBottom: 8 }}>错误分布 (Top 15)</div>
          <ReactECharts
            option={barOption}
            style={{ height: Math.max(200, summary.slice(0, 15).length * 28) }}
            theme="dark"
          />
        </div>
      )}

      <div style={{ background: '#1e2130', borderRadius: 10, padding: 16 }}>
        <div style={{ color: '#9ca3af', fontSize: 12, marginBottom: 8 }}>失败任务明细</div>
        <Table columns={DETAIL_COLS} rows={items} maxHeight={450} />
      </div>
    </div>
  )
}
