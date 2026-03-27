import ReactECharts from 'echarts-for-react'
import Card from '../components/Card'
import ErrorBox from '../components/ErrorBox'
import Loading from '../components/Loading'
import { useOverview, useStates, useTimeline } from '../hooks/useApi'
import { STATE_COLORS } from '../utils/colors'

const STATE_ORDER = ['hold','export','download','cleanup','sample','write','completed','failed']

export default function Overview() {
  const { data: kpis, error: kpiErr } = useOverview()
  const { data: states, error: stErr } = useStates()
  const { data: timeline, error: tlErr } = useTimeline(6)

  if (kpiErr) return <ErrorBox msg={`Overview 加载失败: ${kpiErr.message}`} />

  const stateBarOption = states ? {
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis' },
    grid: { left: 50, right: 20, top: 20, bottom: 40 },
    xAxis: {
      type: 'category',
      data: STATE_ORDER,
      axisLabel: { color: '#9ca3af', fontSize: 11, rotate: 30 },
      axisLine: { lineStyle: { color: '#2d3148' } },
    },
    yAxis: { type: 'value', axisLabel: { color: '#9ca3af', fontSize: 11 }, splitLine: { lineStyle: { color: '#1e2130' } } },
    series: [{
      type: 'bar',
      data: STATE_ORDER.map(s => {
        const item = states.find(x => x.state === s)
        return { value: item?.count ?? 0, itemStyle: { color: STATE_COLORS[s] ?? '#60a5fa' } }
      }),
    }],
  } : null

  const hours = Array.from(new Set((timeline ?? []).map(p => p.hour))).sort()
  const tlOption = timeline && hours.length ? {
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis' },
    legend: { data: STATE_ORDER, textStyle: { color: '#9ca3af', fontSize: 10 }, top: 0 },
    grid: { left: 50, right: 20, top: 40, bottom: 40 },
    xAxis: {
      type: 'category',
      data: hours.map(h => h.slice(11, 16)),
      axisLabel: { color: '#9ca3af', fontSize: 10 },
      axisLine: { lineStyle: { color: '#2d3148' } },
    },
    yAxis: { type: 'value', axisLabel: { color: '#9ca3af', fontSize: 11 }, splitLine: { lineStyle: { color: '#1e2130' } } },
    series: STATE_ORDER.map(s => ({
      name: s,
      type: 'line',
      smooth: true,
      symbol: 'none',
      lineStyle: { color: STATE_COLORS[s] ?? '#60a5fa', width: 2 },
      data: hours.map(h => {
        const pt = timeline.find(p => p.hour === h && p.state === s)
        return pt?.count ?? 0
      }),
    })),
  } : null

  return (
    <div>
      <h2 style={{ color: '#f3f4f6', marginBottom: 16, fontSize: 18 }}>总览</h2>

      {!kpis ? <Loading /> : (
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 12, marginBottom: 24 }}>
          <Card title="总任务" value={kpis.total_jobs} color="#3b82f6" />
          <Card title="运行中" value={kpis.active_jobs} color="#f59e0b" />
          <Card title="已完成" value={kpis.completed_jobs} sub={`${kpis.completion_rate.toFixed(1)}%`} color="#10b981" />
          <Card title="失败" value={kpis.failed_jobs} sub={`${kpis.failure_rate.toFixed(1)}%`} color="#ef4444" />
          <Card title="停滞 (>1h)" value={kpis.stalled_jobs} color="#f97316" />
        </div>
      )}

      {stErr ? <ErrorBox msg={`状态分布加载失败: ${stErr.message}`} /> :
        !states ? <Loading label="加载状态分布…" /> : (
          <div style={{ background: '#1e2130', borderRadius: 10, padding: 16, marginBottom: 20 }}>
            <div style={{ color: '#9ca3af', fontSize: 12, marginBottom: 8 }}>状态分布</div>
            <ReactECharts option={stateBarOption!} style={{ height: 220 }} theme="dark" />
          </div>
        )}

      {tlErr ? <ErrorBox msg={`趋势加载失败: ${tlErr.message}`} /> :
        !timeline ? <Loading label="加载趋势…" /> : hours.length === 0 ? (
          <div style={{ color: '#6b7280', fontSize: 13 }}>近 6 小时无数据</div>
        ) : (
          <div style={{ background: '#1e2130', borderRadius: 10, padding: 16 }}>
            <div style={{ color: '#9ca3af', fontSize: 12, marginBottom: 8 }}>近 6 小时趋势</div>
            <ReactECharts option={tlOption!} style={{ height: 240 }} theme="dark" />
          </div>
        )}

      {kpis && (
        <div style={{ marginTop: 16, fontSize: 11, color: '#4b5563' }}>
          JOB_DIR: {kpis.job_dir} &nbsp;|&nbsp; DB_TABLE: {kpis.db_table}
        </div>
      )}
    </div>
  )
}
