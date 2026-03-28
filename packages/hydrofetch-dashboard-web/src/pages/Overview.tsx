import { useState } from 'react'
import ReactECharts from 'echarts-for-react'
import Card from '../components/Card'
import ErrorBox from '../components/ErrorBox'
import Loading from '../components/Loading'
import { useOverview, useStates, useTimeline } from '../hooks/useApi'
import { api } from '../api/client'
import { STATE_COLORS } from '../utils/colors'
import { formatStateLabel } from '../utils/stateLabels'

const CHART_STATES = ['export', 'download', 'sample', 'write', 'cleanup', 'failed']

function formatLocalTimeLabel(iso: string) {
  return new Date(iso).toLocaleTimeString('zh-CN', {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  })
}

const STATUS_COLOR: Record<string, string> = {
  running: '#10b981',
  finished: '#3b82f6',
  stopped: '#6b7280',
}

const STATUS_LABEL: Record<string, string> = {
  running: '运行中',
  finished: '已完成',
  stopped: '已停止',
}

interface Props {
  projectId: string
}

export default function Overview({ projectId }: Props) {
  const { data: kpis, error: kpiErr, mutate: mutateKpis } = useOverview(projectId)
  const { data: states, error: stErr } = useStates(projectId)
  const { data: timeline, error: tlErr } = useTimeline(projectId, 6)

  const [ctrlLoading, setCtrlLoading] = useState(false)
  const [ctrlError, setCtrlError] = useState<string | null>(null)

  const processStatus = kpis?.process_status ?? 'stopped'
  const isRunning = processStatus === 'running'

  const handleStart = async () => {
    setCtrlError(null)
    setCtrlLoading(true)
    try {
      await api.startProject(projectId)
      await mutateKpis()
    } catch (e: unknown) {
      setCtrlError(e instanceof Error ? e.message : String(e))
    } finally {
      setCtrlLoading(false)
    }
  }

  const handleStop = async () => {
    setCtrlError(null)
    setCtrlLoading(true)
    try {
      await api.stopProject(projectId)
      await mutateKpis()
    } catch (e: unknown) {
      setCtrlError(e instanceof Error ? e.message : String(e))
    } finally {
      setCtrlLoading(false)
    }
  }

  if (kpiErr) return <ErrorBox msg={`Overview 加载失败: ${kpiErr.message}`} />

  const stateBarOption = states ? {
    backgroundColor: 'transparent',
    tooltip: { trigger: 'axis' },
    grid: { left: 50, right: 20, top: 20, bottom: 40 },
    xAxis: {
      type: 'category',
      data: CHART_STATES.map(formatStateLabel),
      axisLabel: { color: '#9ca3af', fontSize: 11, rotate: 30 },
      axisLine: { lineStyle: { color: '#2d3148' } },
    },
    yAxis: { type: 'value', axisLabel: { color: '#9ca3af', fontSize: 11 }, splitLine: { lineStyle: { color: '#1e2130' } } },
    series: [{
      type: 'bar',
      data: CHART_STATES.map(s => {
        const item = states.find(x => x.state === s)
        return { value: item?.count ?? 0, itemStyle: { color: STATE_COLORS[s] ?? '#60a5fa' } }
      }),
    }],
  } : null

  const hours = Array.from(new Set((timeline ?? []).map(p => p.hour))).sort()
  const tlOption = timeline && hours.length ? {
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'axis',
      formatter: (params: Array<{ axisValue: string; seriesName: string; value: number; color: string }>) => {
        if (!params.length) return ''
        const lines = [`<div>${formatLocalTimeLabel(params[0].axisValue)}</div>`]
        for (const item of params) {
          lines.push(`${item.color} ${item.seriesName}: ${item.value}`)
        }
        return lines.join('<br/>')
      },
    },
    legend: { data: CHART_STATES.map(formatStateLabel), textStyle: { color: '#9ca3af', fontSize: 10 }, top: 0 },
    grid: { left: 50, right: 20, top: 40, bottom: 40 },
    xAxis: {
      type: 'category',
      data: hours,
      axisLabel: {
        color: '#9ca3af',
        fontSize: 10,
        interval: 0,
        formatter: (value: string) => formatLocalTimeLabel(value),
      },
      axisLine: { lineStyle: { color: '#2d3148' } },
    },
    yAxis: { type: 'value', axisLabel: { color: '#9ca3af', fontSize: 11 }, splitLine: { lineStyle: { color: '#1e2130' } } },
    series: CHART_STATES.map(s => ({
      name: formatStateLabel(s),
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
      {/* Header row: title + process control */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 14, marginBottom: 16, flexWrap: 'wrap' }}>
        <h2 style={{ color: '#f3f4f6', fontSize: 18, margin: 0 }}>
          {kpis?.project_name ?? '总览'}
        </h2>

        {/* Process status badge */}
        <div style={{
          display: 'flex', alignItems: 'center', gap: 6,
          background: '#1e2130', borderRadius: 20,
          padding: '4px 12px', fontSize: 12,
        }}>
          <span style={{
            width: 7, height: 7, borderRadius: '50%',
            background: STATUS_COLOR[processStatus] ?? '#6b7280',
            boxShadow: isRunning ? `0 0 5px ${STATUS_COLOR.running}` : undefined,
          }} />
          <span style={{ color: STATUS_COLOR[processStatus] ?? '#6b7280', fontWeight: 600 }}>
            {STATUS_LABEL[processStatus] ?? processStatus}
          </span>
        </div>

        {/* Control buttons */}
        {!isRunning ? (
          <button
            onClick={handleStart}
            disabled={ctrlLoading}
            style={{
              background: '#064e3b', border: '1px solid #10b981',
              color: '#10b981', borderRadius: 7, padding: '5px 16px',
              fontSize: 12, fontWeight: 600, cursor: ctrlLoading ? 'not-allowed' : 'pointer',
            }}
          >
            {ctrlLoading ? '启动中…' : '▶ 启动'}
          </button>
        ) : (
          <button
            onClick={handleStop}
            disabled={ctrlLoading}
            style={{
              background: '#3b1f1f', border: '1px solid #ef4444',
              color: '#ef4444', borderRadius: 7, padding: '5px 16px',
              fontSize: 12, fontWeight: 600, cursor: ctrlLoading ? 'not-allowed' : 'pointer',
            }}
          >
            {ctrlLoading ? '停止中…' : '■ 停止'}
          </button>
        )}

        {ctrlError && (
          <div style={{ color: '#fca5a5', fontSize: 12 }}>{ctrlError}</div>
        )}
      </div>

      {!kpis ? <Loading /> : (
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 10, marginBottom: 18 }}>
          <Card title="总任务" value={kpis.total_jobs} color="#3b82f6" />
          <Card title="运行中" value={kpis.active_jobs} color="#f59e0b" />
          <Card title="已完成" value={kpis.completed_jobs} sub={`${kpis.completion_rate.toFixed(1)}%`} color="#10b981" />
          <Card title="失败" value={kpis.failed_jobs} sub={`${kpis.failure_rate.toFixed(1)}%`} color="#ef4444" />
          <Card title="停滞 (>1h)" value={kpis.stalled_jobs} color="#f97316" />
        </div>
      )}

      {stErr ? <ErrorBox msg={`状态分布加载失败: ${stErr.message}`} /> :
        !states ? <Loading label="加载状态分布…" /> : (
          <div style={{ background: '#1e2130', borderRadius: 10, padding: 16, marginBottom: 16 }}>
            <div style={{ color: '#9ca3af', fontSize: 12, marginBottom: 8 }}>状态分布</div>
            <ReactECharts option={stateBarOption!} style={{ height: 200 }} theme="dark" />
          </div>
        )}

      {tlErr ? <ErrorBox msg={`趋势加载失败: ${tlErr.message}`} /> :
        !timeline ? <Loading label="加载趋势…" /> : hours.length === 0 ? (
          <div style={{ color: '#6b7280', fontSize: 13 }}>近 6 小时无数据</div>
        ) : (
          <div style={{ background: '#1e2130', borderRadius: 10, padding: 16 }}>
            <div style={{ color: '#9ca3af', fontSize: 12, marginBottom: 8 }}>近 6 小时趋势</div>
            <ReactECharts option={tlOption!} style={{ height: 220 }} theme="dark" />
          </div>
        )}

      {kpis && (
        <div style={{ marginTop: 12, fontSize: 11, color: '#4b5563' }}>
          JOB_DIR: {kpis.job_dir}
        </div>
      )}
    </div>
  )
}
