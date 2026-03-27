import ReactECharts from 'echarts-for-react'
import Card from '../components/Card'
import ErrorBox from '../components/ErrorBox'
import Loading from '../components/Loading'
import { useOverview, useStates, useTimeline, useIngest, useDbSize } from '../hooks/useApi'
import { STATE_COLORS } from '../utils/colors'

const CHART_STATES = ['export','download','cleanup','sample','write','failed']

// ── DB SIZE helpers ────────────────────────────────────────────────────────────
const GiB = 1024 ** 3
const TOTAL_CAPACITY_GIB = 800
const WARN_RATIO = 0.85
const CRIT_RATIO = 0.95

function byteColor(bytes: number) {
  if (bytes >= TOTAL_CAPACITY_GIB * GiB * CRIT_RATIO) return '#ef4444'
  if (bytes >= TOTAL_CAPACITY_GIB * GiB * WARN_RATIO) return '#f59e0b'
  return '#10b981'
}

function prettyGiB(bytes: number) {
  return (bytes / GiB).toFixed(1) + ' GiB'
}

function usagePercent(bytes: number) {
  return (bytes / (TOTAL_CAPACITY_GIB * GiB) * 100).toFixed(1)
}

function formatLocalTimeLabel(iso: string) {
  return new Date(iso).toLocaleTimeString('zh-CN', {
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  })
}

// ── Right panel: Ingest + DB size ─────────────────────────────────────────────
function IngestPanel() {
  const { data: ingest, error: iErr } = useIngest()
  const { data: dbSize, error: dErr } = useDbSize()

  const kpiStyle = (color: string) => ({
    background: '#161827',
    borderRadius: 8,
    padding: '10px 14px',
    borderLeft: `3px solid ${color}`,
    flex: '1 1 140px',
  })

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 14 }}>

      {/* Ingest KPIs */}
      <div style={{ background: '#1e2130', borderRadius: 10, padding: 14 }}>
        <div style={{ color: '#9ca3af', fontSize: 11, marginBottom: 10, fontWeight: 600 }}>
          入库进度 · era5_forcing
        </div>
        {iErr ? <ErrorBox msg={iErr.message} /> :
          !ingest ? <Loading label="加载入库信息…" /> :
          !ingest.available ? (
            <div style={{ color: '#fbbf24', fontSize: 12 }}>{ingest.message}</div>
          ) : (
            <>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginBottom: 10 }}>
                <div style={kpiStyle('#3b82f6')}>
                  <div style={{ fontSize: 10, color: '#9ca3af' }}>总行数</div>
                  <div style={{ fontSize: 17, fontWeight: 700, color: '#f3f4f6' }}>{ingest.total_rows.toLocaleString()}</div>
                </div>
                <div style={kpiStyle('#10b981')}>
                  <div style={{ fontSize: 10, color: '#9ca3af' }}>最新日期</div>
                  <div style={{ fontSize: 14, fontWeight: 600, color: '#f3f4f6' }}>{ingest.max_date ?? '—'}</div>
                </div>
                <div style={kpiStyle('#a78bfa')}>
                  <div style={{ fontSize: 10, color: '#9ca3af' }}>最近入库</div>
                  <div style={{ fontSize: 12, fontWeight: 600, color: '#f3f4f6' }}>
                    {ingest.latest_ingested_at ? ingest.latest_ingested_at.slice(0, 19) : '—'}
                  </div>
                </div>
              </div>

              {/* Daily bar (compact) */}
              {ingest.daily_counts.length > 0 && (() => {
                const sorted = [...ingest.daily_counts].sort((a, b) => a.date < b.date ? -1 : 1)
                const option = {
                  backgroundColor: 'transparent',
                  tooltip: { trigger: 'axis' },
                  grid: { left: 36, right: 8, top: 8, bottom: 32 },
                  xAxis: {
                    type: 'category',
                    data: sorted.map(d => d.date.slice(5)),
                    axisLabel: { color: '#6b7280', fontSize: 9, rotate: 45, interval: Math.floor(sorted.length / 6) },
                    axisLine: { lineStyle: { color: '#2d3148' } },
                  },
                  yAxis: { type: 'value', axisLabel: { color: '#6b7280', fontSize: 9 }, splitLine: { lineStyle: { color: '#1e2130' } } },
                  series: [{ type: 'bar', data: sorted.map(d => d.row_count), itemStyle: { color: '#10b981' } }],
                }
                return <ReactECharts option={option} style={{ height: 130 }} theme="dark" />
              })()}
            </>
          )}
      </div>

      {/* DB size */}
      <div style={{ background: '#1e2130', borderRadius: 10, padding: 14 }}>
        <div style={{ color: '#9ca3af', fontSize: 11, marginBottom: 10, fontWeight: 600 }}>
          数据库体积
        </div>
        {dErr ? <ErrorBox msg={dErr.message} /> :
          !dbSize ? <Loading label="加载 DB 体积…" /> :
          !dbSize.available ? (
            <div style={{ color: '#fbbf24', fontSize: 12 }}>{dbSize.message}</div>
          ) : (
            <>
              {/* DB total */}
              <div style={{
                display: 'flex', alignItems: 'center', gap: 10, marginBottom: 10,
                background: '#161827', borderRadius: 8, padding: '10px 14px',
                borderLeft: `3px solid ${byteColor(dbSize.db_size_bytes)}`,
              }}>
                <div style={{ flex: 1 }}>
                  <div style={{ fontSize: 10, color: '#9ca3af' }}>数据库总大小 · {dbSize.db_name}</div>
                  <div style={{ fontSize: 20, fontWeight: 700, color: byteColor(dbSize.db_size_bytes) }}>
                    {prettyGiB(dbSize.db_size_bytes)} / {TOTAL_CAPACITY_GIB} GiB
                  </div>
                  <div style={{ fontSize: 10, color: '#6b7280' }}>
                    {dbSize.db_size_pretty} · 已用 {usagePercent(dbSize.db_size_bytes)}%
                  </div>
                </div>
                {dbSize.db_size_bytes >= TOTAL_CAPACITY_GIB * GiB * WARN_RATIO && (
                  <div style={{
                    background: dbSize.db_size_bytes >= TOTAL_CAPACITY_GIB * GiB * CRIT_RATIO ? '#3b1f1f' : '#2a1f0e',
                    color: byteColor(dbSize.db_size_bytes),
                    borderRadius: 6, padding: '4px 8px', fontSize: 11, fontWeight: 600,
                  }}>
                    {dbSize.db_size_bytes >= TOTAL_CAPACITY_GIB * GiB * CRIT_RATIO ? '⚠ 磁盘告警' : '△ 注意空间'}
                  </div>
                )}
              </div>

              {/* Per-table */}
              {dbSize.tables.map(t => (
                <div key={t.table_name} style={{
                  background: '#161827', borderRadius: 8, padding: '8px 14px',
                  marginBottom: 6, borderLeft: `3px solid ${byteColor(t.total_bytes)}`,
                }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <div style={{ fontSize: 11, color: '#d1d5db', fontWeight: 600 }}>{t.table_name}</div>
                    <div style={{ fontSize: 13, fontWeight: 700, color: byteColor(t.total_bytes) }}>{t.total_pretty}</div>
                  </div>
                  <div style={{ fontSize: 10, color: '#6b7280', marginTop: 2 }}>
                    数据 {t.data_pretty} &nbsp;·&nbsp; 索引 {t.index_pretty}
                  </div>
                  {/* Size bar */}
                  {dbSize.db_size_bytes > 0 && (
                    <div style={{ marginTop: 4, height: 3, background: '#2d3148', borderRadius: 2 }}>
                      <div style={{
                        height: '100%', borderRadius: 2,
                        background: byteColor(t.total_bytes),
                        width: `${Math.min(100, t.total_bytes / dbSize.db_size_bytes * 100).toFixed(1)}%`,
                      }} />
                    </div>
                  )}
                </div>
              ))}
            </>
          )}
      </div>
    </div>
  )
}

// ── Left panel: job overview ───────────────────────────────────────────────────
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
      data: CHART_STATES,
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
    legend: { data: CHART_STATES, textStyle: { color: '#9ca3af', fontSize: 10 }, top: 0 },
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

      {/* Two-column layout */}
      <div style={{ display: 'flex', gap: 20, alignItems: 'flex-start' }}>

        {/* Left: job metrics */}
        <div style={{ flex: '1 1 0', minWidth: 0 }}>
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

        {/* Right: ingest + db size */}
        <div style={{ width: 340, flexShrink: 0 }}>
          <IngestPanel />
        </div>
      </div>
    </div>
  )
}
