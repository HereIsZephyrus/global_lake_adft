import ReactECharts from 'echarts-for-react'
import Card from '../components/Card'
import ErrorBox from '../components/ErrorBox'
import Loading from '../components/Loading'
import { useDbSize, useIngest, useOverview, useProjects } from '../hooks/useApi'
import { formatDateTimeShanghai } from '../utils/time'

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

const kpiStyle = (color: string): React.CSSProperties => ({
  background: '#161827',
  borderRadius: 8,
  padding: '10px 14px',
  borderLeft: `3px solid ${color}`,
  flex: '1 1 140px',
})

const previewCardWrapStyle: React.CSSProperties = {
  display: 'grid',
  gridTemplateColumns: 'repeat(2, minmax(0, 1fr))',
  gap: 10,
}

function ProjectPreview({ projectId, projectName }: { projectId: string; projectName: string }) {
  const { data: kpis, error } = useOverview(projectId)

  return (
    <div style={{
      background: '#161827',
      borderRadius: 10,
      padding: 18,
      border: '1px solid #2d3148',
      boxShadow: 'inset 0 1px 0 rgba(255,255,255,0.02)',
    }}>
      <div style={{
        color: '#f3f4f6',
        fontSize: 15,
        fontWeight: 700,
        marginBottom: 14,
      }}>
        {projectName}
      </div>

      {error ? <ErrorBox msg={`项目统计加载失败: ${error.message}`} /> :
        !kpis ? <Loading label="加载项目统计…" /> : (
          <div style={previewCardWrapStyle}>
            <Card title="总任务" value={kpis.total_jobs} color="#3b82f6" />
            <Card title="运行中" value={kpis.active_jobs} color="#f59e0b" />
            <Card title="已完成" value={kpis.completed_jobs} sub={`${kpis.completion_rate.toFixed(1)}%`} color="#10b981" />
            <Card title="失败" value={kpis.failed_jobs} sub={`${kpis.failure_rate.toFixed(1)}%`} color="#ef4444" />
          </div>
        )}
    </div>
  )
}

export default function GlobalView() {
  const { data: ingest, error: iErr } = useIngest()
  const { data: dbSize, error: dErr } = useDbSize()
  const { data: projects, error: pErr } = useProjects()
  const projectList = projects ?? []

  return (
    <div>
      <h2 style={{ color: '#f3f4f6', marginBottom: 16, fontSize: 18 }}>全局视图</h2>

      {/* Ingest */}
      <div style={{ background: '#1e2130', borderRadius: 10, padding: 18, marginBottom: 16 }}>
        <div style={{ color: '#9ca3af', fontSize: 12, marginBottom: 12, fontWeight: 600 }}>
          入库进度 · era5_forcing
        </div>
        {iErr ? <ErrorBox msg={iErr.message} /> :
          !ingest ? <Loading label="加载入库信息…" /> :
          !ingest.available ? (
            <div style={{ color: '#fbbf24', fontSize: 13 }}>{ingest.message}</div>
          ) : (
            <>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 10, marginBottom: 14 }}>
                <div style={kpiStyle('#3b82f6')}>
                  <div style={{ fontSize: 10, color: '#9ca3af' }}>总行数</div>
                  <div style={{ fontSize: 22, fontWeight: 700, color: '#f3f4f6' }}>{ingest.total_rows.toLocaleString()}</div>
                </div>
                <div style={kpiStyle('#10b981')}>
                  <div style={{ fontSize: 10, color: '#9ca3af' }}>最新日期</div>
                  <div style={{ fontSize: 16, fontWeight: 600, color: '#f3f4f6' }}>{ingest.max_date ?? '—'}</div>
                </div>
                <div style={kpiStyle('#a78bfa')}>
                  <div style={{ fontSize: 10, color: '#9ca3af' }}>起始日期</div>
                  <div style={{ fontSize: 16, fontWeight: 600, color: '#f3f4f6' }}>{ingest.min_date ?? '—'}</div>
                </div>
                <div style={kpiStyle('#f59e0b')}>
                  <div style={{ fontSize: 10, color: '#9ca3af' }}>最近入库</div>
                  <div style={{ fontSize: 12, fontWeight: 600, color: '#f3f4f6' }}>
                    {formatDateTimeShanghai(ingest.latest_ingested_at)}
                  </div>
                </div>
              </div>

              {ingest.daily_counts.length > 0 && (() => {
                const sorted = [...ingest.daily_counts].sort((a, b) => a.date < b.date ? -1 : 1)
                const option = {
                  backgroundColor: 'transparent',
                  tooltip: { trigger: 'axis' },
                  grid: { left: 44, right: 12, top: 10, bottom: 36 },
                  xAxis: {
                    type: 'category',
                    data: sorted.map(d => d.date.slice(5)),
                    axisLabel: { color: '#6b7280', fontSize: 9, rotate: 45, interval: Math.floor(sorted.length / 8) },
                    axisLine: { lineStyle: { color: '#2d3148' } },
                  },
                  yAxis: {
                    type: 'value',
                    axisLabel: { color: '#6b7280', fontSize: 9 },
                    splitLine: { lineStyle: { color: '#1e2130' } },
                  },
                  series: [{ type: 'bar', data: sorted.map(d => d.row_count), itemStyle: { color: '#10b981' } }],
                }
                return <ReactECharts option={option} style={{ height: 180 }} theme="dark" />
              })()}
            </>
          )}
      </div>

      {/* DB size */}
      <div style={{ background: '#1e2130', borderRadius: 10, padding: 18 }}>
        <div style={{ color: '#9ca3af', fontSize: 12, marginBottom: 12, fontWeight: 600 }}>
          数据库体积
        </div>
        {dErr ? <ErrorBox msg={dErr.message} /> :
          !dbSize ? <Loading label="加载 DB 体积…" /> :
          !dbSize.available ? (
            <div style={{ color: '#fbbf24', fontSize: 13 }}>{dbSize.message}</div>
          ) : (
            <>
              <div style={{
                display: 'flex', alignItems: 'center', gap: 10, marginBottom: 12,
                background: '#161827', borderRadius: 8, padding: '12px 16px',
                borderLeft: `3px solid ${byteColor(dbSize.db_size_bytes)}`,
              }}>
                <div style={{ flex: 1 }}>
                  <div style={{ fontSize: 10, color: '#9ca3af' }}>数据库总大小 · {dbSize.db_name}</div>
                  <div style={{ fontSize: 22, fontWeight: 700, color: byteColor(dbSize.db_size_bytes) }}>
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

      {/* Project previews */}
      <div style={{ background: '#1e2130', borderRadius: 10, padding: 18, marginTop: 16 }}>
        <div style={{ color: '#9ca3af', fontSize: 12, marginBottom: 12, fontWeight: 600 }}>
          项目任务预览
        </div>

        {pErr ? <ErrorBox msg={`项目列表加载失败: ${pErr.message}`} /> :
          !projects ? <Loading label="加载项目列表…" /> :
          projectList.length === 0 ? (
            <div style={{ color: '#6b7280', fontSize: 13 }}>暂无项目</div>
          ) : (
            <div style={{
              display: 'grid',
              gridTemplateColumns: 'repeat(4, minmax(0, 1fr))',
              gap: 16,
              alignItems: 'start',
            }}>
              {projectList.map((project) => (
                <ProjectPreview
                  key={project.project_id}
                  projectId={project.project_id}
                  projectName={project.project_name || project.project_id}
                />
              ))}
            </div>
          )}
      </div>
    </div>
  )
}
