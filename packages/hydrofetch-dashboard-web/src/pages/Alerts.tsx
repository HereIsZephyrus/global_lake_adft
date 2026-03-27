import ErrorBox from '../components/ErrorBox'
import Loading from '../components/Loading'
import Table from '../components/Table'
import { useAlerts, useLogs } from '../hooks/useApi'

const STALLED_COLS = [
  { key: 'job_id', label: 'Job ID', width: 200 },
  { key: 'state', label: '状态', width: 90 },
  { key: 'tile_id', label: 'Tile', width: 70 },
  { key: 'date_iso', label: '日期', width: 100 },
  { key: 'updated_age_hours', label: '停滞时长 (h)', width: 110 },
  { key: 'updated_at', label: '最后更新', width: 160 },
]

const RETRY_COLS = [
  { key: 'job_id', label: 'Job ID', width: 200 },
  { key: 'state', label: '状态', width: 90 },
  { key: 'tile_id', label: 'Tile', width: 70 },
  { key: 'date_iso', label: '日期', width: 100 },
  { key: 'attempt', label: '尝试次数', width: 80 },
  { key: 'updated_at', label: '最后更新', width: 160 },
]

const LOG_COLS = [
  { key: 'timestamp', label: '时间', width: 160 },
  { key: 'level', label: '级别', width: 70 },
  { key: 'logger', label: 'Logger', width: 160 },
  { key: 'message', label: '消息', width: 400 },
]

function Section({ title, count, color, children }: { title: string; count: number; color: string; children: React.ReactNode }) {
  return (
    <div style={{ background: '#1e2130', borderRadius: 10, padding: 16, marginBottom: 20 }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10 }}>
        <div style={{ color, fontWeight: 700, fontSize: 13 }}>{title}</div>
        <div style={{ background: color + '33', color, borderRadius: 12, padding: '1px 8px', fontSize: 11, fontWeight: 600 }}>{count}</div>
      </div>
      {children}
    </div>
  )
}

interface Props {
  projectId: string
}

export default function Alerts({ projectId }: Props) {
  const { data: alerts, error: aErr } = useAlerts(projectId)
  const { data: logs, error: lErr } = useLogs(projectId)

  if (aErr) return <ErrorBox msg={`告警加载失败: ${aErr.message}`} />

  return (
    <div>
      <h2 style={{ color: '#f3f4f6', marginBottom: 16, fontSize: 18 }}>运行告警</h2>

      {!alerts ? <Loading /> : (
        <>
          <Section title="停滞任务 (>1h 未更新)" count={alerts.stalled.length} color="#f97316">
            <Table columns={STALLED_COLS} rows={alerts.stalled} maxHeight={280} emptyText="无停滞任务" />
          </Section>

          <Section title="高重试任务 (≥2次)" count={alerts.high_retry.length} color="#f59e0b">
            <Table columns={RETRY_COLS} rows={alerts.high_retry} maxHeight={280} emptyText="无高重试任务" />
          </Section>
        </>
      )}

      {lErr ? <ErrorBox msg={`日志加载失败: ${lErr.message}`} /> :
        !logs ? <Loading label="加载日志…" /> : (
          <Section title="日志 ERROR" count={logs.errors.length} color="#ef4444">
            <Table columns={LOG_COLS} rows={logs.errors} maxHeight={320} emptyText="无 ERROR 日志" />
          </Section>
        )}
    </div>
  )
}
