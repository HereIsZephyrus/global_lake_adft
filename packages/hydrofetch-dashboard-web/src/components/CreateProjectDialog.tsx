import { useState } from 'react'
import { api, type CreateProjectBody } from '../api/client'

interface Props {
  onClose: () => void
  onCreated: (projectId: string) => void
}

const inputStyle: React.CSSProperties = {
  background: '#1a1d2e',
  border: '1px solid #2d3148',
  color: '#d1d5db',
  borderRadius: 6,
  padding: '7px 10px',
  fontSize: 13,
  width: '100%',
  boxSizing: 'border-box',
  outline: 'none',
}

const labelStyle: React.CSSProperties = {
  display: 'block',
  fontSize: 11,
  color: '#9ca3af',
  marginBottom: 5,
  fontWeight: 600,
}

const fieldStyle: React.CSSProperties = {
  marginBottom: 14,
}

export default function CreateProjectDialog({ onClose, onCreated }: Props) {
  const [form, setForm] = useState<CreateProjectBody>({
    project_name: '',
    gee_project: '',
    credentials_file: '',
    start_date: '',
    end_date: '',
    max_concurrent: 5,
  })
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const set = (key: keyof CreateProjectBody, value: string | number) =>
    setForm(f => ({ ...f, [key]: value }))

  const submit = async (andStart: boolean) => {
    setError(null)
    const required: (keyof CreateProjectBody)[] = [
      'project_name', 'gee_project', 'credentials_file', 'start_date', 'end_date',
    ]
    const missing = required.filter(k => !String(form[k]).trim())
    if (missing.length) {
      setError(`请填写: ${missing.join(', ')}`)
      return
    }
    setLoading(true)
    try {
      const created = await api.createProject(form)
      if (andStart) {
        await api.startProject(created.project_id)
      }
      onCreated(created.project_id)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }

  return (
    /* Backdrop */
    <div
      style={{
        position: 'fixed', inset: 0,
        background: 'rgba(0,0,0,0.6)',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        zIndex: 1000,
      }}
      onClick={e => { if (e.target === e.currentTarget) onClose() }}
    >
      {/* Dialog */}
      <div style={{
        background: '#13151e',
        border: '1px solid #2d3148',
        borderRadius: 12,
        padding: 28,
        width: 480,
        maxWidth: '95vw',
        fontFamily: 'ui-monospace, monospace',
      }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 20 }}>
          <div style={{ color: '#f3f4f6', fontWeight: 700, fontSize: 15 }}>新建 GEE 项目</div>
          <span
            onClick={onClose}
            style={{ color: '#6b7280', cursor: 'pointer', fontSize: 20, lineHeight: 1 }}
          >×</span>
        </div>

        <div style={fieldStyle}>
          <label style={labelStyle}>项目名称 *</label>
          <input
            style={inputStyle}
            placeholder="e.g. 北美 2020"
            value={form.project_name}
            onChange={e => set('project_name', e.target.value)}
          />
        </div>

        <div style={fieldStyle}>
          <label style={labelStyle}>GEE Cloud Project ID *</label>
          <input
            style={inputStyle}
            placeholder="e.g. my-gee-project"
            value={form.gee_project}
            onChange={e => set('gee_project', e.target.value)}
          />
        </div>

        <div style={fieldStyle}>
          <label style={labelStyle}>Credentials 文件路径（服务器本地路径）*</label>
          <input
            style={inputStyle}
            placeholder="e.g. /home/user/.hydrofetch/client_secret.json"
            value={form.credentials_file}
            onChange={e => set('credentials_file', e.target.value)}
          />
        </div>

        <div style={{ display: 'flex', gap: 12, marginBottom: 14 }}>
          <div style={{ flex: 1 }}>
            <label style={labelStyle}>起始日期 *</label>
            <input
              type="date"
              style={inputStyle}
              value={form.start_date}
              onChange={e => set('start_date', e.target.value)}
            />
          </div>
          <div style={{ flex: 1 }}>
            <label style={labelStyle}>结束日期（不含）*</label>
            <input
              type="date"
              style={inputStyle}
              value={form.end_date}
              onChange={e => set('end_date', e.target.value)}
            />
          </div>
        </div>

        <div style={fieldStyle}>
          <label style={labelStyle}>最大并发导出数</label>
          <input
            type="number"
            min={1}
            max={50}
            style={{ ...inputStyle, width: 100 }}
            value={form.max_concurrent}
            onChange={e => set('max_concurrent', Math.max(1, Number(e.target.value)))}
          />
        </div>

        {error && (
          <div style={{
            background: '#3b1f1f', border: '1px solid #ef4444',
            borderRadius: 7, padding: '9px 12px', color: '#fca5a5',
            fontSize: 12, marginBottom: 14,
          }}>
            {error}
          </div>
        )}

        <div style={{ display: 'flex', gap: 10, justifyContent: 'flex-end', marginTop: 4 }}>
          <button
            onClick={onClose}
            disabled={loading}
            style={{
              background: 'transparent', border: '1px solid #2d3148',
              color: '#9ca3af', borderRadius: 6, padding: '7px 18px',
              fontSize: 13, cursor: 'pointer',
            }}
          >
            取消
          </button>
          <button
            onClick={() => submit(false)}
            disabled={loading}
            style={{
              background: '#1e2130', border: '1px solid #3b82f6',
              color: '#93c5fd', borderRadius: 6, padding: '7px 18px',
              fontSize: 13, cursor: loading ? 'not-allowed' : 'pointer',
            }}
          >
            {loading ? '创建中…' : '仅创建'}
          </button>
          <button
            onClick={() => submit(true)}
            disabled={loading}
            style={{
              background: '#1d4ed8', border: 'none',
              color: '#fff', borderRadius: 6, padding: '7px 18px',
              fontSize: 13, fontWeight: 600, cursor: loading ? 'not-allowed' : 'pointer',
            }}
          >
            {loading ? '创建中…' : '创建并启动'}
          </button>
        </div>
      </div>
    </div>
  )
}
