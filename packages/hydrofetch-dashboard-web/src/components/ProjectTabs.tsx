import type { ProjectConfig } from '../api/client'

interface Props {
  projects: ProjectConfig[]
  activeId: string | null
  onSelect: (id: string | null) => void
  onAdd: () => void
  onDelete: (id: string) => void
}

const STATUS_DOT: Record<string, string> = {
  running: '#10b981',
  finished: '#3b82f6',
  stopped: '#6b7280',
}

const STATUS_LABEL: Record<string, string> = {
  running: '运行中',
  finished: '已完成',
  stopped: '已停止',
}

export default function ProjectTabs({ projects, activeId, onSelect, onAdd, onDelete }: Props) {
  const tabBase: React.CSSProperties = {
    display: 'flex',
    alignItems: 'center',
    gap: 7,
    padding: '7px 14px',
    fontSize: 13,
    cursor: 'pointer',
    borderBottom: '2px solid transparent',
    whiteSpace: 'nowrap',
    userSelect: 'none',
    transition: 'all 0.15s',
    flexShrink: 0,
  }

  const activeStyle: React.CSSProperties = {
    borderBottomColor: '#3b82f6',
    color: '#f3f4f6',
    fontWeight: 600,
    background: 'rgba(59,130,246,0.08)',
  }

  const inactiveStyle: React.CSSProperties = {
    color: '#6b7280',
    borderBottomColor: 'transparent',
  }

  return (
    <div style={{
      display: 'flex',
      alignItems: 'center',
      background: '#13151e',
      borderBottom: '1px solid #1e2130',
      paddingLeft: 8,
      overflowX: 'auto',
      flexShrink: 0,
      minHeight: 40,
    }}>
      {/* Project tabs */}
      {projects.map(p => {
        const isActive = p.project_id === activeId
        return (
          <div
            key={p.project_id}
            style={{ ...tabBase, ...(isActive ? activeStyle : inactiveStyle) }}
            onClick={() => onSelect(p.project_id)}
          >
            {/* Status dot */}
            <span
              title={STATUS_LABEL[p.status] ?? p.status}
              style={{
                width: 7,
                height: 7,
                borderRadius: '50%',
                background: STATUS_DOT[p.status] ?? '#6b7280',
                flexShrink: 0,
                boxShadow: p.status === 'running' ? `0 0 5px ${STATUS_DOT.running}` : undefined,
              }}
            />
            <span style={{ maxWidth: 140, overflow: 'hidden', textOverflow: 'ellipsis' }}>
              {p.project_name}
            </span>
            {/* Delete button */}
            <span
              onClick={e => { e.stopPropagation(); onDelete(p.project_id) }}
              title="删除项目"
              style={{
                marginLeft: 2,
                color: '#4b5563',
                fontSize: 15,
                lineHeight: 1,
                padding: '0 2px',
                borderRadius: 3,
                cursor: 'pointer',
              }}
              onMouseEnter={e => (e.currentTarget.style.color = '#ef4444')}
              onMouseLeave={e => (e.currentTarget.style.color = '#4b5563')}
            >
              ×
            </span>
          </div>
        )
      })}

      {/* Add button */}
      <div
        style={{ ...tabBase, ...inactiveStyle, color: '#3b82f6' }}
        onClick={onAdd}
        title="新建项目"
      >
        + 新建
      </div>

      {/* Spacer */}
      <div style={{ flex: 1 }} />

      {/* Global tab */}
      <div
        style={{
          ...tabBase,
          ...(activeId === null ? activeStyle : inactiveStyle),
          marginRight: 8,
        }}
        onClick={() => onSelect(null)}
        title="全局视图（入库进度 & 数据库）"
      >
        全局
      </div>
    </div>
  )
}
