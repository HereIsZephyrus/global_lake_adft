import { useState } from 'react'
import { BrowserRouter, NavLink, Route, Routes, useNavigate } from 'react-router-dom'
import useSWR from 'swr'

import Overview from './pages/Overview'
import Failures from './pages/Failures'
import Jobs from './pages/Jobs'
import Alerts from './pages/Alerts'
import GlobalView from './pages/GlobalView'

import ProjectTabs from './components/ProjectTabs'
import CreateProjectDialog from './components/CreateProjectDialog'

import { api } from './api/client'

const NAV_ITEMS = [
  { path: '/', label: '总览' },
  { path: '/failures', label: '失败诊断' },
  { path: '/jobs', label: '任务明细' },
  { path: '/alerts', label: '运行告警' },
]

// ---------------------------------------------------------------------------
// Inner app — needs router context for NavLink
// ---------------------------------------------------------------------------

function Inner() {
  const navigate = useNavigate()

  // Active project: null = global view; string = project_id
  const [activeId, setActiveId] = useState<string | null>(null)
  const [showCreate, setShowCreate] = useState(false)
  const [deleteError, setDeleteError] = useState<string | null>(null)

  const { data: projects = [], mutate: mutateProjects } = useSWR(
    'projects',
    api.projects,
    { refreshInterval: 60_000, revalidateOnFocus: false },
  )

  const handleDelete = async (id: string) => {
    if (!window.confirm(`确认删除项目 "${id}"？这将删除该项目的所有任务数据。`)) return
    setDeleteError(null)
    try {
      await api.deleteProject(id)
      await mutateProjects()
      if (activeId === id) setActiveId(null)
    } catch (e: unknown) {
      setDeleteError(e instanceof Error ? e.message : String(e))
    }
  }

  const handleCreated = async (newId: string) => {
    setShowCreate(false)
    await mutateProjects()
    setActiveId(newId)
    navigate('/')
  }

  const handleSelectProject = (id: string | null) => {
    setActiveId(id)
    // Navigate to overview when switching projects
    navigate('/')
  }

  const isGlobal = activeId === null

  return (
    <div style={{
      display: 'flex', flexDirection: 'column',
      minHeight: '100vh', background: '#0f1117',
      fontFamily: 'ui-monospace, monospace',
    }}>
      {/* Top: branding + tab bar */}
      <div style={{
        display: 'flex', alignItems: 'center',
        background: '#13151e', borderBottom: '1px solid #1e2130',
        flexShrink: 0,
      }}>
        <div style={{
          color: '#60a5fa', fontWeight: 700, fontSize: 14,
          padding: '0 20px', whiteSpace: 'nowrap', flexShrink: 0,
          height: 40, display: 'flex', alignItems: 'center',
          borderRight: '1px solid #1e2130',
        }}>
          Hydrofetch
        </div>
        <ProjectTabs
          projects={projects}
          activeId={activeId}
          onSelect={handleSelectProject}
          onAdd={() => setShowCreate(true)}
          onDelete={handleDelete}
        />
      </div>

      {deleteError && (
        <div style={{
          background: '#3b1f1f', borderBottom: '1px solid #ef4444',
          color: '#fca5a5', fontSize: 12, padding: '7px 20px',
        }}>
          删除失败: {deleteError}
          <span
            style={{ marginLeft: 12, cursor: 'pointer', color: '#f87171' }}
            onClick={() => setDeleteError(null)}
          >×</span>
        </div>
      )}

      <div style={{ display: 'flex', flex: 1, minHeight: 0 }}>
        {/* Sidebar — hidden when global view and no project selected with a nav */}
        {!isGlobal && (
          <nav style={{
            width: 160, background: '#13151e', borderRight: '1px solid #1e2130',
            display: 'flex', flexDirection: 'column', padding: '16px 0', flexShrink: 0,
          }}>
            {NAV_ITEMS.map(item => (
              <NavLink
                key={item.path}
                to={item.path}
                end={item.path === '/'}
                style={({ isActive }) => ({
                  display: 'block',
                  padding: '9px 20px',
                  color: isActive ? '#f3f4f6' : '#6b7280',
                  background: isActive ? '#1e2130' : 'transparent',
                  borderLeft: isActive ? '3px solid #3b82f6' : '3px solid transparent',
                  textDecoration: 'none',
                  fontSize: 13,
                  fontWeight: isActive ? 600 : 400,
                  transition: 'all 0.15s',
                })}
              >
                {item.label}
              </NavLink>
            ))}
          </nav>
        )}

        {/* Main content */}
        <main style={{ flex: 1, padding: '28px 32px', overflowY: 'auto', minWidth: 0 }}>
          {isGlobal ? (
            <GlobalView />
          ) : (
            <Routes>
              <Route path="/" element={<Overview projectId={activeId!} />} />
              <Route path="/failures" element={<Failures projectId={activeId!} />} />
              <Route path="/jobs" element={<Jobs projectId={activeId!} />} />
              <Route path="/alerts" element={<Alerts projectId={activeId!} />} />
            </Routes>
          )}

          {/* Prompt when no projects exist */}
          {isGlobal && projects.length === 0 && (
            <div style={{
              marginTop: 32, textAlign: 'center', color: '#6b7280', fontSize: 14,
            }}>
              <div style={{ marginBottom: 12 }}>暂无项目。点击上方「+ 新建」创建第一个 GEE 项目。</div>
              <button
                onClick={() => setShowCreate(true)}
                style={{
                  background: '#1d4ed8', border: 'none', color: '#fff',
                  borderRadius: 7, padding: '8px 20px', fontSize: 13,
                  fontWeight: 600, cursor: 'pointer',
                }}
              >
                + 新建项目
              </button>
            </div>
          )}
        </main>
      </div>

      {showCreate && (
        <CreateProjectDialog
          onClose={() => setShowCreate(false)}
          onCreated={handleCreated}
        />
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Root
// ---------------------------------------------------------------------------

export default function App() {
  return (
    <BrowserRouter>
      <Inner />
    </BrowserRouter>
  )
}
