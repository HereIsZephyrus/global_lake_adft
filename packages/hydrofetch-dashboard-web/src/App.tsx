import { BrowserRouter, NavLink, Route, Routes } from 'react-router-dom'
import Overview from './pages/Overview'
import Failures from './pages/Failures'
import Jobs from './pages/Jobs'
import Ingest from './pages/Ingest'
import Alerts from './pages/Alerts'

const NAV_ITEMS = [
  { path: '/', label: '总览' },
  { path: '/failures', label: '失败诊断' },
  { path: '/jobs', label: '任务明细' },
  { path: '/ingest', label: '入库进度' },
  { path: '/alerts', label: '运行告警' },
]

export default function App() {
  return (
    <BrowserRouter>
      <div style={{ display: 'flex', minHeight: '100vh', background: '#0f1117', fontFamily: 'ui-monospace, monospace' }}>
        {/* Sidebar */}
        <nav style={{
          width: 180, background: '#13151e', borderRight: '1px solid #1e2130',
          display: 'flex', flexDirection: 'column', padding: '24px 0', flexShrink: 0,
        }}>
          <div style={{ color: '#60a5fa', fontWeight: 700, fontSize: 14, padding: '0 20px 24px' }}>
            🌊 Hydrofetch
          </div>
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

        {/* Main content */}
        <main style={{ flex: 1, padding: '28px 32px', overflowY: 'auto' }}>
          <Routes>
            <Route path="/" element={<Overview />} />
            <Route path="/failures" element={<Failures />} />
            <Route path="/jobs" element={<Jobs />} />
            <Route path="/ingest" element={<Ingest />} />
            <Route path="/alerts" element={<Alerts />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  )
}
