# hydrofetch-dashboard-web

Hydrofetch 本地监控 Dashboard 的 React/Vite 前端。

## 快速启动

```bash
cd packages/hydrofetch-dashboard-web
npm run dev
```

默认在 `http://localhost:5173` 启动，通过 Vite 代理将 `/api` 请求转发到 `http://127.0.0.1:8000`。

## 一键启动前后端

从 monorepo 根目录执行：

```bash
bash scripts/start_dashboard.sh
```

## 页面

| 路由 | 功能 |
|------|------|
| `/` | 总览：KPI、状态分布、近 6h 趋势 |
| `/failures` | 失败诊断：错误分布 + 明细表 |
| `/jobs` | 任务明细：按状态/tile/尝试次数筛选 + 分页 |
| `/ingest` | 入库进度：总行数、日期范围、每日入库量 |
| `/alerts` | 运行告警：停滞任务、高重试任务、日志 ERROR |

## 数据刷新策略

- KPI / 状态分布：10 秒轮询
- 趋势 / 失败 / 告警：15–30 秒轮询
- 任务明细：筛选变化或手动触发
- 入库统计：60 秒轮询
