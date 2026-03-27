# hydrofetch-dashboard-web

`hydrofetch-dashboard-web` 是 Hydrofetch 本地监控 Dashboard 的 React/Vite 前端，用来查看多项目运行状态，并通过界面创建和控制采集项目。

## 快速启动

```bash
cd packages/hydrofetch-dashboard-web
npm install
npm run dev
```

默认在 `http://localhost:5170` 启动，通过 Vite 代理将 `/api` 请求转发到 `http://127.0.0.1:8050`。

## 一键启动前后端

从 monorepo 根目录执行：

```bash
bash scripts/start_dashboard.sh
```

脚本会同时启动：

- 前端：`http://localhost:5170`
- 后端：`http://127.0.0.1:8050`

## 如何验证

开发前建议先做一次静态验证：

```bash
cd packages/hydrofetch-dashboard-web
npm run build
npm run lint
```

运行中可通过以下方式验证：

- 打开 `http://localhost:5170`
- 确认首页能正常加载项目列表
- 如果后端已启动，确认页面不再出现 `/api` 请求错误

## 如何添加项目

启动 Dashboard 后，可直接在页面中点击“新建 GEE 项目”，填写：

- 项目名称
- GEE Cloud Project ID
- `credentials_file` 本机路径
- 起始日期
- 结束日期（不含）
- 最大并发导出数

界面支持：

- 仅创建项目
- 创建并立即启动项目
- 切换项目查看详情
- 删除已有项目

项目实际由后端创建在 `data/projects/<project_id>/` 下。

## 页面

| 路由 | 功能 |
|------|------|
| 无项目选中时首页 | 全局视图：项目列表、数据库体量、日志与日期进度 |
| `/` | 总览：KPI、状态分布、近 6h 趋势 |
| `/failures` | 失败诊断：错误分布 + 明细表 |
| `/jobs` | 任务明细：按状态/tile/尝试次数筛选 + 分页 |
| `/alerts` | 运行告警：停滞任务、高重试任务、日志 ERROR |

## 数据刷新策略

- KPI / 状态分布：10 秒轮询
- 趋势 / 失败 / 告警：15–30 秒轮询
- 任务明细：筛选变化或手动触发
- 全局日志 / 体量统计：30–60 秒轮询
