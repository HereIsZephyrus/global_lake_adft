# hydrofetch-dashboard-api

`hydrofetch-dashboard-api` 是 Hydrofetch 本地监控 Dashboard 的 FastAPI 后端，负责：

- 提供项目列表、任务状态、失败统计、日志和入库进度 API
- 创建、启动、停止和删除多项目运行目录
- 为前端 Dashboard 提供统一数据源

## 快速启动

以下命令在仓库根目录执行：

```bash
uv sync --package hydrofetch-dashboard-api --group dev
uv run --package hydrofetch-dashboard-api python -m hydrofetch_dashboard_api.main
```

默认监听 `http://127.0.0.1:8050`。API 文档见 `http://127.0.0.1:8050/docs`。

## 运行前准备

后端会自动加载 `packages/hydrofetch/.env`，因此建议先准备：

```bash
cp packages/hydrofetch/.env.example packages/hydrofetch/.env
```

若要通过 Dashboard 直接启动 `hydrofetch` 项目，需要确认：

- `HYDROFETCH_TILE_MANIFEST` 已配置，或仓库中存在 `data/continents/continents_manifest.json`
- `HYDROFETCH_DB`、`HYDROFETCH_DB_USER`、`HYDROFETCH_DB_PASSWORD` 等数据库变量已配置

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `HYDROFETCH_DASHBOARD_JOB_DIR` | `data/hydrofetch_full_file_db_jobs` | 单目录模式下的 Job JSON 目录 |
| `HYDROFETCH_DASHBOARD_LOG_DIR` | `logs/` | 单目录模式下的日志目录 |
| `HYDROFETCH_DASHBOARD_PROJECTS_DIR` | `data/projects` | 多项目根目录 |
| `HYDROFETCH_DASHBOARD_DB_TABLE` | `era5_forcing` | Dashboard 默认统计的目标表 |
| `HYDROFETCH_TILE_MANIFEST` | （若存在）`data/continents/continents_manifest.json` | 启动项目时传给 `hydrofetch era5 --tile-manifest` 的路径 |
| `HYDROFETCH_DASHBOARD_API_PORT` | `8050` | API 端口 |
| `HYDROFETCH_DB` | — | PostgreSQL 数据库名 |
| `HYDROFETCH_DB_USER` | — | 数据库用户 |
| `HYDROFETCH_DB_PASSWORD` | — | 数据库密码 |
| `HYDROFETCH_DB_HOST` | `localhost` | 数据库主机 |
| `HYDROFETCH_DB_PORT` | `5432` | 数据库端口 |

## 如何验证

启动服务后先做最小验证：

```bash
curl http://127.0.0.1:8050/api/health
curl http://127.0.0.1:8050/api/projects
```

如果需要验证代码侧启动是否正常，可直接打开 `http://127.0.0.1:8050/docs` 查看 OpenAPI 文档。

## 如何添加项目

每个项目会保存在 `data/projects/<project_id>/` 下，目录中会自动创建：

- `config.json`
- `jobs/`
- `raw/`
- `sample/`
- `logs/`

创建项目示例：

```bash
curl -X POST http://127.0.0.1:8050/api/projects \
  -H 'Content-Type: application/json' \
  -d '{
    "project_name": "North America 2020",
    "gee_project": "your-gee-project-id",
    "credentials_file": "/home/you/.hydrofetch/credentials.json",
    "start_date": "2020-01-01",
    "end_date": "2020-02-01",
    "max_concurrent": 5
  }'
```

启动、停止和删除示例：

```bash
curl -X POST http://127.0.0.1:8050/api/projects/north-america-2020/start
curl -X POST http://127.0.0.1:8050/api/projects/north-america-2020/stop
curl -X DELETE http://127.0.0.1:8050/api/projects/north-america-2020
```

## API 端点

| Method | Path | 说明 |
|--------|------|------|
| GET | `/api/health` | 健康检查 |
| GET | `/api/projects` | 项目列表 |
| POST | `/api/projects` | 创建项目 |
| GET | `/api/projects/{project_id}` | 单项目配置与状态 |
| DELETE | `/api/projects/{project_id}` | 删除项目 |
| POST | `/api/projects/{project_id}/start` | 启动项目 |
| POST | `/api/projects/{project_id}/stop` | 停止项目 |
| GET | `/api/projects/{project_id}/overview` | 单项目 KPI 汇总 |
| GET | `/api/projects/{project_id}/states` | 单项目状态统计 |
| GET | `/api/projects/{project_id}/timeline?hours=6` | 单项目趋势 |
| GET | `/api/projects/{project_id}/failures` | 单项目失败统计 |
| GET | `/api/projects/{project_id}/jobs` | 单项目分页任务列表 |
| GET | `/api/projects/{project_id}/alerts` | 单项目告警 |
| GET | `/api/projects/{project_id}/logs` | 单项目日志 |
| GET | `/api/overview` | 全局 KPI 汇总 |
| GET | `/api/states` | 全局状态数量 |
| GET | `/api/timeline?hours=6` | 全局最近 N 小时趋势 |
| GET | `/api/failures` | 全局失败任务与错误摘要 |
| GET | `/api/jobs` | 全局分页任务列表 |
| GET | `/api/tile-progress` | 按 tile 聚合进度 |
| GET | `/api/ingest` | PostgreSQL 入库统计 |
| GET | `/api/alerts` | 全局停滞/高重试告警 |
| GET | `/api/logs` | 全局日志 ERROR/WARNING |

## 包结构

```text
src/hydrofetch_dashboard_api/
  main.py               # FastAPI app 入口
  config.py             # 环境变量配置
  api/routes.py         # 路由定义
  services/
    metrics.py          # 聚合计算逻辑
    process_manager.py  # 启停 hydrofetch 子进程
  sources/
    jobs.py             # 读取 job JSON（带缓存）
    logs.py             # 解析日志文件
    database.py         # PostgreSQL 查询
    projects.py         # 项目配置 CRUD
```
