# hydrofetch-dashboard-api

Hydrofetch 本地监控 Dashboard 的 FastAPI 后端。提供 job 状态、失败统计、入库进度等只读 HTTP API。

## 快速启动

```bash
# 从 monorepo 根目录
uv run --package hydrofetch-dashboard-api python -m hydrofetch_dashboard_api.main
```

默认监听 `http://127.0.0.1:8000`。API 文档见 http://127.0.0.1:8000/docs。

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `HYDROFETCH_DASHBOARD_JOB_DIR` | `data/hydrofetch_full_file_db_jobs` | Job JSON 目录 |
| `HYDROFETCH_DASHBOARD_LOG_DIR` | `logs/` | 日志目录 |
| `HYDROFETCH_DASHBOARD_DB_TABLE` | `era5_forcing` | 入库统计的目标表 |
| `HYDROFETCH_DASHBOARD_API_PORT` | `8000` | 监听端口 |
| `HYDROFETCH_DB` | — | PostgreSQL 数据库名 |
| `HYDROFETCH_DB_USER` | — | 数据库用户 |
| `HYDROFETCH_DB_PASSWORD` | — | 数据库密码 |
| `HYDROFETCH_DB_HOST` | `localhost` | 数据库主机 |

## API 端点

| Method | Path | 说明 |
|--------|------|------|
| GET | `/api/health` | 健康检查 |
| GET | `/api/overview` | KPI 汇总 |
| GET | `/api/states` | 各状态数量 |
| GET | `/api/timeline?hours=6` | 最近 N 小时趋势 |
| GET | `/api/failures` | 失败任务与错误摘要 |
| GET | `/api/jobs` | 分页任务列表（支持筛选） |
| GET | `/api/tile-progress` | 按 tile 聚合进度 |
| GET | `/api/ingest` | PostgreSQL 入库统计 |
| GET | `/api/alerts` | 停滞/高重试告警 |
| GET | `/api/logs` | 日志 ERROR/WARNING |

## 包结构

```
src/hydrofetch_dashboard_api/
  main.py          # FastAPI app 入口
  config.py        # 环境变量配置
  api/routes.py    # 路由定义
  services/
    metrics.py     # 聚合计算逻辑
  sources/
    jobs.py        # 读取 job JSON（带缓存）
    logs.py        # 解析日志文件
    database.py    # PostgreSQL 查询
```
