# Hydrofetch Dashboard

本目录提供一个基于 `Streamlit` 的本机监控面板，用于查看 `hydrofetch` 的 job 状态、失败分布、任务明细、入库进度和运行告警。

## 启动

在仓库根目录执行：

```bash
uv run --package hydrofetch streamlit run \
  packages/hydrofetch/scripts/dashboard/app.py \
  --server.port 8501
```

启动后默认访问：

```text
http://localhost:8501
```

## 默认数据源

- Job 目录：`data/hydrofetch_full_file_db_jobs`
- 日志目录：`logs`
- 数据库表：默认从 job 配置中读取，缺省为 `era5_forcing`

## 页面

- `总览`：任务规模、状态分布、近 6 小时趋势、按 tile 进度
- `失败`：失败任务表、错误聚合、按 tile 和日期分布
- `任务`：按状态、tile、attempt 过滤单任务详情
- `入库`：数据库统计和 job 与 DB 的对照
- `告警`：停滞任务、高重试任务、最近日志错误和写库日志

## 说明

- 数据库统计是可选增强项；若未配置 `HYDROFETCH_DB_*`，页面仍可基于本地 job JSON 正常工作。
- 自动刷新由页面侧边栏控制，默认 30 秒。
- 当前实现优先面向本机巡检，不包含鉴权与共享访问控制。
