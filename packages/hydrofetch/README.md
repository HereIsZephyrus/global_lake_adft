# hydrofetch

`hydrofetch` 是本仓库的核心抓取 CLI，负责把 ERA5-Land 影像从 Google Earth Engine 导出到 Google Drive，再下载、采样并写出为本地文件或数据库结果。

每个任务都会持久化状态，因此中断后可以继续恢复，不必从头重跑。

## 状态机

```text
Hold → Export → Download → Cleanup → Sample → Write → Completed
                                                 ↑
                                           Failed (max retries)
```

## 快速开始

以下命令默认在 monorepo 根目录执行。

### 1. 安装

```bash
uv sync --package hydrofetch --group dev
```

### 2. 配置

```bash
cp packages/hydrofetch/.env.example packages/hydrofetch/.env
```

至少要填写：

```bash
HYDROFETCH_GEE_PROJECT=your-gee-project-id
HYDROFETCH_CREDENTIALS_FILE=~/.hydrofetch/credentials.json
HYDROFETCH_TOKEN_FILE=~/.hydrofetch/token.json
```

### 3. 认证

```bash
earthengine authenticate
uv run --package hydrofetch hydrofetch auth
```

其中：

- `earthengine authenticate` 负责 Earth Engine 认证。
- `hydrofetch auth` 会读取 `HYDROFETCH_CREDENTIALS_FILE`，启动 Google Drive OAuth 浏览器流程，并把 token 保存到 `HYDROFETCH_TOKEN_FILE`。

### 4. 启动任务

```bash
uv run --package hydrofetch hydrofetch era5 \
  --start 2020-01-01 \
  --end 2020-02-01 \
  --tile-manifest data/continents/continents_manifest.json \
  --output-dir ./results \
  --run
```

常用命令：

```bash
uv run --package hydrofetch hydrofetch status
uv run --package hydrofetch hydrofetch status --verbose
uv run --package hydrofetch hydrofetch retry --job-id era5_land_daily_image_20200115
```

## 如何验证

### 验证认证与连通性

```bash
uv run --package hydrofetch python packages/hydrofetch/scripts/check_google_connectivity.py
```

如果只检查某一侧：

```bash
uv run --package hydrofetch python packages/hydrofetch/scripts/check_google_connectivity.py --skip-drive
uv run --package hydrofetch python packages/hydrofetch/scripts/check_google_connectivity.py --skip-gee
```

### 验证代码质量

```bash
uv run --package hydrofetch pytest packages/hydrofetch/tests
uv run --package hydrofetch pylint packages/hydrofetch/src/hydrofetch
```

## 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `HYDROFETCH_GEE_PROJECT` | 必填 | GEE Cloud Project ID |
| `HYDROFETCH_CREDENTIALS_FILE` | 必填 | Google OAuth client secret JSON 路径 |
| `HYDROFETCH_TOKEN_FILE` | `~/.hydrofetch/token.json` | Drive OAuth token 保存路径 |
| `HYDROFETCH_DRIVE_FOLDER_NAME` | 根目录 | GEE 导出到 Drive 的目录名 |
| `HYDROFETCH_JOB_DIR` | `./hydrofetch_jobs` | 任务 JSON 目录 |
| `HYDROFETCH_RAW_DIR` | `./hydrofetch_raw` | 原始 GeoTIFF 目录 |
| `HYDROFETCH_SAMPLE_DIR` | `./hydrofetch_sample` | 采样结果目录 |
| `HYDROFETCH_MAX_CONCURRENT` | `5` | 最大并发 GEE 导出数 |
| `HYDROFETCH_POLL_INTERVAL` | `15` | 任务轮询间隔，单位秒 |
| `HYDROFETCH_DB` | 可选 | `db` sink 使用的目标数据库 |
| `HYDROFETCH_DB_USER` | 可选 | PostgreSQL 用户名 |
| `HYDROFETCH_DB_PASSWORD` | 可选 | PostgreSQL 密码 |
| `HYDROFETCH_DB_HOST` | `localhost` | PostgreSQL 主机 |
| `HYDROFETCH_DB_PORT` | `5432` | PostgreSQL 端口 |
| `ALTAS_DB` | 可选 | smoke fixture 生成时读取 LakeATLAS 的数据库 |

## 几何与 tile manifest

推荐使用 `--tile-manifest` 驱动分 tile 运行。manifest 中每个条目通常包含：

- `tile_id`
- `geometry_path`
- `region_path`

其中 `geometry_path` 对应采样湖泊集合，`region_path` 用于限制导出范围。

## Smoke 测试

```bash
bash packages/hydrofetch/scripts/smoke/run_smoke.sh
```

这个脚本会基于数据库生成一套最小湖泊样本，并验证 manifest 展开、导出、采样和写出链路。

## 恢复机制

任务记录以 `<job_dir>/<job_id>.json` 形式原子写入。进程重启后会重新加载未完成任务，并继续：

1. 读取非终态任务。
2. 恢复并发槽位占用。
3. 对 `Export` 任务重新轮询 GEE `task_id`。
4. 对 `Download` 任务检查本地文件是否已存在。

各状态处理器均为幂等设计。

## 项目结构

```text
packages/hydrofetch/
├── src/hydrofetch/
│   ├── cli.py
│   ├── config.py
│   ├── catalog/
│   ├── drive/
│   ├── export/
│   ├── gee/
│   ├── jobs/
│   ├── monitor/
│   ├── sample/
│   ├── state_machine/
│   └── write/
├── scripts/
└── tests/
```

## 与 Dashboard 的关系

如果通过 Dashboard 启动项目，后端会为每个项目单独设置：

- `HYDROFETCH_JOB_DIR`
- `HYDROFETCH_RAW_DIR`
- `HYDROFETCH_SAMPLE_DIR`
- `HYDROFETCH_TOKEN_FILE`

因此同一台机器上可以并行维护多个采集项目，而不互相覆盖运行目录。
