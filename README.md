# global_lake_adft

`global_lake_adft` 是一个以 `uv workspace` 为核心的 monorepo，用来统一管理湖泊数据抓取、监控和分析相关项目。

当前包含 4 个主要子项目：

- `hydrofetch`：负责 GEE 导出、Google Drive 下载、本地采样与结果写出。
- `hydrofetch-dashboard-api`：提供多项目监控与控制的 FastAPI 后端。
- `hydrofetch-dashboard-web`：提供 Dashboard 前端界面。
- `lakeanalysis`：负责湖泊分析、统计建模与下游消费。

## 仓库结构

```text
global_lake_adft/
├── data/                        # 本地数据、tile manifest、项目运行目录
├── docs/                        # 架构、迁移、研究与运行手册
├── packages/
│   ├── hydrofetch/
│   ├── hydrofetch-dashboard-api/
│   ├── hydrofetch-dashboard-web/
│   └── lakeanalysis/
├── scripts/                     # 仓库级脚本，例如一键启动 dashboard
├── tests/integration/           # 跨包测试
├── pyproject.toml               # uv workspace 定义
└── uv.lock
```

## 首次安装

以下命令默认在仓库根目录执行：

```bash
uv sync --all-packages --group dev
cd packages/hydrofetch-dashboard-web && npm install
```

如果只想安装单个 Python 包：

```bash
uv sync --package hydrofetch --group dev
uv sync --package hydrofetch-dashboard-api --group dev
uv sync --package lakeanalysis --group dev
```

## 首次配置与认证

先准备本地环境文件：

```bash
cp packages/hydrofetch/.env.example packages/hydrofetch/.env
cp packages/lakeanalysis/.env.example packages/lakeanalysis/.env
```

`hydrofetch` 至少需要配置：

```bash
HYDROFETCH_GEE_PROJECT=your-gee-project-id
HYDROFETCH_CREDENTIALS_FILE=~/.hydrofetch/credentials.json
HYDROFETCH_TOKEN_FILE=~/.hydrofetch/token.json
```

首次认证流程：

```bash
earthengine authenticate
uv run --package hydrofetch hydrofetch auth
```

说明：

- `HYDROFETCH_CREDENTIALS_FILE` 指向 Google Cloud Console 下载的 OAuth Desktop App 凭证 JSON。
- `HYDROFETCH_TOKEN_FILE` 是 `hydrofetch auth` 成功后保存 Drive token 的位置。
- Dashboard 为每个运行项目单独维护 `data/projects/<project_id>/token.json`，但仍需要提供 `credentials_file` 作为授权入口。

## 如何验证环境

推荐按下面顺序验证：

```bash
# 1. 验证 GEE / Drive 连通性
uv run --package hydrofetch python packages/hydrofetch/scripts/check_google_connectivity.py

# 2. Python 包测试
uv run --package hydrofetch pytest packages/hydrofetch/tests
uv run --package lakeanalysis pytest packages/lakeanalysis/tests

# 3. Python 静态检查
uv run --package hydrofetch pylint packages/hydrofetch/src/hydrofetch
uv run --package lakeanalysis pylint packages/lakeanalysis/src/lakeanalysis

# 4. Dashboard 前端检查
npm --prefix packages/hydrofetch-dashboard-web run build
npm --prefix packages/hydrofetch-dashboard-web run lint
```

## 如何启动

### 启动 `hydrofetch` CLI

```bash
uv run --package hydrofetch hydrofetch era5 \
  --start 2020-01-01 \
  --end 2020-02-01 \
  --tile-manifest data/continents/continents_manifest.json \
  --output-dir ./results \
  --run
```

查看任务状态：

```bash
uv run --package hydrofetch hydrofetch status
uv run --package hydrofetch hydrofetch status --verbose
```

### 启动 Dashboard

一键启动前后端：

```bash
bash scripts/start_dashboard.sh
```

启动后访问：

- 前端：`http://localhost:5170`
- 后端 API 文档：`http://127.0.0.1:8050/docs`

如果只启动后端：

```bash
uv run --package hydrofetch-dashboard-api python -m hydrofetch_dashboard_api.main
```

如果只启动前端：

```bash
cd packages/hydrofetch-dashboard-web
npm run dev
```

## 如何添加项目

这里的“项目”有两种常见含义。

### 1. 添加一个新的采集运行项目

推荐方式是启动 Dashboard 后，在前端界面点击“新建 GEE 项目”，填写：

- `project_name`
- `gee_project`
- `credentials_file`
- `start_date`
- `end_date`
- `max_concurrent`

创建后，系统会自动在 `data/projects/<project_id>/` 下生成：

- `config.json`
- `jobs/`
- `raw/`
- `sample/`
- `logs/`

也可以直接调用 API：

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

### 2. 向 monorepo 新增一个代码项目

最小步骤如下：

1. 在 `packages/` 下创建新目录，并补齐 `pyproject.toml` 或前端包清单。
2. 给新项目添加独立 `README.md`、源码目录和测试目录。
3. 如果是 Python workspace 包，把路径加入根级 `pyproject.toml` 的 `[tool.uv.workspace].members`。
4. 在根 README 补充该项目职责和常用命令。
5. 运行 `uv sync --all-packages --group dev` 验证 workspace 仍可解析。

## 文档入口

- `docs/runbooks/monorepo-usage.md`：统一使用说明，覆盖认证、验证、启动和添加项目。
- `packages/hydrofetch/README.md`：抓取管线与认证说明。
- `packages/hydrofetch-dashboard-api/README.md`：Dashboard 后端接口与多项目控制。
- `packages/hydrofetch-dashboard-web/README.md`：Dashboard 前端开发与使用方式。
- `packages/lakeanalysis/README.md`：分析包开发说明。

## 迁移与背景

- `docs/migration/from-legacy-repos.md`
- `docs/migration/geeconnect-to-hydrofetch.md`
- `docs/architecture/repository-layout.md`
- `docs/architecture/package-boundaries.md`
- `docs/research/README.md`