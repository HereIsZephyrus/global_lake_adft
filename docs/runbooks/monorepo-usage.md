# Monorepo 使用说明

本文档面向日常开发和运行维护，覆盖：

- 首次安装
- GEE / Drive 认证
- 环境验证
- `hydrofetch` 与 Dashboard 启动方式
- 添加新的采集运行项目
- 向 monorepo 新增代码项目

## 1. 首次安装

在仓库根目录执行：

```bash
uv sync --all-packages --group dev
cd packages/hydrofetch-dashboard-web && npm install
```

如果只开发 Python 部分，也可以按包安装：

```bash
uv sync --package hydrofetch --group dev
uv sync --package hydrofetch-dashboard-api --group dev
uv sync --package lakeanalysis --group dev
```

## 2. 环境文件准备

```bash
cp packages/hydrofetch/.env.example packages/hydrofetch/.env
cp packages/lakeanalysis/.env.example packages/lakeanalysis/.env
```

`packages/hydrofetch/.env` 至少需要确认：

```bash
HYDROFETCH_GEE_PROJECT=your-gee-project-id
HYDROFETCH_CREDENTIALS_FILE=~/.hydrofetch/credentials.json
HYDROFETCH_TOKEN_FILE=~/.hydrofetch/token.json
```

说明：

- `HYDROFETCH_CREDENTIALS_FILE` 是 Google Cloud Console 下载的 OAuth Desktop App 凭证文件。
- `HYDROFETCH_TOKEN_FILE` 是 `hydrofetch auth` 完成后保存的 Google Drive token。

## 3. 首次认证

先做 Earth Engine 认证，再做 Drive 认证：

```bash
earthengine authenticate
uv run --package hydrofetch hydrofetch auth
```

如果本机需要显式指定凭证位置，确保环境变量已生效：

```bash
HYDROFETCH_CREDENTIALS_FILE=~/.hydrofetch/credentials.json
HYDROFETCH_TOKEN_FILE=~/.hydrofetch/token.json
uv run --package hydrofetch hydrofetch auth
```

## 4. 如何验证

### 4.1 验证外部依赖连通性

```bash
uv run --package hydrofetch python packages/hydrofetch/scripts/check_google_connectivity.py
```

常见成功信号：

- `Earth Engine: OK`
- `Google Drive: OK`

### 4.2 验证 Python 项目

```bash
uv run --package hydrofetch pytest packages/hydrofetch/tests
uv run --package hydrofetch pylint packages/hydrofetch/src/hydrofetch

uv run --package lakeanalysis pytest packages/lakeanalysis/tests
uv run --package lakeanalysis pylint packages/lakeanalysis/src/lakeanalysis
```

### 4.3 验证 Dashboard 前端

```bash
npm --prefix packages/hydrofetch-dashboard-web run build
npm --prefix packages/hydrofetch-dashboard-web run lint
```

### 4.4 验证 Dashboard API

启动后访问：

```bash
curl http://127.0.0.1:8050/api/health
curl http://127.0.0.1:8050/api/projects
```

## 5. 如何启动

### 5.1 直接启动 `hydrofetch`

```bash
uv run --package hydrofetch hydrofetch era5 \
  --start 2020-01-01 \
  --end 2020-02-01 \
  --tile-manifest data/continents/continents_manifest.json \
  --output-dir ./results \
  --run
```

查看运行状态：

```bash
uv run --package hydrofetch hydrofetch status
uv run --package hydrofetch hydrofetch status --verbose
```

### 5.2 启动 Dashboard

一键启动：

```bash
bash scripts/start_dashboard.sh
```

访问地址：

- 前端：`http://localhost:5170`
- 后端：`http://127.0.0.1:8050`
- API 文档：`http://127.0.0.1:8050/docs`

分开启动：

```bash
uv run --package hydrofetch-dashboard-api python -m hydrofetch_dashboard_api.main
cd packages/hydrofetch-dashboard-web && npm run dev
```

## 6. 如何添加新的采集运行项目

这里的“项目”指 Dashboard 管理的一组独立抓取任务。

### 6.1 通过前端添加

1. 启动 Dashboard。
2. 在页面中点击“新建 GEE 项目”。
3. 填写 `project_name`、`gee_project`、`credentials_file`、`start_date`、`end_date`、`max_concurrent`。
4. 选择“仅创建”或“创建并启动”。

创建后，后端会自动生成：

```text
data/projects/<project_id>/
├── config.json
├── jobs/
├── logs/
├── raw/
└── sample/
```

同时该项目会使用独立的 `token.json`，避免与其他项目互相覆盖。

### 6.2 通过 API 添加

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

后续控制：

```bash
curl -X POST http://127.0.0.1:8050/api/projects/north-america-2020/start
curl -X POST http://127.0.0.1:8050/api/projects/north-america-2020/stop
curl -X DELETE http://127.0.0.1:8050/api/projects/north-america-2020
```

## 7. 如何向 monorepo 新增代码项目

如果你说的“添加项目”是新增一个包或应用，建议按下面流程：

1. 在 `packages/` 下创建新目录。
2. 新建对应包清单：
   - Python 项目：`pyproject.toml`
   - 前端项目：`package.json`
3. 补齐 `README.md`、源码目录和测试目录。
4. 如果是 Python workspace 包，把路径加入根级 `pyproject.toml` 的 `[tool.uv.workspace].members`。
5. 如需根级命令或统一入口，更新根 `README.md` 和本目录运行手册。
6. 运行以下命令验证 workspace 仍可解析：

```bash
uv sync --all-packages --group dev
```

## 8. 常见问题

### `hydrofetch auth` 失败

优先检查：

- `HYDROFETCH_CREDENTIALS_FILE` 是否存在且为 OAuth Desktop App JSON
- 浏览器授权流程是否已完成
- `HYDROFETCH_TOKEN_FILE` 父目录是否可写

### Dashboard 无法启动项目

优先检查：

- `HYDROFETCH_TILE_MANIFEST` 是否存在
- `credentials_file` 是否是后端机器上的绝对路径
- 数据库变量是否已在 `packages/hydrofetch/.env` 中配置

### 前端能打开但没有数据

优先检查：

- 后端是否运行在 `127.0.0.1:8050`
- 浏览器网络面板中 `/api` 请求是否成功
- `data/projects/` 或单项目 job 目录下是否已有任务 JSON
