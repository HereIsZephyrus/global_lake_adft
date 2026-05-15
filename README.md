# global_lake_adft

`global_lake_adft` 现已收敛为以 `lakeanalysis` 为核心的分析仓库。

当前正式维护的代码包只有：

- `lakeanalysis`：湖泊分析、统计建模、数据库访问与下游研究脚本

## 仓库结构

```text
global_lake_adft/
├── data/                        # 本地/远端共享输入数据（不纳入版本控制）
├── docs/
│   ├── architecture/            # 仓库结构与边界说明
│   └── research/                # 研究方法与算法说明
├── packages/
│   └── lakeanalysis/
├── pyproject.toml               # uv workspace 定义
└── uv.lock
```

## 安装

以下命令默认在仓库根目录执行：

```bash
uv sync --package lakeanalysis --group dev
```

## 环境配置

```bash
cp packages/lakeanalysis/.env.example packages/lakeanalysis/.env
```

常用变量：

```bash
ALTAS_DB=
SERIES_DB=
DB_USER=
DB_PASSWORD=
DB_HOST=
DB_PORT=
```

## 验证

```bash
uv run --package lakeanalysis pytest packages/lakeanalysis/tests
uv run --package lakeanalysis pylint packages/lakeanalysis/src/lakeanalysis
```

## HPC 同步约定

- 代码仓库 `global_lake_adft/` 只通过 Git 同步
- 共享输入 `data/` 通过 `bash scripts/rsync_hpc.sh --push-data|--pull-data` 在本地与 HPC 仓库内同步
- HPC 作业脚本 `lsf/` 与 HPC 仓库外 `lsf/` 通过 `bash scripts/rsync_hpc.sh --push-lsf|--pull-lsf` 同步
- 结果目录 `output/` 按需通过 `bash scripts/rsync_hpc.sh --pull-output [--filter ...]` 回传

这套约定避免用 rsync 覆盖 Git 工作区，减少 HPC 本地改动、缓存文件和结果文件混入代码仓库的风险。

## 文档入口

- `packages/lakeanalysis/README.md`：包级使用说明
- `docs/architecture/repository-layout.md`：仓库结构
- `docs/architecture/package-boundaries.md`：包边界与数据边界
- `docs/research/README.md`：研究型文档索引
