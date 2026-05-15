# lakeanalysis

`lakeanalysis` 是当前仓库的核心 Python package，用于湖泊分析、统计建模、数据库访问和研究型脚本执行。

## 快速开始

以下命令从仓库根目录执行：

```bash
uv sync --package lakeanalysis --group dev
uv run --package lakeanalysis pytest packages/lakeanalysis/tests
uv run --package lakeanalysis pylint packages/lakeanalysis/src/lakeanalysis
```

## CLI（`lake_adft`）

本包提供统一的命令行入口 `lake_adft`，基于 typer 构建，包含 10 个子命令组：

| 子命令组 | 说明 |
|----------|------|
| `quality` | 质量过滤与指标计算 |
| `hawkes` | Hawkes 过程建模与挖掘 |
| `pwm` | PWM 极值估计与事件识别 |
| `eot` | 极值阈值检验 |
| `entropy` | 熵分析流水线 |
| `comparison` | 方法对比与全局/gt10 面板 |
| `spatial` | 空间聚合与区域统计 |
| `shift` | 突变检测与标签计算 |
| `plot` | 绘图（18 个子命令，按图形内容命名） |
| `sync` | Parquet ↔ PostgreSQL 数据同步 |

### 使用方式

```bash
# 查看所有子命令组
lake_adft --help

# 查看某个组的子命令
lake_adft plot --help

# 运行具体命令
lake_adft --filter gt10 comparison global
lake_adft --filter full hawkes mine --limit-id 100
lake_adft --filter no_pwm_err eot run --chunk-size 5000
```

如果未安装到 PATH，可通过 `uv run` 调用：

```bash
uv run --package lakeanalysis lake_adft --help
```

## 环境变量

复制 `packages/lakeanalysis/.env.example` 到本地 `.env`，然后填入数据库连接与日志相关配置。

```bash
cp packages/lakeanalysis/.env.example packages/lakeanalysis/.env
```

当前示例变量包括：

```bash
ALTAS_DB=
SERIES_DB=
DB_USER=
DB_PASSWORD=
DB_HOST=
DB_PORT=
PARQUET_DATA_DIR=
```

其中：

- `ALTAS_DB` 用于湖泊几何或基础资料查询
- `SERIES_DB` 用于时序结果消费
- `DB_HOST`、`DB_PORT` 为空时会回退到默认本地连接
- `PARQUET_DATA_DIR` 用于共享 Parquet 输入根目录，默认读取仓库内 `data/`

## HPC 同步边界

- 代码 checkout 走 Git，不要用 rsync 覆盖 HPC 工作区
- 共享输入通过 `bash scripts/rsync_hpc.sh --push-data|--pull-data` 在本地与 HPC 仓库内 `data/` 间同步
- `lsf/` 通过 `bash scripts/rsync_hpc.sh --push-lsf|--pull-lsf` 与 HPC 仓库外脚本目录同步
- 结果目录通过 `bash scripts/rsync_hpc.sh --pull-output [--filter full|gt10|no_pwm_err]` 回传

## Batch 框架

`lakeanalysis.batch` 提供 MPI 分布式批处理能力，支持四种算法：

| 算法 | 脚本 / Calculator | 说明 |
|------|-------------------|------|
| Quantile | `scripts/run_quantile.py` | 月距平分位数极端事件识别 |
| PWM Extreme | `scripts/run_pwm_extreme.py` | PWM 极值阈值估计 |
| EOT | `scripts/run_eot.py` | 极值阈值检验 |
| EOT Hawkes | `EOTHawkesCalculator` (via CLI) | EOT + Hawkes 联合批处理 |

### 单进程运行

```bash
python scripts/run_quantile.py --chunk-size 10000
```

### MPI 分布式运行

```bash
mpiexec -np 4 python scripts/run_quantile.py --chunk-size 10000
mpiexec -np 64 --hostfile hosts.txt python scripts/run_quantile.py --chunk-size 10000
```

### 参数说明

| 参数 | 说明 |
|------|------|
| `--chunk-size` | 每个 chunk 的 hylak_id 数量 |
| `--id-start` | 起始 hylak_id |
| `--id-end` | 结束 hylak_id |
| `--limit-id` | 等价于 `--id-end`（向后兼容） |
| `--io-budget` | 最大并发 DB IO 数（MPI 模式） |

### 架构

详见 `docs/architecture/batch-framework.md`。

## 包结构

```text
packages/lakeanalysis/
├ scripts/
│   run_quantile.py
│   run_pwm_extreme.py
│   run_eot.py
│   run_entropy.py
│   run_hawkes_mining.py
│   _archived/              # 已废弃脚本
├ src/lakeanalysis/
│   cli/                    # typer CLI（lake_adft 入口）
│     __init__.py
│     _common.py
│     quality.py, hawkes.py, pwm.py, eot.py, entropy.py
│     comparison.py, spatial.py, shift.py, plot.py, export.py
│   batch/
│     engine.py
│     io.py
│     manager.py
│     single_process.py
│     worker.py
│     protocol.py
│     calculator/
│       quantile.py, pwm_extreme.py, eot.py, eot_hawkes.py
│   quantile/
│   pwm_extreme/
│   eot/
│   entropy/
│     runner.py
│     service.py            # 流水线编排（从 runner 拆分）
│   hawkes/
│     mining.py             # 纯数据函数（从脚本提取）
├ tests/
│   smoke/
│   unit/
└ .env.example
```

## 文档

- `docs/architecture/batch-framework.md`：Batch 框架架构说明
- `docs/architecture/package-boundaries.md`：包边界与数据边界
- `docs/research/README.md`：研究方法与算法文档索引
