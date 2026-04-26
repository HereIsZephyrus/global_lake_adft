# lakeanalysis

`lakeanalysis` 是当前仓库的核心 Python package，用于湖泊分析、统计建模、数据库访问和研究型脚本执行。

## 快速开始

以下命令从仓库根目录执行：

```bash
uv sync --package lakeanalysis --group dev
uv run --package lakeanalysis pytest packages/lakeanalysis/tests
uv run --package lakeanalysis pylint packages/lakeanalysis/src/lakeanalysis
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
LAKE_DATA_DIR=
```

其中：

- `ALTAS_DB` 用于湖泊几何或基础资料查询
- `SERIES_DB` 用于时序结果消费
- `DB_HOST`、`DB_PORT` 为空时会回退到默认本地连接
- `LAKE_DATA_DIR` 用于 Parquet 文件后端（可选）

## Batch 框架

`lakeanalysis.batch` 提供 MPI 分布式批处理能力，支持三种算法：

| 算法 | 脚本 | 说明 |
|------|------|------|
| Quantile | `scripts/run_quantile.py` | 月距平分位数极端事件识别 |
| PWM Extreme | `scripts/run_pwm_extreme.py` | PWM 极值阈值估计 |
| EOT | `scripts/run_eot.py` | 极值阈值检验 |

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
├ src/lakeanalysis/
│   batch/
│     engine.py
│     manager.py
│     worker.py
│     protocol.py
│     calculator/
│   quantile/
│   pwm_extreme/
│   eot/
├ tests/
│   smoke/
│   unit/
└ .env.example
```

## 文档

- `docs/architecture/batch-framework.md`：Batch 框架架构说明
- `docs/architecture/package-boundaries.md`：包边界与数据边界
- `docs/research/README.md`：研究方法与算法文档索引