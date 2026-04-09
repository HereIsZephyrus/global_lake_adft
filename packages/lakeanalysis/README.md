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
```

其中：

- `ALTAS_DB` 用于湖泊几何或基础资料查询
- `SERIES_DB` 用于时序结果消费
- `DB_HOST`、`DB_PORT` 为空时会回退到默认本地连接

## 如何验证

```bash
uv run --package lakeanalysis pytest packages/lakeanalysis/tests
uv run --package lakeanalysis pylint packages/lakeanalysis/src/lakeanalysis
```

如果本地尚未通过 `uv sync` 安装，也可以临时用源码路径做最小验证：

```bash
PYTHONPATH=packages/lakeanalysis/src ./.venv/bin/pytest packages/lakeanalysis/tests
```

## 文档

- `packages/lakeanalysis/docs/refactor-plan.md`
- `docs/architecture/repository-layout.md`
- `docs/architecture/package-boundaries.md`
- `docs/research/README.md`

## 包结构

```text
packages/lakeanalysis/
├── docs/
├── scripts/
├── src/lakeanalysis/
├── tests/
└── .env.example
```
