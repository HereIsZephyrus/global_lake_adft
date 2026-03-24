# lakeanalysis

`lakeanalysis` 是 monorepo 中负责湖泊分析、建模和数据库访问的 Python package。

## Quick Start

以下命令从仓库根目录执行：

```bash
uv sync --package lakeanalysis --group dev
uv run --package lakeanalysis pytest packages/lakeanalysis/tests
uv run --package lakeanalysis pylint packages/lakeanalysis/src/lakeanalysis
```

## Environment Variables

复制 `packages/lakeanalysis/.env.example` 到本地 `.env`，然后填入数据库连接与日志相关配置。

## Documentation

- 包级结构说明见 `packages/lakeanalysis/docs/refactor-plan.md`
- 研究型方法文档已统一收敛到根级 `docs/research/`
- `geeconnect` 退役迁移说明见 `docs/migration/geeconnect-to-hydrofetch.md`

## Package Layout

```text
packages/lakeanalysis/
├── docs/
├── src/lakeanalysis/
├── scripts/
├── tests/
└── .env.example
```
