# global_lake_adft

`global_lake_adft` 是一个基于 `uv workspace` 管理的 Python monorepo，当前统一承载两个相互协作但边界独立的包：

- `hydrofetch`: 负责 GEE 导出、Drive 下载、本地采样与结果写出。
- `lakeanalysis`: 负责湖泊分析、统计建模与下游消费。

## Repository Layout

```text
global_lake_adft/
├── docs/
│   ├── architecture/
│   ├── migration/
│   ├── research/
│   └── runbooks/
├── packages/
│   ├── hydrofetch/
│   └── lakeanalysis/
├── pyproject.toml
├── tests/
│   └── integration/
├── uv.lock
└── scripts/
```

## Quick Start

1. 安装 `uv`，并确保本地 Python 版本与根级 `.python-version` 一致。
2. 在仓库根目录同步所有 workspace 包及开发依赖：

```bash
uv sync --all-packages --group dev
```

3. 按需同步单个包：

```bash
uv sync --package hydrofetch --group dev
uv sync --package lakeanalysis --group dev
```

## Common Commands

在仓库根目录执行：

```bash
# hydrofetch tests / lint
uv run --package hydrofetch pytest packages/hydrofetch/tests
uv run --package hydrofetch pylint packages/hydrofetch/src/hydrofetch

# lakeanalysis tests / lint
uv run --package lakeanalysis pytest packages/lakeanalysis/tests
uv run --package lakeanalysis pylint packages/lakeanalysis/src/lakeanalysis
```

## Package Notes

- `packages/hydrofetch/README.md` 说明 GEE/Drive 认证、环境变量与 CLI 用法。
- `packages/lakeanalysis/README.md` 说明分析包结构、数据库环境变量与开发方式。
- `packages/lakeanalysis/docs/refactor-plan.md` 记录 `lakeanalysis` 从旧职责拆分为分析包的收敛方案。

## Migration Notes

本仓库已经从两个独立项目整合为 monorepo。迁移背景、目录布局和边界说明见：

- `docs/migration/from-legacy-repos.md`
- `docs/migration/geeconnect-to-hydrofetch.md`
- `docs/architecture/repository-layout.md`
- `docs/architecture/package-boundaries.md`
- `docs/research/README.md`