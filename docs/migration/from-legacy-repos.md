# From Legacy Repos

`global_lake_adft` 已将原先分散维护的两个 Python 项目整合为一个 monorepo。

## What Changed

- 原顶层 `hydrofetch/` 已迁移到 `packages/hydrofetch/`
- 原顶层 `lakeanalysis/` 已迁移到 `packages/lakeanalysis/`
- 根目录新增统一的 `uv workspace` 配置与 `uv.lock`
- 开发入口从“分别进入两个仓库”调整为“在仓库根目录按 package 操作”

## What Stayed Stable

- Python import 名仍保持为 `hydrofetch` 与 `lakeanalysis`
- 两个包仍独立维护自己的依赖定义和环境变量样例
- 与包强绑定的脚本、源码和 README 仍保留在各自包目录中

## New Development Workflow

在仓库根目录执行：

```bash
uv sync --all-packages --group dev
uv run --package hydrofetch pytest packages/hydrofetch/tests
uv run --package lakeanalysis pytest packages/lakeanalysis/tests
```

## Follow-up

- 若旧仓库仍对外开放，建议在其 README 顶部标明已迁移到 monorepo
- 若后续需要包间 Python 级依赖，应通过 workspace source 显式声明，而不是依赖隐式相对路径
