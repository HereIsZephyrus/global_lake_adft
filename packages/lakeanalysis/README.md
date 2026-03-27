# lakeanalysis

`lakeanalysis` 是 monorepo 中负责湖泊分析、建模和数据库访问的 Python package。

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

如果需要验证与 `hydrofetch` 的协作契约，可额外查看 `tests/integration/README.md` 中约定的跨包测试范围。

## 与 `hydrofetch` 的关系

`lakeanalysis` 不负责 GEE/Drive 认证，也不直接启动抓取任务。通常工作流是：

1. 使用 `hydrofetch` 生产采样结果或入库数据。
2. 在 `lakeanalysis` 中读取数据库或中间结果做分析、统计和建模。

因此如果你在排查抓取失败、认证失败或 Dashboard 启动问题，应优先查看 `hydrofetch` 与 Dashboard 文档。

## 文档

- 包级结构说明见 `packages/lakeanalysis/docs/refactor-plan.md`
- 研究型方法文档已统一收敛到根级 `docs/research/`
- `geeconnect` 退役迁移说明见 `docs/migration/geeconnect-to-hydrofetch.md`

## 包结构

```text
packages/lakeanalysis/
├── docs/
├── src/lakeanalysis/
├── scripts/
├── tests/
└── .env.example
```
