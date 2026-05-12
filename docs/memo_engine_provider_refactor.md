# Engine / Provider 重构 Memo

> 日期: 2026-05-07
> 背景: 当前 `lakeanalysis` 中仅 `quantile / pwm / eot / comparison` 主链路较完整地接入了 batch engine；`quality`、`entropy`、`interpolation`、`artificial/*` 仍存在直接依赖 `lakesource.postgres` 的实现。需要先完成架构收敛，再继续业务开发。

---

## 1. 已确认的架构原则

### 1.1 Provider 职责

`Provider` 属于 `lakesource`，只负责后端 IO 能力，不负责任务工作流。

Provider 应负责：

- PostgreSQL / Parquet 的读写实现
- `.env` / `SourceConfig` / DB 连接参数解析
- `SERIES_DB` / `ALTAS_DB` 等后端细节管理
- 双库访问封装在 `LakePostgresqlProvider` 内部
- 基础表和结果表的通用读写能力

Provider 不负责：

- chunk 切分
- done / resume 语义
- task-specific schema ensure / reset 策略
- read -> compute -> persist 主循环

### 1.2 Reader / Writer 职责

`Reader` / `Writer` 属于 `lakeanalysis` 的 task 适配层，应复用统一 engine。

Reader 应负责：

- chunk / id-batch 读取策略
- task-specific done 语义
- task-specific resume 语义
- 组织 task 运行所需的输入数据

Writer 应负责：

- task-specific schema ensure
- task-specific reset 策略
- task-specific persist 规则
- 输出表拆分和写入顺序

### 1.3 Engine 职责

`lakeanalysis.batch.engine` 是统一执行框架，不应只服务 `quantile / pwm / eot`。

Engine 应负责：

- 单进程 / MPI 执行
- read -> compute -> persist 主循环
- worker / manager 调度
- 通用 chunk 和 id-batch 执行骨架

Engine 不应负责：

- 具体 task 的 IO 规则
- 具体 task 的 done 语义
- 具体 task 的结果表结构

---

## 2. 目标状态

所有 analysis 任务最终都收敛到同一模式：

```python
provider = create_provider(source_config)
reader = TaskReader(provider, ...)
writer = TaskWriter(provider, ...)
calculator = TaskCalculator(...)
engine = Engine(reader, writer, calculator, ...)
engine.run()
```

约束：

- `lakeanalysis` 不再直接 import `lakesource.postgres`
- `lakeanalysis` 不再直接使用 `series_db` / `atlas_db`
- task-specific parquet 特化逻辑统一回收到 task reader/writer
- `batch/io.py` 最终只保留协议层，不再承载具体算法硬编码

---

## 3. 当前问题清单

### 3.1 仍直接依赖 postgres 的 analysis 代码

至少包括：

- `packages/lakeanalysis/src/lakeanalysis/quality/runner.py`
- `packages/lakeanalysis/src/lakeanalysis/quality/maintenance_runner.py`
- `packages/lakeanalysis/src/lakeanalysis/entropy/runner.py`
- `packages/lakeanalysis/src/lakeanalysis/artificial/similarity/runner.py`
- `packages/lakeanalysis/src/lakeanalysis/artificial/impact/runner.py`
- `packages/lakeanalysis/src/lakeanalysis/artificial/pfaf/runner.py`
- `packages/lakeanalysis/src/lakeanalysis/artificial/pfaf/nearest_runner.py`
- `packages/lakeanalysis/src/lakeanalysis/artificial/fetch.py`
- `packages/lakeanalysis/src/lakeanalysis/quality/interpolation_runner.py`（读已部分走 parquet/provider，但写路径仍未统一）

### 3.2 batch/io 当前处于过渡态

`packages/lakeanalysis/src/lakeanalysis/batch/io.py` 目前同时承担：

- 抽象协议层
- Postgres / Parquet backend 适配
- `quantile / pwm / eot / comparison` 的 task-specific 硬编码

当前硬编码迹象包括：

- `fetch_done_ids(self, algorithm, ...)`
- `ensure_schema(self, algorithm)`
- `_APPEND_ONLY_TABLES`
- `_get_upsert_fn()` 中的算法表映射

这不利于把 engine 推广到 `quality`、`entropy`、`interpolation`、`artificial/*`。

### 3.3 quality 当前是最直接的痛点

`quality` 现在没有接入统一 engine，且当前业务正需要：

- 使用本地 parquet 全量跑 `run_quality`
- 后续继续推进 `shift` 的 maintenance / parquet / HPC 路径

因此 `quality` 应作为第一优先级迁移对象。

---

## 4. 已确认的 task-specific 语义归属

### 4.1 quality done 语义

当前 `quality` 的 done 语义依赖 `area_processed`。

重构后该语义应由 `quality reader` 负责，不应泄漏到 runner，也不应放进 provider 任务层逻辑。

### 4.2 parquet 模式下的 done / resume

这不是 provider 的职责问题，而是 task reader / writer 需要定义清楚的工作流语义问题。

例如 `quality` 需要明确：

- done 的判断依据是什么
- reset 如何执行
- parquet 结果如何安全 resume
- 半写入状态如何识别和修复

### 4.3 双库访问

`pfaf` 等依赖双库的场景，由 `LakePostgresqlProvider` 内部处理。

analysis 侧只声明读取能力，不感知 `series_db` / `atlas_db`。

### 4.4 interpolation 的 parquet 特化

`interpolation_runner` 现有 parquet 读取逻辑不能继续作为旁路保留；应抽回到统一的 task reader / writer + engine 体系中。

---

## 5. 分阶段重构计划

### Phase 1: 固化边界，整理抽象

目标：先把 provider / reader / writer / engine 的边界在代码结构中固定下来。

任务：

1. 评估 `lakesource.provider.base.LakeProvider` 的现有接口，补齐 analysis 通用 IO 能力
2. 明确 `LakePostgresqlProvider` / `LakeParquetProvider` 只做 IO，不引入 task-specific 工作流逻辑
3. 将 `batch/io.py` 的职责收缩为协议层，准备把 task-specific 逻辑外移
4. 列出所有现存 runner 所需的 provider 能力矩阵

验收：

- 抽象边界文档化
- 后续迁移不再引入新的 `lakesource.postgres` 直连用法

### Phase 2: quality 接入 engine

目标：把 `quality` 主链路迁到统一 engine，解锁 parquet 全量运行。

新增组件：

- `quality.reader`
- `quality.writer`
- `quality.calculator`（必要时为现有逻辑提供 engine 适配层）

reader 负责：

- chunk / id-batch 读 `lake_area`
- 读取 `anomaly`
- 读取 `lake_info` 中 atlas area
- 定义 `area_processed` 基础上的 done 语义

writer 负责：

- ensure / reset `area_quality`、`area_anomalies`
- 写入上述结果表
- 视实现状态决定是否同步纳入 `area_shift_labels`

runner 目标：

- `run_quality.py` 改为 provider + reader/writer + engine 组装模式
- 移除 `quality/runner.py` 对 `lakesource.postgres` 的直接依赖

验收：

- `run_quality` 支持 postgres backend
- `run_quality` 支持 parquet backend
- `lakeanalysis.quality` 不再直接使用 `series_db`

### Phase 3: quality maintenance 收敛

目标：先允许 `maintenance_runner` 保留专用 runner，但必须复用 provider + quality reader/writer 思路。

约束：

- 暂不强制其完全 engine 化
- 但不允许继续直接 import `lakesource.postgres`

迁移内容：

- `recompute_shift` 读取路径改走 provider
- 结果写入改走 quality writer 或 maintenance 专用 writer
- `area_shift_labels` 的写入语义与主 quality 输出保持一致

验收：

- `quality/maintenance_runner.py` 不再直接依赖 postgres helper

### Phase 4: entropy 接入 engine

目标：将 `entropy` 从自定义 chunk loop 迁到统一 engine。

新增组件：

- `entropy.reader`
- `entropy.writer`
- `entropy.calculator`

需要处理：

- seasonal amplitude 读取
- 中间 chunk parquet 输出是否保留，若保留则纳入 writer 语义
- entropy done 语义如何定义

验收：

- `entropy/runner.py` 不再直接依赖 postgres helper

### Phase 5: interpolation 接入 engine

目标：回收已存在的 parquet 特化读取逻辑，统一到 engine。

新增组件：

- `interpolation.reader`
- `interpolation.writer`
- `interpolation.calculator`

要求：

- 不保留单独旁路
- 继续支持 parquet backend
- DB 写入路径通过 writer 统一处理

验收：

- `interpolation_runner.py` 不再直接操作 `series_db`

### Phase 6: artificial/* 接入 provider + engine 体系

目标：迁移剩余 direct-postgres 的 artificial 相关逻辑。

范围：

- `artificial/similarity/runner.py`
- `artificial/impact/runner.py`
- `artificial/pfaf/runner.py`
- `artificial/pfaf/nearest_runner.py`
- `artificial/fetch.py`

要求：

- 双库细节只留在 `LakePostgresqlProvider`
- analysis 层不再区分 `series_db` / `atlas_db`

### Phase 7: 清理 batch/io 中的算法硬编码

目标：让 engine 成为真正通用框架。

处理方向：

1. `batch/io.py` 保留 `BatchReader` / `BatchWriter` 协议
2. 将 task-specific `ensure_schema` / `persist` / `done_ids` 逻辑迁入各 task reader/writer
3. 删除或瘦身 `_APPEND_ONLY_TABLES`、`_get_upsert_fn()`、算法表映射等硬编码
4. 已接入 engine 的 `quantile / pwm / eot / comparison` 逐步迁到 task-specific reader/writer

验收：

- engine 不再隐式偏向少数算法
- 所有 task 使用统一骨架

---

## 6. 迁移优先级

按业务与架构收益排序：

1. `quality`
2. `quality maintenance_runner`
3. `entropy`
4. `interpolation`
5. `artificial/*`
6. 清理 `quantile / pwm / eot / comparison` 的遗留硬编码

---

## 7. 实施约束

- 在重构完成前，不继续扩散新的 direct-postgres runner 实现
- 优先做最小正确迁移，不引入多余兼容层
- provider 保持 IO 边界，不把 task 工作流塞回 `lakesource`
- maintenance runner 允许暂时不完全 engine 化，但必须停止直接依赖 postgres helper

---

## 8. 下一步执行建议

建议从 `quality` 开始，按下面顺序落地：

1. 盘点 `quality` 所需 provider 能力
2. 设计 `quality.reader` / `quality.writer` 接口
3. 将 `run_quality.py` 切到 engine
4. 验证 postgres backend
5. 验证本地 parquet backend
6. 再迁 `maintenance_runner`

这一步完成后，再继续 `shift` 的 parquet / HPC 业务路径。
