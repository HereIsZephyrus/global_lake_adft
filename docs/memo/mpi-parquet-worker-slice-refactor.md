# MPI Parquet Worker Slice Refactor Memo

## Goal

- 删除 `io_budget` 及其 query 级 IO 调度机制
- 明确 MPI 只支持 parquet backend 的高效执行路径
- 将 manager → worker 的协议从 `list[LakeDatasetQuery]` 改为 `worker slice`
- 让每个 worker 在启动后只做一次 IO / preload
- 将 `query` 降级为 worker 内部的纯计算分块概念
- 为 quantile/pwm/eot batch 主路径补齐可观测的日志和计时
- 保持 smoke 输出与生产 `full` 结果物理隔离

## Problem Summary

当前实现的性能瓶颈不在算法计算，而在 dataset 生命周期设计：

1. manager 先把全局候选湖切成很多 `LakeDatasetQuery`
2. worker 接收的是 `list[LakeDatasetQuery]`
3. worker 每处理一个 query 都重新 `factory.build(query)`
4. `build(query)` 触发新的 provider 读取和 dataset 组装
5. 在小 `chunk_size` + 多 worker 下，读放大远大于算放大

运行日志已经证明这一点：

- `calc` 常见耗时：`0.01s - 0.08s`
- `read` 常见耗时：`110s - 140s`

这说明当前系统几乎在做：

- 花两分钟读 3 个湖
- 再花几十毫秒算 3 个湖

该模型在 parquet-only 模式下没有保留价值。

## Current Architecture

### Manager Side

当前 manager 职责包括：

- 解析全局 candidate ids
- 应用 quality / done / range / filter
- 按 `chunk_size` 切成 `id_batches`
- 将 `id_batches` 转为 `LakeDatasetQuery`
- 将 query 分配给 worker
- 通过 `io_budget` 对 worker 进入 `READING` 做节流
- 汇总 worker 状态与结果并统一 flush

### Worker Side

当前 worker 流程：

1. 收到 manager 分配的一组 query
2. 等待 `TRIGGER_READ`
3. `factory.build(query)`
4. `calculator.run_dataset(dataset)`
5. 发回结果
6. 进入下一个 query

### Data Lifecycle

当前是：

- `query-scoped transient dataset`

而不是：

- `worker-scoped resident dataset slice`

## Target Architecture

### Core Principle

每个 worker 在启动后只做一次 IO。

manager 不再把 query 当作协议层对象下发，而只下发：

- 该 worker 负责的完整 `id_subset`

worker 拿到这个 subset 后：

1. 一次性 preload 所有输入数据
2. 在本地构建 worker-scoped resident slice
3. 再根据 `chunk_size` 在内存里切 query
4. query 只负责计算粒度，不再负责读取粒度

### New Role Split

#### Manager

manager 只负责：

- 解析全局 candidate ids
- 应用 quality / done / range / filter
- 将最终 ids 分配成 `worker slice`
- 广播 `worker slice assignments`
- 收集 worker 数据和最终状态
- 统一 flush

manager 不再负责：

- `TRIGGER_READ`
- `READING` 许可发放
- `_io_budget`
- `_io_active`
- `_read_queue`
- `_queued_workers`
- query 级 IO 调度

#### Worker

worker 负责：

- 接收自己的 worker slice
- 一次性 preload 该 slice 的全部湖数据
- 在内存里构建 resident cache / slice
- 在本地把 worker slice 切成 query
- 遍历 query 做纯内存 dataset materialization + compute
- 汇总并发送结果

### Query Semantics

重构后应明确：

- 一个 query 不是一个湖
- 一个 query 是 worker 内部根据 `chunk_size` 切出的一个小 batch
- query 不再通过 manager 下发
- query 只存在于 worker 内部

### Dataset Semantics

重构后应明确：

- 一个 worker 只对应一个大的 resident dataset slice
- query 只是这个 resident slice 的一个逻辑子集
- worker 不再为每个 query 重新访问 provider

## Protocol Redesign

### Old Protocol

manager 广播：

```python
dict[int, list[LakeDatasetQuery]]
```

worker 处理：

- 收到一组 query
- query 级 wait/read/build/calc

### New Protocol

manager 广播：

```python
dict[int, frozenset[int]]
```

或等价的：

```python
dict[int, WorkerSliceAssignment]
```

其中 `WorkerSliceAssignment` 至少应包含：

- `id_subset: frozenset[int]`
- 可选的元信息：`algorithm`, `chunk_size`

worker 收到后：

1. preload `id_subset`
2. 本地按 `chunk_size` 切 query
3. 本地循环计算

## Factory / Dataset Refactor

### Problem in Current Factory

当前 `LakeDatasetFactory` 同时承担：

- 全局候选集解析
- query 级 provider 读取
- query 级 dataset materialization

这会导致：

- query 已经带 `id_subset` 时仍重复做候选集解析
- worker 很难实现 resident preload

### New Two-Stage Model

建议拆成两层：

#### Stage A: Worker Preload

新增 worker 级 preload 入口，例如：

```python
WorkerDatasetSliceFactory.preload(id_subset: set[int]) -> WorkerDatasetSlice
```

职责：

- 一次性读取全部 `id_subset` 对应的输入
- 构建 resident in-memory structures

#### Stage B: Query Materialization

在 resident slice 上提供：

```python
WorkerDatasetSlice.build_query(id_subset: set[int]) -> LakeDataset
```

职责：

- 从已 preload 的 resident data 中选子集
- 轻量 materialize `LakeDataset`
- 不再访问 provider

### Candidate Resolution Rule

重构后必须固定一条原则：

- worker 侧 `id_subset` 是 authoritative truth

因此 worker 侧不能再做：

- `_fetch_id_set(raw_source)`
- quality 再过滤
- done 再过滤

这些工作都只允许在 manager 侧发生一次。

## Provider / IO Refactor

### Parquet-Only Premise

本重构不再为 postgres backend 保留 batch/MPI 设计约束。

provider 目标改为：

- 纯只读 parquet 输入提供者
- 支撑 worker 一次性 preload

### Required Provider Capabilities

provider 应高效支持：

- `fetch_lake_area_by_ids(id_list)`
- `fetch_frozen_year_months_by_ids(id_list)`
- `fetch_atlas_area_by_ids(id_list)`
- 任何其它算法需要的批量只读接口

### Registration Strategy

provider 仍应只提供 IO，不负责调度。

但在 parquet-only 模式下，provider 应做：

- 按需注册 view
- 避免初始化时扫描整个 `data/`
- 避免空目录 warning 放大

### Output Routing

输入固定来自：

- `config.data_dir`

输出固定写到：

- `config.output_dir`

这保证：

- worker preload 只碰输入根
- manager/writer 只碰输出根

## `io_budget` Removal Plan

### Why Remove It

在 worker-once-IO 模型下，`io_budget` 不再有存在前提：

- 不再存在 query 级读取
- 不再存在 manager 侧 query 级 read gating
- 不再需要 `TRIGGER_READ` 许可机制

### Delete Scope

应删除：

- CLI `--io-budget`
- `_common.py` 中 `IoBudgetOpt`
- `run_batch_engine(... io_budget=...)`
- `Engine.__init__(..., io_budget=...)`
- `Manager.__init__(..., io_budget=...)`
- manager 内所有 read slot 字段与方法
- protocol 中与 `TRIGGER_READ` 相关的逻辑
- worker 中 `comm.recv(... TAG_TRIGGER)` 等待逻辑
- 相关测试与 smoke 参数

### Replacement

无替代参数。

如果后续确实需要控制 preload 并发，应引入一个新的、语义准确的参数，例如：

- `preload_parallelism`

但当前阶段不应保留 `io_budget` 这个旧名字和旧语义。

## Logging and Timing Plan

### Version Logging

保留：

- 单一 commitizen 版本来源

删除：

- 多包 workspace version banner

约束：

- 只允许 rank 0 打版本行一次

### Manager Logs

新增或保留：

- candidate ids resolved
- worker slice assignment summary
- flush summary
- total elapsed

### Worker Logs

新增：

- preload start
- preload done
  - worker slice size
  - preload elapsed
  - resident slice statistics
- per-query materialization done
  - query index / total
  - subset size
  - materialize elapsed
- per-query calculation done
  - success / error
  - calc elapsed
- worker done
  - total elapsed

### Timing Data

至少应记录：

- `worker_preload_seconds`
- `query_materialize_seconds`
- `query_calc_seconds`
- `worker_total_seconds`

建议放入：

- 日志
- 或单独 profiling 输出

不建议先污染正式 `run_status` schema，除非后续明确要把 timing 作为结果表的一部分。

## Smoke Isolation Plan

### Requirement

smoke 不能与生产 `full` 输出共用结果空间。

### Directory Model

建议固定：

```text
output/smoke_mpi/full/
output/prod/full/
output/prod/gt10/
```

### Meaning

- `full/gt10/no_pwm_err`：业务 filter 命名空间
- `smoke_mpi/prod/debug`：运行场景命名空间

这样既保留 filter 隔离，也避免覆盖生产 `full` 结果。

## Test Refactor Plan

### Remove Obsolete Tests

应删除或重写：

- 所有依赖 `io_budget=1` 的 no-deadlock 测试
- 所有依赖 `TRIGGER_READ` 的 worker 状态机测试
- 所有依赖旧 `_archived/run_quantile.py` 的 MPI smoke 测试

### New Test Focus

新增测试应覆盖：

1. manager 广播 worker slice，而不是 query list
2. worker preload 只执行一次
3. worker 内部 query 切分正确
4. query materialization 不再触发 provider IO
5. smoke 输出写到独立 `output/smoke_mpi/...`
6. 结果不覆盖生产 `output/full/...`

## Incremental Implementation Order

### Phase 1

先移除 `io_budget` 和 query 级 read gating：

- 删除 CLI 参数链路
- 删除 manager read-slot 调度逻辑
- 删除 worker `TRIGGER_READ` 等待

### Phase 2

将 manager 协议改成发 worker slice：

- manager 不再广播 `list[LakeDatasetQuery]`
- manager 只广播 worker slice assignment

### Phase 3

实现 worker preload：

- worker 合并全部 ids
- 一次性读取 lake_area / frozen / extra
- 构建 resident slice

### Phase 4

实现 worker 内部 query 切分：

- 由 worker 根据 `chunk_size` 在本地切 query
- query 只在 resident slice 上 materialize

### Phase 5

补全日志与 timing：

- preload elapsed
- materialize elapsed
- calc elapsed
- worker total elapsed

### Phase 6

更新 smoke 和 profiling：

- 独立 output namespace
- quantile smoke 验证一次性 IO 模型是否生效

## Acceptance Criteria

重构完成后，应满足：

1. batch/MPI 路径中不再存在 `io_budget`
2. manager 不再进行 query 级 IO 调度
3. 每个 worker 启动后只做一次 IO / preload
4. worker 后续 query 计算不再触发 provider 读取
5. worker 侧不再重复做全表候选解析
6. smoke 结果不会写入生产 `output/full/`
7. quantile 日志中能够清楚看到：
   - preload time
   - query materialize time
   - query calc time
8. `read >> calc` 的极端失衡应显著缓解

## Notes

- 本 memo 明确以 parquet-only batch/MPI 为目标，不为 postgres backend 保留架构复杂度。
- 当前最重要的重构不是继续调 `NP` 或 `chunk_size`，而是修正 worker dataset lifecycle。
- 在 worker-scoped resident slice 模型落地前，对 `NP=16` 的任何性能结论都没有代表性。
