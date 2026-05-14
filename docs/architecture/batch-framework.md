# Batch 计算框架

`lakeanalysis.batch` 是当前湖泊算法的统一批处理执行框架，支持单进程和 MPI 两种运行模式。

当前实现遵循两个核心原则：

- **dataset-first**：`Calculator` 的唯一公开入口是 `run_dataset(dataset)`
- **quality-domain first**：批处理的候选湖泊全集来自 `area_quality.hylak_id`

也就是说，Engine 不是先按连续 ID 空间切块，再在后面“自然过滤”；而是先确定真正允许计算的湖泊集合，再对这批 ID 做 chunking 和调度。

## 当前组件分层

- `Engine`：运行入口，负责候选 ID 解析、query 构建、单机/MPI 路由
- `SingleProcessLakeDatasetRunner`：单进程 dataset 执行器
- `Manager` / `Worker`：MPI 调度和执行
- `LakeDatasetFactory`：按 query 读取源数据并物化 `LakeDataset`
- `Calculator`：逐湖计算逻辑与结果序列化
- `BatchReader` / `BatchWriter`：批处理读写语义

## 架构概览

```text
scripts / CLI
  -> SourceConfig
  -> CalculatorFactory.create(...)
  -> LakeDatasetFactory.from_config(...)
  -> build_provider_batch_reader / writer
  -> Engine.run()

Engine
  -> _resolve_candidate_ids()
      1. reader.fetch_quality_ids()            # area_quality domain
      2. apply lake_filter                     # RangeFilter / IdSetFilter
      3. subtract done_ids                     # resume / skip completed
  -> _build_queries()
      -> chunk exact sorted IDs into id_subset batches
  -> single process OR MPI

Single process
  -> SingleProcessLakeDatasetRunner
  -> LakeDatasetFactory.build(query)
  -> Calculator.run_dataset(dataset)
  -> writer.persist(rows)

MPI
  rank 0 -> Manager
      -> assign id_subset batches to workers
      -> throttle read IO with io_budget
      -> collect TAG_DATA rows and flush centrally
  rank 1+ -> Worker
      -> wait for TRIGGER_READ
      -> LakeDatasetFactory.build(query)
      -> Calculator.run_dataset(dataset)
      -> send rows back to rank 0
```

## 计算域定义

当前 batch engine 的计算域定义为：

```text
U = area_quality.hylak_id
```

然后依次应用：

1. `lake_filter`
2. 当前算法的 `done_ids`
3. chunk 分片

因此 Engine 中真正进入调度的 ID 集合是：

```text
effective_ids = sort((area_quality_ids ∩ user_filter) - done_ids)
```

这个定义同时适用于：

- 单进程 `_build_queries()`
- MPI rank 0 的 worker assignment

这样可以保证：

- `total_chunks` 反映真实有效湖泊集合，而不是连续 ID 空间
- 稀疏 ID 集不会被退化成巨大 range
- 单进程和 MPI 的 chunk 语义一致

## 关键数据对象

### LakeDatasetQuery

`LakeDatasetQuery` 是 worker / runner 读取数据的结构化请求。

当前 batch engine 的主路径使用：

- `id_subset`: 精确的一批 `hylak_id`
- `algorithm`: 用于算法上下文
- `require_quality=False`: 因为进入 query 之前已经限定在 quality domain
- `exclude_done=False`: 因为 done 过滤已经在 Engine 中提前完成

`id_range` 仍然保留在数据结构中，但当前 Engine 主路径不再用它做真实分片表达。

### LakeDataset

`LakeDataset` 是 worker 侧的稠密内存表示，包含：

- `hylak_ids: np.ndarray`
- `year_months: np.ndarray`
- `values: np.ndarray`
- `frozen_mask: np.ndarray | None`
- `extra: dict[str, np.ndarray] | None`

它是 `LakeDatasetFactory.build(query)` 的输出，也是 `Calculator.run_dataset()` 的输入。

### LakeTask

`LakeTask` 是单湖任务对象，由 `LakeDataset.to_task(idx)` 按需构造。它不是公共调度入口，而是 `Calculator._compute_lake()` 的内部输入。

字段包括：

- `hylak_id`
- `series_df`
- `frozen_year_months`
- `extra`

## 数据流

### 1. CLI / 脚本层

CLI 或脚本通过 `run_batch_engine(...)` 或显式组装方式构造：

- `SourceConfig`
- `Calculator`
- `LakeDatasetFactory`
- `BatchReader`
- `BatchWriter`
- `Engine`

### 2. Engine 解析候选 ID

`Engine._resolve_candidate_ids()` 的流程是：

1. `reader.fetch_quality_ids()` 读取 `area_quality` 的 `hylak_id`
2. 对这批 ID 应用 `RangeFilter` 或 `IdSetFilter`
3. 调用 `reader.fetch_done_ids(...)` 排除已完成湖泊
4. 返回排序后的 `list[int]`

这个阶段完成后，真正的计算域已经固定。

### 3. Engine 构建 query

`Engine._build_queries()` 会对 `sorted_ids` 调用 `_iter_id_batches(...)`，为每个 batch 生成：

```python
LakeDatasetQuery(
    algorithm=self._algorithm,
    id_subset=frozenset(id_batch),
    require_quality=False,
    exclude_done=False,
)
```

这表示 query 已经是“精确 ID 子集”，不需要在 factory 内再做质量筛选或 done 筛选。

### 4. LakeDatasetFactory 物化数据集

`LakeDatasetFactory.build(query)` 负责：

1. 解析 query 中的 `id_subset` / `id_range`
2. 读取 `lake_area` 中对应湖泊的时序数据
3. 读取 frozen year-month 信息
4. 读取 `fields` 指定的附加字段，例如 `atlas_area`
5. 组装成 `LakeDataset`

注意：

- **计算域定义不在 factory 中完成**
- factory 的职责是“按给定 query 取数并物化数据集”

### 5. Calculator 执行

`Calculator` 的唯一公开入口是：

```python
run_dataset(dataset) -> (rows_by_table, success_lakes, error_lakes)
```

基类默认实现会：

1. 遍历 `dataset`
2. 调用 `dataset.to_task(idx)` 构造 `LakeTask`
3. 调用子类实现的 `_compute_lake(task)`
4. 用 `result_to_rows(...)` 转成写入行
5. 如有异常，用 `error_to_rows(...)` 生成错误行

也就是说，当前架构中：

- `run_dataset()` 负责通用的逐湖循环
- `_compute_lake()` 负责具体算法

## 单进程路径

单进程路径由 `SingleProcessLakeDatasetRunner` 执行：

1. `writer.ensure_schema(algorithm)`
2. `dataset_factory.build(query)`
3. `calculator.run_dataset(dataset)`
4. `writer.persist(rows_by_table)`
5. 汇总 `RunReport`

当前单进程只有这一个 runner，不再区分早期的 range runner / id-batch runner。

## MPI 路径

### Manager

`Manager` 只运行在 rank 0，负责：

- 把 `sorted_ids` 切成 worker 级别的 `id_subset` queries
- 用 `io_budget` 限制同时进入读取阶段的 worker 数量
- 接收 worker 的 `TAG_STATUS` 和 `TAG_DATA`
- 聚合结果并批量 `flush`
- 汇总 `RunReport`

`Manager._assign_dataset_queries()` 保留稀疏批次的精确 `id_subset`，不会把 `{2, 100, 300}` 这种集合扩张成 `(2, 301)`。

### Worker

`Worker` 只运行在 rank 1+，负责：

1. 等待 `TRIGGER_READ`
2. `factory.build(query)` 读取自己的 dataset
3. `calculator.run_dataset(dataset)` 执行计算
4. 把 `rows_by_table` 通过 `TAG_DATA` 发给 rank 0
5. 报告状态和统计信息

### 状态机

```text
PENDING -> READING -> CALCULATING -> PENDING -> ... -> DONE
```

其中：

- `PENDING`：worker 已空闲，等待被分配下一次读机会
- `READING`：正在执行数据读取
- `CALCULATING`：数据已到内存，正在计算
- `DONE`：该 worker 的所有 query 已完成

Manager 只对 **read IO** 做预算控制，写 IO 始终集中在 rank 0。

## BatchReader / BatchWriter

### BatchReader

当前和 Engine 强相关的读取能力包括：

- `fetch_quality_ids()`
- `fetch_done_ids(...)`
- `fetch_lake_area_by_ids(...)`
- `fetch_frozen_year_months_by_ids(...)`

其中：

- `fetch_quality_ids()` 定义批处理的候选 lake universe
- `fetch_done_ids(...)` 支持断点续跑

### BatchWriter

`BatchWriter` 负责：

- `ensure_schema(algorithm)`
- `persist(rows_by_table)`
- `truncate_run_status(algorithm)`

`truncate_run_status()` 只在 **全量运行**（`lake_filter is None`）后触发，用于清空增量状态表。

## 为什么不再按连续 range 分片

旧思路是对 `0..max_hylak_id` 做区间切块，然后在后续读取中自然过滤掉无效湖泊。

当前实现不再这样做，原因是：

- 连续 range 会产生大量空 chunk 或稀疏 chunk
- `total_chunks` 和真实工作量不一致
- MPI 会把无效范围分配给 worker
- 稀疏 quality IDs 会被扩张成巨大区间，造成额外读放大

当前改成 quality-domain first 后：

- chunk 数量更真实
- 调度更稳定
- 单机与 MPI 一致性更强

## 扩展约定

### 添加新算法

1. 实现 `Calculator` 子类
2. 实现 `_compute_lake()`、`result_to_rows()`、`error_to_rows()`
3. 注册到 `CalculatorFactory`
4. 通过脚本或 CLI 组装到 `Engine`

### 添加新字段

如果某算法需要附加字段，例如 `atlas_area`：

1. 在 query 中通过 `fields` 声明
2. 在 `LakeDatasetFactory._materialize_extra()` 中补充读取逻辑
3. 在 `LakeTask.extra` 中消费

### 调整计算域

如果未来需要改变候选 lake universe，不应从 calculator 或 factory 临时绕过；应优先调整：

1. `BatchReader.fetch_quality_ids()` 或其上游语义
2. `Engine._resolve_candidate_ids()`
3. 对应文档与测试

这样才能保证 query、调度、统计和计算域始终一致。
