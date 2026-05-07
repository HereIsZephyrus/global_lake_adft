# Batch 计算框架

`lakeanalysis.batch` 提供统一的单进程 / MPI 批处理框架，用于按湖泊粒度执行大规模算法计算。

当前实现已经明确拆分为四层：

- `Engine`：运行模式选择与对象组装入口
- `SingleProcessRunner` / `SingleProcessIdBatchRunner`：单进程执行循环
- `Manager` / `Worker`：MPI 调度与执行
- `BatchReader` / `BatchWriter`：批处理业务 IO 语义

`lakesource.provider` 不再承担 batch 的业务语义；它只保留 backend-oriented 的共享数据访问能力。

## 架构概览

```text
┌──────────────────────────────────────────────────────────────────────┐
│                               scripts                               │
│   SourceConfig -> build_batch_reader/writer -> Engine.run()         │
└──────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────┐
│                               Engine                                │
│  负责：                                                              │
│  - 检测 MPI 环境                                                     │
│  - 选择单进程 / MPI 模式                                             │
│  - 选择 range / id-batch 模式                                        │
└──────────────────────────────────────────────────────────────────────┘
                 │                                      │
                 ▼                                      ▼
┌─────────────────────────────────┐    ┌──────────────────────────────┐
│     SingleProcessRunner         │    │           MPI Mode            │
│  / SingleProcessIdBatchRunner   │    │  rank 0 -> Manager           │
│                                 │    │  rank 1+ -> Worker           │
└─────────────────────────────────┘    └──────────────────────────────┘
                 │                                      │
                 └──────────────────┬───────────────────┘
                                    ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     BatchReader / BatchWriter                        │
│  PostgresBatchReader/Writer                                          │
│  ParquetBatchReader/Writer                                           │
└──────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌──────────────────────────────────────────────────────────────────────┐
│                            Calculator                                │
│  Quantile / PWM Extreme / EOT / Comparison                           │
└──────────────────────────────────────────────────────────────────────┘
```

## 核心组件

### Engine

入口类，负责：

- 检测 MPI 环境，决定单进程或分布式模式
- range 模式与 id-batch 模式选择
- 在单进程模式下选择 `SingleProcessRunner` 或 `SingleProcessIdBatchRunner`
- 在 MPI 模式下启动 `Manager` 或 `Worker`

```python
from lakesource.config import SourceConfig

from lakeanalysis.batch import Engine, RangeFilter, build_batch_reader, build_batch_writer
from lakeanalysis.batch.calculator import CalculatorFactory

source_config = SourceConfig()

engine = Engine(
    reader=build_batch_reader(source_config),
    writer=build_batch_writer(source_config),
    calculator=CalculatorFactory.create("quantile"),
    algorithm="quantile",
    lake_filter=RangeFilter(start=0, end=100000),
    chunk_size=10000,
    io_budget=4,
)
report = engine.run()
```

### SingleProcessRunner

单进程执行器，负责完整串行循环：

- `ensure_schema`
- 读取 chunk / id batch
- done-id 跳过
- 构造 `LakeTask`
- 调用 `Calculator`
- 批量 `persist`
- 汇总 `RunReport`

`Engine` 不再直接持有这些执行细节。

### Manager (rank 0)

MPI 模式下的调度器，负责：

- 分配 chunk range 或 id batches 给各 worker
- 控制读 IO 并发预算 `io_budget`
- 接收 worker 状态消息
- 接收 worker 发送的结果行并集中 flush
- 汇总 `RunReport`

`Manager` 只依赖 `BatchWriter`，不直接读取源数据。

### Worker (rank 1+)

MPI 模式下的执行单元，负责：

- 等待 `TRIGGER_READ`
- 通过 `BatchReader` 读取 chunk / id batch
- 构造 `LakeTask`
- 调用 `Calculator`
- 把结果行发送给 `Manager`

状态机如下：

```text
PENDING -> READING -> CALCULATING -> PENDING -> ... -> DONE
```

`Worker` 只依赖 `BatchReader`，不负责写入。

### BatchReader

批处理读取接口，表达 batch 自己的业务读取语义：

```python
class BatchReader(ABC):
    def fetch_lake_area_chunk(self, chunk_start: int, chunk_end: int) -> dict[int, DataFrame]: ...
    def fetch_lake_area_by_ids(self, id_list: list[int]) -> dict[int, DataFrame]: ...
    def fetch_frozen_year_months_chunk(self, chunk_start: int, chunk_end: int) -> dict[int, set[int]]: ...
    def fetch_frozen_year_months_by_ids(self, id_list: list[int]) -> dict[int, set[int]]: ...
    def fetch_max_hylak_id(self) -> int: ...
    def fetch_done_ids(self, algorithm: str, chunk_start: int, chunk_end: int) -> set[int]: ...
```

当前实现：

- `PostgresBatchReader`
- `ParquetBatchReader`

### BatchWriter

批处理写接口，表达 batch 自己的业务写语义：

```python
class BatchWriter(ABC):
    def persist(self, rows_by_table: dict[str, list[dict]]) -> None: ...
    def ensure_schema(self, algorithm: str) -> None: ...
```

当前实现：

- `PostgresBatchWriter`
- `ParquetBatchWriter`

### LakeProvider

`LakeProvider` 已降级为 backend-oriented 共享能力层，主要服务：

- 基础湖泊时序读取
- geometry 读取
- grid aggregation 读取
- 其他非 batch 的共享 backend 访问场景

它不再作为 batch 框架的直接依赖，也不再定义 `done_ids / persist / ensure_schema` 这类 batch 语义。

### Calculator

纯计算逻辑，不做调度和底层 IO：

```python
class Calculator(ABC):
    def run(self, task: LakeTask) -> Any: ...
    def result_to_rows(self, result: Any) -> dict[str, list[dict]]: ...
    def error_to_rows(self, hylak_id: int, error: Exception, chunk_start: int, chunk_end: int) -> dict[str, list[dict]]: ...
```

已实现：

- `QuantileCalculator`
- `PWMExtremeCalculator`
- `EOTCalculator`
- `ComparisonCalculator`

### LakeTask

引擎与算法之间的数据契约：

```python
@dataclass(frozen=True)
class LakeTask:
    hylak_id: int
    series_df: pd.DataFrame
    frozen_year_months: frozenset[int]
```

### RangeFilter / IdSetFilter

用于控制批处理范围：

```python
RangeFilter(start=0, end=100000)
IdSetFilter({1, 2, 3})
```

## MPI 通信协议

### 消息类型

| Tag | 方向 | 说明 |
|-----|------|------|
| `TAG_STATUS` | Worker -> Manager | 状态更新 |
| `TAG_TRIGGER` | Manager -> Worker | 读取触发信号 |
| `TAG_DATA` | Worker -> Manager | 结果行数据 |

### 状态消息

```python
(state: str, stats: dict)
```

状态值：

- `pending`
- `reading`
- `calculating`
- `done`

### 触发信号

- `TRIGGER_READ`：允许 worker 开始下一轮读取

## IO 调度策略

当前实现只对**读 IO 并发**做预算控制。

1. Worker 进入 `PENDING` 后加入 `read_queue`
2. `Manager` 在 `io_active < io_budget` 时发送 `TRIGGER_READ`
3. Worker 进入 `READING`，执行 batch 读取
4. Worker 进入 `CALCULATING` 或直接回 `PENDING/DONE` 时释放 IO slot
5. Worker 把结果行通过 `TAG_DATA` 发回 `Manager`
6. `Manager` 聚合行数据并按 flush 策略集中持久化

也就是说，当前架构是：

- Worker 负责读和算
- Manager 负责集中写

不是早期设计稿中的双向 `READ/WRITE` IO 状态机。

## 运行报告

```python
@dataclass
class RunReport:
    total_chunks: int
    processed_chunks: int
    skipped_chunks: int
    source_lakes: int
    skipped_lakes: int
    success_lakes: int
    error_lakes: int
```

## 扩展指南

### 添加新算法

1. 实现 `Calculator` 子类
2. 注册到 `CalculatorFactory`
3. 在脚本层组装 `SourceConfig + BatchReader/BatchWriter + Engine`

### 添加新 batch backend

为 batch 新增：

- `MyBackendBatchReader`
- `MyBackendBatchWriter`

然后在 `build_batch_reader()` / `build_batch_writer()` 中注册。

### 添加新共享 backend 能力

如果能力是 batch 之外的共享能力，例如 geometry 或 grid aggregation，则扩展 `lakesource.provider`。
