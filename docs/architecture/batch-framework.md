# Batch 计算框架

`lakeanalysis.batch` 提供 MPI 分布式批处理能力，支持大规模湖泊数据分析。

## 架构概览

```
┌─────────────────────────────────────────────────────────────┐
│                         Engine                              │
│  入口：判断 MPI/单进程，组装 Manager/Worker                  │
└─────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
   ┌────────────┐      ┌────────────┐      ┌────────────┐
   │  Manager   │      │   Worker   │      │  Provider  │
   │  (rank 0)  │      │ (rank 1+)  │      │  (IO 层)   │
   │  IO 调度   │      │  状态机    │      │  DB/Parquet│
   └────────────┘      └────────────┘      └────────────┘
                              │
                              ▼
                       ┌────────────┐
                       │ Calculator │
                       │  (算法层)  │
                       └────────────┘
```

## 核心组件

### Engine

入口类，负责：
- 检测 MPI 环境，决定单进程或分布式模式
- 单进程模式：直接执行计算循环
- MPI 模式：rank 0 启动 Manager，其他 rank 启动 Worker

```python
from lakeanalysis.batch import Engine, RangeFilter
from lakeanalysis.batch.calculator import CalculatorFactory

engine = Engine(
    provider=provider,
    calculator=CalculatorFactory.create("quantile", ...),
    algorithm="quantile",
    lake_filter=RangeFilter(start=0, end=100000),
    chunk_size=10000,
    io_budget=4,
)
report = engine.run()
```

### Manager (rank 0)

IO 调度器，负责：
- 分配 chunk range 给各 Worker
- 控制 DB IO 并发（`io_budget`）
- 追踪 Worker 状态
- 汇总运行报告

### Worker (rank 1+)

状态机驱动的计算单元：

```
PENDING ──TRIGGER_READ──▶ READING ──(auto)──▶ CALCULATING ──(auto)──▶ PENDING
PENDING ──TRIGGER_WRITE──▶ WRITING ──(auto)──▶ PENDING / DONE
```

每个 Worker：
- 独立持有 `LakeProvider` 和 `Calculator`
- 自主处理分配的 chunk range
- 通过状态消息与 Manager 通信

### LakeProvider

统一 IO 接口（来自 `lakesource` 包）：

```python
class LakeProvider:
    def fetch_max_hylak_id(self) -> int: ...
    def fetch_lake_area_chunk(self, start, end) -> dict[int, DataFrame]: ...
    def fetch_frozen_year_months_chunk(self, start, end) -> dict[int, set[int]]: ...
    def fetch_done_ids(self, algorithm, start, end) -> set[int]: ...
    def persist(self, rows_by_table: dict[str, list[dict]]) -> None: ...
    def ensure_schema(self, algorithm: str) -> None: ...
```

支持两种后端：
- `PostgresProvider`：PostgreSQL 数据库
- `ParquetProvider`：Parquet 文件

### Calculator

纯计算逻辑，无 IO 依赖：

```python
class Calculator(ABC):
    def run(self, task: LakeTask) -> Any: ...
    def result_to_rows(self, result) -> dict[str, list[dict]]: ...
    def error_to_rows(self, hylak_id, error, chunk_start, chunk_end) -> dict[str, list[dict]]: ...
```

已实现：
- `QuantileCalculator`
- `PWMExtremeCalculator`
- `EOTCalculator`

### RangeFilter

hylak_id 范围过滤器：

```python
lake_filter = RangeFilter(start=0, end=100000)
```

## MPI 通信协议

### 消息类型

| Tag | 方向 | 说明 |
|-----|------|------|
| `TAG_STATUS` | Worker → Manager | 状态更新 |
| `TAG_TRIGGER` | Manager → Worker | IO 触发信号 |

### 状态消息

```python
(state: str, stats: dict)
```

状态值：`pending`, `reading`, `calculating`, `writing`, `done`

### 触发信号

- `TRIGGER_READ`：允许 Worker 读取数据
- `TRIGGER_WRITE`：允许 Worker 写入结果

## IO 调度策略

Manager 通过 `io_budget` 控制 DB IO 并发：

1. Worker 进入 `PENDING` 状态后加入 `read_queue`
2. 当 `io_active < io_budget` 时，发送 `TRIGGER_READ`
3. Worker 读取完成后进入 `CALCULATING`，释放 IO slot
4. 计算完成后进入 `PENDING(prev=calculating)`，加入 `write_queue`
5. 当 `io_active < io_budget` 时，发送 `TRIGGER_WRITE`
6. 写入完成后进入 `PENDING(prev=writing)`，释放 IO slot

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

1. 实现 `Calculator` 子类：

```python
class MyCalculator(Calculator):
    def run(self, task: LakeTask) -> MyResult: ...
    def result_to_rows(self, result) -> dict[str, list[dict]]: ...
    def error_to_rows(self, hylak_id, error, chunk_start, chunk_end) -> dict[str, list[dict]]: ...
```

2. 注册到 `CalculatorFactory`：

```python
CalculatorFactory._registry["my_algorithm"] = MyCalculator
```

3. 创建运行脚本：

```python
# scripts/run_my_algorithm.py
engine = Engine(
    provider=provider,
    calculator=CalculatorFactory.create("my_algorithm"),
    algorithm="my_algorithm",
    ...
)
```

### 添加新数据源

在 `lakesource` 包中实现 `LakeProvider` 子类。