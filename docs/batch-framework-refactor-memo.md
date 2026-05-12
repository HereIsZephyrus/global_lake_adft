# Batch 计算框架重构 Memo

## 背景

当前项目有三个独立的 batch 计算模块（Quantile、PWM Extreme、EOT），各自实现了不同的调度和 IO 逻辑，存在大量重复代码和不一致的设计模式。本次重构目标是建立统一的计算框架，支持 MPI 集群调度，彻底解耦脚本、IO 和算法逻辑。

**注**：Hawkes 是 EOT 的下游 Consumer，不在本次规划范围内，后续单独做调度。

## 现状分析

### 三个模块对比

| 维度 | Quantile | PWM Extreme | EOT |
|------|----------|-------------|-----|
| batch.py | 有（295行） | 有（219行） | 无（全在脚本） |
| 并行调度 | 无 | 无 | ProcessPoolExecutor |
| 单湖入口 | `run_single_lake_service()` | `run_single_lake_service()` | `EOTEstimator.fit()` |
| 结果类型 | `QuantileResult` | `PWMExtremeResult` | `(result_row, extreme_rows)` |
| 持久化 | 4表 | 2表 | 2表 |
| 跳过检查 | `fetch_processed_hylak_ids_in_chunk` | **无** | `_is_chunk_done` SQL |
| frozen 处理 | 不使用 | 不使用 | 使用 |
| 任务粒度 | (lake) | (lake) | (lake, tail, quantile) |

### 核心观察

1. **Pipeline 完全一致**：`fetch chunk → skip check → split → parallel compute → collect → persist → next chunk`
2. **差异仅在**：
   - 单湖计算函数不同（但签名可统一）
   - 结果→DB行的转换不同（但可抽象）
   - 持久化涉及的表不同
3. **EOT 任务粒度特殊**：(lake, tail, quantile) 三元组而非单个湖，需要内部展开

### 算法分类

| 类型 | 算法 | frozen 处理 |
|------|------|------------|
| **A: 序列拟合型** | EOT | 使用 frozen_map，跳过冻结月再拟合 |
| **B: 统计计算型** | Quantile, PWM | 传递 frozen_map（预留后续拓展） |

## 设计目标

1. **统一调度框架**：所有算法共享同一套调度逻辑
2. **MPI 集群支持**：替代 ProcessPoolExecutor，支持计算集群部署
3. **IO 层抽象**：Reader/Writer 统一接口，支持 DB 和文件两种实现
4. **算法层解耦**：Calculator 只负责计算，不碰 IO 和调度
5. **脚本极简化**：脚本只做参数解析 + 组装 Engine
6. **DB IO 优化**：DB IO 是瓶颈，最小化连接数和查询

## 架构设计

### 三层架构

```
┌─────────────────────────────────────────────────────────────┐
│                         Engine                              │
│  MPI Master-Worker 调度                                     │
│  - reader: Reader                                           │
│  - writer: Writer                                           │
│  - calculator: Calculator                                   │
│  - filter: LakeFilter                                       │
└─────────────────────────────────────────────────────────────┘
                              │
                              │ 组合
                              ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│    Reader    │    │  Calculator  │    │    Writer    │
│    (ABC)     │    │    (ABC)     │    │    (ABC)     │
├──────────────┤    ├──────────────┤    ├──────────────┤
│  DBReader    │    │ QuantileCalc │    │  DBWriter    │
│  FileReader  │    │ PWMExtreme   │    │ FileWriter   │
│              │    │ EOTCalculator│    │ CompositeWrt │
└──────────────┘    └──────────────┘    └──────────────┘
                              │
                              │ 构造
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                        Factory 层                           │
│  IOFactory: create_reader(algorithm) → Reader               │
│             create_writer(algorithm) → Writer               │
│  CalculatorFactory: create(algorithm, **kwargs) → Calculator│
└─────────────────────────────────────────────────────────────┘
                              │
                              │ 组装
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                        脚本层                               │
│  参数解析 → Factory 构造 → Engine 组装 → run()              │
└─────────────────────────────────────────────────────────────┘
```

### 核心类

#### LakeTask

引擎和算法之间的数据契约：

```python
@dataclass(frozen=True)
class LakeTask:
    hylak_id: int
    series_df: pd.DataFrame
    frozen_year_months: set[int]  # 所有算法都传递，Calculator 自行决定是否使用
```

#### Reader (ABC)

统一数据读取接口：

```python
class Reader(ABC):
    @abstractmethod
    def fetch_lake_map(self, chunk_start: int, chunk_end: int) -> dict[int, pd.DataFrame]: ...
    
    @abstractmethod
    def fetch_frozen_map(self, chunk_start: int, chunk_end: int) -> dict[int, set[int]]:
        """返回 frozen_year_months 映射，默认返回空 dict"""
        return {}
    
    @abstractmethod
    def fetch_done_ids(self, chunk_start: int, chunk_end: int) -> set[int]:
        """查对应算法的 run_status 表，返回已完成的 hylak_id 集合"""
        ...
    
    @abstractmethod
    def max_hylak_id(self) -> int: ...
    
    @abstractmethod
    def ensure_schema(self) -> None: ...
```

#### Writer (ABC)

统一数据写入接口：

```python
class Writer(ABC):
    @abstractmethod
    def persist(self, rows_by_table: dict[str, list[dict]]) -> None: ...
```

#### Calculator (ABC)

纯计算逻辑：

```python
class Calculator(ABC):
    @abstractmethod
    def run(self, task: LakeTask) -> Any:
        """执行计算"""
        ...
    
    @abstractmethod
    def result_to_rows(self, result: Any) -> dict[str, list[dict]]:
        """将结果转为 {table_name: rows}"""
        ...
    
    @abstractmethod
    def error_to_rows(self, hylak_id: int, error: Exception,
                      chunk_start: int, chunk_end: int) -> dict[str, list[dict]]:
        """将错误转为 run_status 行"""
        ...
```

#### LakeFilter

hylak_id 过滤器：

```python
class LakeFilter(ABC):
    @abstractmethod
    def __call__(self, hylak_ids: Iterable[int]) -> set[int]: ...

class RangeFilter(LakeFilter):
    """只保留 [start, end) 范围内的 hylak_id"""
    def __init__(self, start: int = 0, end: int | None = None): ...

class IdSetFilter(LakeFilter):
    """只保留指定集合中的 hylak_id"""
    def __init__(self, ids: set[int]): ...
```

#### Engine

MPI Master-Worker 调度引擎：

```python
class Engine:
    def __init__(
        self,
        reader: Reader,
        writer: Writer,
        calculator: Calculator,
        *,
        filter: LakeFilter | None = None,
        chunk_size: int = 10_000,
        limit_id: int | None = None,
    ): ...
    
    def run(self) -> RunReport | None:
        """MPI 调度：rank 0 为 master，其他为 worker"""
        ...
```

### MPI 调度设计

#### 通信协议

```
Master (rank 0)                    Workers (rank 1..n-1)
     │                                    │
     │  读 chunk: lake_map, frozen_map    │
     │                                    │
     │  ── BCAST chunk_data ─────────────▶│  (广播 chunk 数据)
     │                                    │
     │  ── SEND hylak_ids ───────────────▶│  (点对点分发任务)
     │                                    │  处理任务
     │  ◀── SEND results ─────────────────│  (返回结果)
     │                                    │
     │  ... 循环直到所有任务完成 ...        │
     │                                    │
     │  ── BCAST None ───────────────────▶│  (广播终止信号)
```

#### ChunkData 广播结构

```python
@dataclass
class ChunkData:
    lake_map: dict[int, pd.DataFrame]
    frozen_map: dict[int, set[int]]
```

#### Master 逻辑

```python
def _run_master(self, comm, size) -> RunReport:
    for chunk_start, chunk_end in chunk_ranges:
        # 1. 读 chunk 数据
        lake_map = reader.fetch_lake_map(chunk_start, chunk_end)
        
        # 2. Filter + Skip check
        candidate_ids = set(lake_map.keys())
        if filter:
            candidate_ids = candidate_ids & filter(candidate_ids)
        done_ids = reader.fetch_done_ids(chunk_start, chunk_end)
        pending_ids = candidate_ids - done_ids
        
        # 3. 读 frozen_map
        frozen_map = reader.fetch_frozen_map(chunk_start, chunk_end)
        
        # 4. 广播 chunk 数据
        chunk_data = ChunkData(lake_map, frozen_map)
        comm.bcast(chunk_data, root=0)
        
        # 5. 动态分发任务
        task_queue = list(pending_ids)
        pending_workers = set(range(1, size))
        results = defaultdict(list)
        
        while task_queue or pending_workers:
            # 接收结果
            result_msg = comm.recv(source=MPI.ANY_SOURCE, tag=TAG_RESULT)
            worker_rank = status.Get_source()
            pending_workers.add(worker_rank)
            
            # 分发任务
            while pending_workers and task_queue:
                worker_rank = pending_workers.pop()
                batch = task_queue[:batch_size]
                task_queue = task_queue[batch_size:]
                comm.send(batch, dest=worker_rank, tag=TAG_TASK)
        
        # 6. 写 DB
        writer.persist(results)
    
    # 7. 广播终止信号
    comm.bcast(None, root=0)
```

#### Worker 逻辑

```python
def _run_worker(self, comm, rank):
    while True:
        # 1. 接收广播的 chunk 数据
        chunk_data = comm.bcast(root=0)
        if chunk_data is None:
            break  # 终止信号
        
        # 2. 接收任务
        hylak_ids = comm.recv(source=0, tag=TAG_TASK)
        
        # 3. 处理任务
        results = defaultdict(list)
        for hid in hylak_ids:
            task = LakeTask(
                hylak_id=hid,
                series_df=chunk_data.lake_map[hid],
                frozen_year_months=chunk_data.frozen_map.get(hid, set()),
            )
            result = calculator.run(task)
            for table, rows in calculator.result_to_rows(result).items():
                results[table].extend(rows)
        
        # 4. 返回结果
        comm.send(dict(results), dest=0, tag=TAG_RESULT)
```

### DB IO 优化策略

**关键约束**：DB IO 是瓶颈

| 策略 | 说明 |
|------|------|
| 仅 rank 0 连接 DB | Master 负责所有读写，Worker 不连 DB |
| chunk 级别广播 | lake_map + frozen_map 一并广播，避免多次传输 |

### 各 Calculator 实现

#### QuantileCalculator

```python
class QuantileCalculator(Calculator):
    def __init__(self, min_valid_per_month, min_valid_observations): ...
    
    def run(self, task: LakeTask) -> QuantileResult:
        return run_monthly_anomaly_transition(
            task.series_df,
            hylak_id=task.hylak_id,
            frozen_year_months=task.frozen_year_months or None,  # 预留后续拓展
            min_valid_per_month=...,
            min_valid_observations=...,
        )
    
    def result_to_rows(self, result) -> dict[str, list[dict]]:
        return {
            "quantile_labels": result_to_label_rows(result, ...),
            "quantile_extremes": result_to_extreme_rows(result, ...),
            "quantile_abrupt_transitions": result_to_transition_rows(result, ...),
            "quantile_run_status": [make_run_status_row(...)],
        }
```

#### PWMExtremeCalculator

```python
class PWMExtremeCalculator(Calculator):
    def __init__(self, pwm_config, min_valid_per_month, min_valid_observations): ...
    
    def run(self, task: LakeTask) -> PWMExtremeResult:
        return compute_monthly_thresholds(
            task.series_df,
            hylak_id=task.hylak_id,
            config=self._pwm_config,
            frozen_year_months=task.frozen_year_months or None,  # 预留后续拓展
        )
    
    def result_to_rows(self, result) -> dict[str, list[dict]]:
        return {
            "pwm_extreme_thresholds": result_to_threshold_rows(result, ...),
            "pwm_extreme_run_status": [make_run_status_row(...)],
        }
```

#### EOTCalculator

```python
class EOTCalculator(Calculator):
    def __init__(self, tails: list[str], quantiles: list[float]): ...
    
    def run(self, task: LakeTask) -> EOTResult:
        """内部循环 tails × quantiles，Engine 按 lake 粒度调度"""
        estimator = EOTEstimator()
        fits = []
        for tail in self._tails:
            for q in self._quantiles:
                fit = estimator.fit(
                    task.series_df,
                    tail=tail,
                    threshold_quantile=q,
                    frozen_year_months=task.frozen_year_months,  # 使用
                )
                fits.append((tail, q, fit))
        return EOTResult(hylak_id=task.hylak_id, fits=fits)
    
    def result_to_rows(self, result) -> dict[str, list[dict]]:
        # 展开所有 (tail, q, fit) → eot_results + eot_extremes + eot_run_status rows
        return {
            "eot_results": [...],
            "eot_extremes": [...],
            "eot_run_status": [make_run_status_row(...)],
        }
```

### Factory 层

#### IOFactory

```python
class IOFactory(ABC):
    @abstractmethod
    def create_reader(self, algorithm: str) -> Reader: ...
    
    @abstractmethod
    def create_writer(self, algorithm: str) -> Writer: ...

class DBIOFactory(IOFactory):
    def __init__(self, connection_source): ...
    
    def create_reader(self, algorithm: str) -> Reader:
        return DBReader(self._conn, algorithm=algorithm)
    
    def create_writer(self, algorithm: str) -> Writer:
        return DBWriter(self._conn, algorithm=algorithm)

class FileIOFactory(IOFactory):
    def __init__(self, data_root: Path): ...
    
    def create_reader(self, algorithm: str) -> Reader:
        return FileReader(self._root, algorithm=algorithm)
    
    def create_writer(self, algorithm: str) -> Writer:
        return FileWriter(self._root, algorithm=algorithm)
```

#### CalculatorFactory

```python
class CalculatorFactory:
    _registry = {
        "quantile": QuantileCalculator,
        "pwm_extreme": PWMExtremeCalculator,
        "eot": EOTCalculator,
    }
    
    @staticmethod
    def create(algorithm: str, **kwargs) -> Calculator:
        cls = CalculatorFactory._registry[algorithm]
        return cls(**kwargs)
```

### 脚本示例

#### run_quantile.py

```python
from lakeanalysis.batch.engine import Engine, RangeFilter
from lakeanalysis.batch.io import DBIOFactory
from lakeanalysis.batch.calculator import CalculatorFactory

def main():
    args = parse_args()  # --workers, --chunk-size, --limit-id, --id-range, + 算法参数
    
    factory = DBIOFactory(series_db)
    reader = factory.create_reader("quantile")
    writer = factory.create_writer("quantile")
    calculator = CalculatorFactory.create(
        "quantile",
        min_valid_per_month=args.min_valid_per_month,
        min_valid_observations=args.min_valid_observations,
    )
    
    engine = Engine(
        reader=reader,
        writer=writer,
        calculator=calculator,
        filter=RangeFilter(start=args.id_start, end=args.id_end) if args.id_range else None,
        chunk_size=args.chunk_size,
        limit_id=args.limit_id,
    )
    
    report = engine.run()
    if report:
        log.info("Done: %d success, %d error", report.success_lakes, report.error_lakes)
```

### MPI 运行方式

```bash
# 单机多进程（测试）
mpiexec -np 4 python scripts/run_quantile.py --chunk-size 10000

# 集群部署
mpiexec -np 64 -hostfile hosts.txt python scripts/run_quantile.py --chunk-size 10000

# 单进程模式（本地开发，不启动 MPI）
python scripts/run_quantile.py --chunk-size 10000  # size=1 时自动走单进程逻辑
```

## 文件结构

```
lakeanalysis/
  batch/
    __init__.py              # re-export Engine, RunReport, LakeTask, LakeFilter
    engine.py                # Engine, RunReport, LakeTask, LakeFilter, ChunkData
    io/
      __init__.py            # re-export
      factory.py             # IOFactory(ABC), DBIOFactory, FileIOFactory
      reader.py              # Reader(ABC), DBReader, FileReader
      writer.py              # Writer(ABC), DBWriter, FileWriter, CompositeWriter
    calculator/
      __init__.py            # re-export Calculator, CalculatorFactory
      base.py                # Calculator(ABC)
      factory.py             # CalculatorFactory
      quantile.py            # QuantileCalculator
      pwm_extreme.py         # PWMExtremeCalculator
      eot.py                 # EOTCalculator, EOTResult

scripts/
  run_quantile.py            # 替代 run_quantile_batch.py
  run_pwm_extreme.py         # 新增
  run_eot.py                 # 替代 run_eot_batch.py
```

## 删除/废弃文件

| 文件 | 处理 |
|------|------|
| `lakeanalysis/quantile/batch.py` | 删除，逻辑移至 `batch/calculator/quantile.py` |
| `lakeanalysis/pwm_extreme/batch.py` | 删除，逻辑移至 `batch/calculator/pwm_extreme.py` |
| `scripts/run_quantile_batch.py` | 替换为 `scripts/run_quantile.py` |
| `scripts/run_eot_batch.py` | 替换为 `scripts/run_eot.py` |

## 关键设计决策汇总

| 决策 | 选择 | 原因 |
|------|------|------|
| MPI 框架 | mpi4py | Python 原生绑定，最灵活 |
| 调度模式 | Master-Worker | rank 0 为 master，其他为 worker |
| DB 连接 | 仅 rank 0 | DB IO 是瓶颈，最小化连接数 |
| 数据传输 | chunk 级别广播 | IO 最少，避免多次传输 |
| 任务分发 | 动态分发 | 负载均衡 |
| Calculator 访问数据 | `run(task)` | 不持有 Reader，无状态 |
| IO 实例创建 | Factory 在脚本层构造 | Engine 直接持有 reader/writer |
| frozen 数据 | Engine 统一 fetch，通过 LakeTask 传递 | 所有算法都传递，Calculator 自行决定是否使用 |
| EOT 多参数 | tails/quantiles 作为构造参数 | `run()` 内部循环 |
| run_status | 各算法独立表 | 统一按 lake 粒度跳过检查 |
| LakeFilter | CLI 用 `--id-range` + `--limit-id` | 灵活过滤 |

## 实施计划

### Phase 1: 框架搭建（~250行）

1. 创建 `batch/` 目录结构
2. 实现 `engine.py`：Engine, RunReport, LakeTask, LakeFilter, ChunkData
3. 实现 `io/reader.py`：Reader(ABC)
4. 实现 `io/writer.py`：Writer(ABC)
5. 实现 `io/factory.py`：IOFactory, DBIOFactory
6. 实现 `calculator/base.py`：Calculator(ABC)
7. 实现 `calculator/factory.py`：CalculatorFactory

### Phase 2: Quantile 验证（~150行）

1. 实现 `DBReader`（Quantile 专用）
2. 实现 `DBWriter`（通用 upsert）
3. 实现 `QuantileCalculator`
4. 创建 `scripts/run_quantile.py`
5. 单进程测试 + MPI 4进程测试

### Phase 3: PWM Extreme（~80行）

1. 实现 `PWMExtremeCalculator`
2. 创建 `scripts/run_pwm_extreme.py`
3. 测试

### Phase 4: EOT（~180行）

1. 新建 `eot_run_status` 表
2. 实现 `EOTCalculator`（tails×quantiles 展开）
3. 实现 `EOTResult` 包装类
4. 创建 `scripts/run_eot.py`
5. 测试

### Phase 5: 清理

1. 删除旧 batch.py 文件
2. 删除旧脚本
3. 更新 `__init__.py` 导出
4. 全量测试

### Phase 6: FileReader 实现（未来）

1. 实现 `FileReader`（parquet/csv）
2. 实现 `FileIOFactory`
3. 支持无 DB 平台部署

## 代码量估算

| 模块 | 行数 |
|------|------|
| `batch/engine.py` | ~200 |
| `batch/io/factory.py` | ~60 |
| `batch/io/reader.py` | ~120 |
| `batch/io/writer.py` | ~80 |
| `batch/calculator/base.py` | ~30 |
| `batch/calculator/factory.py` | ~20 |
| `batch/calculator/quantile.py` | ~70 |
| `batch/calculator/pwm_extreme.py` | ~50 |
| `batch/calculator/eot.py` | ~130 |
| 脚本 × 3 | ~40 × 3 |
| **总计** | ~800 |

## 风险与注意事项

1. **MPI pickle 问题**：`LakeTask` 和 `ChunkData` 必须可 pickle，pandas DataFrame 可 pickle
2. **内存压力**：广播大 chunk 数据可能导致内存压力，需要合理设置 chunk_size
3. **错误处理**：Worker 异常需要优雅处理，不能导致整个 job 失败
4. **进度监控**：MPI 模式下进度日志只在 master 输出
5. **单进程兼容**：`size=1` 时走单进程逻辑，方便本地开发测试
6. **DB 连接池**：rank 0 的 DB 连接需要处理长时间运行的连接稳定性

## 附录：run_status 表设计

各算法使用独立的 run_status 表，统一按 lake 粒度跳过检查：

| 算法 | run_status 表 | 说明 |
|------|--------------|------|
| Quantile | `quantile_run_status` | 已存在 |
| PWM Extreme | `pwm_extreme_run_status` | 已存在 |
| EOT | `eot_run_status` | **新建** |

`DBReader.fetch_done_ids()` 根据 algorithm 参数查对应表：

```python
def fetch_done_ids(self, chunk_start, chunk_end) -> set[int]:
    if self._algorithm == "quantile":
        return fetch_quantile_status_ids_in_range(...)
    elif self._algorithm == "pwm_extreme":
        return fetch_pwm_extreme_status_ids_in_range(...)
    elif self._algorithm == "eot":
        return fetch_eot_run_status_ids_in_range(...)  # 新建表
```

### eot_run_status 表结构

```sql
CREATE TABLE eot_run_status (
    hylak_id BIGINT NOT NULL,
    chunk_start BIGINT NOT NULL,
    chunk_end BIGINT NOT NULL,
    workflow_version VARCHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL,  -- 'done' | 'error'
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (hylak_id, workflow_version)
);
```

## 后续规划：Hawkes

Hawkes 是 EOT 的下游 Consumer，不在本次规划范围内。后续单独做调度：

1. 先跑 EOT，有结果后再跑 Hawkes
2. Hawkes 从 DB 读取 EOT 结果（`eot_results`, `eot_extremes`）
3. Hawkes 有自己独立的调度流程和 Consumer 接口
