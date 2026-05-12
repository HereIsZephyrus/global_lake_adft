# Quality 模块重构与 Area Ratio Filter Memo

## 背景

`lakeanalysis.quality` 模块当前存在以下问题：

1. **`compute.py` 职责混杂**：同时包含纯计算函数（`compute_median_area`、`compute_mean_area`、`compute_flatness_metrics`、`compute_area_range`）和分类判定函数（`is_anomalous`、`classify_area_anomaly`、`classify_outside_range`），命名和结构都不清晰
2. **异常类型不可持久化**：`area_anomalies` 表没有 `anomaly_type` 列，无法区分一个湖泊属于哪种异常，也无法绘制 UpSet Plot
3. **新增 filter 缺乏统一模式**：`median_zero`、`flat`、`outside_range` 三个 filter 散落在 `compute.py` 各处，`area_ratio` 即将新增，需要统一的 filter 框架
4. **`comparison.py` 职责混杂**：同时包含纯计算函数（`compute_area_ratio`、`compute_relative_diff`、`compute_log2_ratio`、`classify_agreement`）和高层组合逻辑（`enrich_comparison_df`、`summarize_comparison`）
5. **`run_comparison.py` 冗余**：`quality/run_comparison.py` 是 `scripts/run_area_comparison.py` 的早期精简版，无任何代码引用，应删除
6. **`protocol.py` 残留**：`TRIGGER_WRITE` 和 `WorkerState.WRITING` 已不再使用

## 现状分析

### 当前目录结构

```
quality/
├── __init__.py          # 公共 API 导出
├── compute.py           # 混杂：计算 + 分类（200行）
├── comparison.py        # 混杂：计算 + 组合（228行）
├── frozen.py            # 冰冻月预处理（172行，不动）
├── interpolation.py     # 插值检测（227行，不动）
└── run_comparison.py    # 冗余脚本（180行，删除）
```

### 当前异常分类体系

| 异常类型 | 判定条件 | 所在函数 | 是否持久化 |
|---------|---------|---------|-----------|
| median_zero | `rs_area_median == 0` | `is_anomalous()` | 否 |
| flat | `dominant_ratio >= 0.8` | `classify_area_anomaly()` 内 | 否 |
| outside_range | `atlas_area < min_area` 或 `> max_area` | `classify_outside_range()` | 否 |
| area_ratio（待新增） | `rs_area_median / atlas_area ∉ [0.1, 10]` | 待实现 | 否 |

### `area_anomalies` 表当前 schema

```sql
CREATE TABLE IF NOT EXISTS area_anomalies (
    hylak_id       INTEGER PRIMARY KEY,
    rs_area_mean   DOUBLE PRECISION,
    rs_area_median DOUBLE PRECISION,
    atlas_area     DOUBLE PRECISION,
    computed_at    TIMESTAMPTZ DEFAULT now()
);
```

**关键缺陷**：无 `anomaly_type` 列，无法区分异常类型，无法绘制 UpSet Plot。

### `area_quality` 表当前 schema

```sql
CREATE TABLE IF NOT EXISTS area_quality (
    hylak_id       INTEGER PRIMARY KEY,
    rs_area_mean   DOUBLE PRECISION,
    rs_area_median DOUBLE PRECISION,
    atlas_area     DOUBLE PRECISION,
    computed_at    TIMESTAMPTZ DEFAULT now()
);
```

## 设计目标

1. **Filter 模式**：每个异常类型是一个独立的 filter，遵循统一 Protocol
2. **聚合器**：一个 `classify_area_anomaly()` 函数运行所有 filter，合并结果
3. **异常类型持久化**：`area_anomalies` 表增加布尔列，记录每个湖泊属于哪些异常集合
4. **UpSet Plot**：用 `upsetplot` 库绘制异常集合交集图，展示各类异常的比例和重叠
5. **计算与分类分离**：纯计算函数归 `metrics.py`，分类判定归 `filters/`
6. **向后兼容**：公共 API（`classify_area_anomaly`、`FlatnessFilterConfig` 等）保持从 `quality` 顶层导出

## 架构设计

### 重构后目录结构

```
quality/
├── __init__.py          # 公共 API 导出（保持不变）
├── metrics.py           # 纯计算函数
├── filters/
│   ├── __init__.py      # AnomalyFilter Protocol, AnomalyFlag, LakeContext
│   ├── median_zero.py   # MedianZeroFilter
│   ├── flatness.py      # FlatnessFilter
│   ├── area_ratio.py    # AreaRatioFilter（新增）
│   └── outside_range.py # OutsideRangeFilter
├── classify.py          # classify_area_anomaly() 聚合器 + default_filters()
├── comparison.py        # enrich_comparison_df, summarize_comparison（高层组合）
├── frozen.py            # 冰冻月预处理（不动）
└── interpolation.py     # 插值检测（不动）
```

### 删除的文件

| 文件 | 原因 |
|------|------|
| `quality/compute.py` | 内容分散到 `metrics.py` + `filters/` + `classify.py` |
| `quality/run_comparison.py` | 冗余，`scripts/run_area_comparison.py` 是完整版且无代码引用 |

### 不动的文件

| 文件 | 原因 |
|------|------|
| `quality/frozen.py` | 数据预处理模块，不是异常 filter |
| `quality/interpolation.py` | 独立检测模块，未来可选迁入 `filters/` |
| `scripts/run_area_comparison.py` | 已在正确位置 |

### 核心类型定义

```python
# filters/__init__.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import pandas as pd


@dataclass(frozen=True)
class LakeContext:
    """Single-lake data bundle passed to every filter."""

    df: pd.DataFrame
    rs_area_median: float
    rs_area_mean: float
    atlas_area: float


@dataclass(frozen=True)
class AnomalyFlag:
    """Result from a single anomaly filter."""

    name: str
    is_anomaly: bool
    detail: dict[str, float | bool]


class AnomalyFilter(Protocol):
    """Protocol for anomaly classification filters."""

    name: str

    def classify(self, ctx: LakeContext) -> AnomalyFlag: ...
```

### Filter 实现

#### MedianZeroFilter

```python
# filters/median_zero.py

from . import AnomalyFilter, AnomalyFlag, LakeContext


class MedianZeroFilter:
    name = "median_zero"

    def classify(self, ctx: LakeContext) -> AnomalyFlag:
        is_anomaly = ctx.rs_area_median == 0.0
        return AnomalyFlag(name=self.name, is_anomaly=is_anomaly, detail={})
```

#### FlatnessFilter

```python
# filters/flatness.py

from dataclasses import dataclass

from ..metrics import compute_flatness_metrics
from . import AnomalyFilter, AnomalyFlag, LakeContext


@dataclass(frozen=True)
class FlatnessFilterConfig:
    dominant_ratio_threshold: float = 0.8
    round_digits: int | None = None


class FlatnessFilter:
    name = "flat"

    def __init__(self, config: FlatnessFilterConfig | None = None) -> None:
        self._config = config or FlatnessFilterConfig()

    def classify(self, ctx: LakeContext) -> AnomalyFlag:
        metrics = compute_flatness_metrics(
            ctx.df,
            value_column="water_area",
            round_digits=self._config.round_digits,
        )
        dominant_ratio = metrics["dominant_ratio"]
        is_anomaly = dominant_ratio >= self._config.dominant_ratio_threshold
        return AnomalyFlag(
            name=self.name,
            is_anomaly=is_anomaly,
            detail={"dominant_ratio": dominant_ratio},
        )
```

#### AreaRatioFilter

```python
# filters/area_ratio.py

from dataclasses import dataclass

import numpy as np

from ..metrics import compute_area_ratio
from . import AnomalyFilter, AnomalyFlag, LakeContext


@dataclass(frozen=True)
class AreaRatioConfig:
    min_ratio: float = 0.1
    max_ratio: float = 10.0


class AreaRatioFilter:
    name = "area_ratio"

    def __init__(self, config: AreaRatioConfig | None = None) -> None:
        self._config = config or AreaRatioConfig()

    def classify(self, ctx: LakeContext) -> AnomalyFlag:
        ratio = float(compute_area_ratio(
            np.asarray([ctx.rs_area_median]),
            np.asarray([ctx.atlas_area]),
        )[0])
        is_anomaly = (
            np.isnan(ratio)
            or ratio < self._config.min_ratio
            or ratio > self._config.max_ratio
        )
        return AnomalyFlag(
            name=self.name,
            is_anomaly=is_anomaly,
            detail={"area_ratio": ratio},
        )
```

#### OutsideRangeFilter

```python
# filters/outside_range.py

from ..metrics import compute_area_range
from . import AnomalyFilter, AnomalyFlag, LakeContext


class OutsideRangeFilter:
    name = "outside_range"

    def classify(self, ctx: LakeContext) -> AnomalyFlag:
        range_metrics = compute_area_range(ctx.df)
        min_area = range_metrics["min_area"]
        max_area = range_metrics["max_area"]
        atlas = ctx.atlas_area

        if atlas <= 0:
            is_outside = False
            is_below = False
            is_above = False
        else:
            is_below = atlas < min_area
            is_above = atlas > max_area
            is_outside = is_below or is_above

        return AnomalyFlag(
            name=self.name,
            is_anomaly=is_outside,
            detail={
                "is_below_min": bool(is_below),
                "is_above_max": bool(is_above),
                "min_area": min_area,
                "max_area": max_area,
            },
        )
```

### 聚合器

```python
# classify.py

from __future__ import annotations

from .filters import AnomalyFilter, AnomalyFlag, LakeContext
from .filters.median_zero import MedianZeroFilter
from .filters.flatness import FlatnessFilter, FlatnessFilterConfig
from .filters.area_ratio import AreaRatioFilter, AreaRatioConfig
from .filters.outside_range import OutsideRangeFilter


def classify_area_anomaly(
    ctx: LakeContext,
    filters: list[AnomalyFilter],
) -> dict[str, bool | float]:
    """Run all filters and merge results into a flat dict.

    Returns:
        Dict with:
          - is_anomalous: True if any filter flags anomaly
          - is_{filter.name}: bool per filter
          - {detail keys}: merged from all filter details
    """
    flags: list[AnomalyFlag] = [f.classify(ctx) for f in filters]
    result: dict[str, bool | float] = {
        "is_anomalous": any(f.is_anomaly for f in flags),
    }
    for f in flags:
        result[f"is_{f.name}"] = f.is_anomaly
        result.update(f.detail)
    return result


def default_filters(
    flat_config: FlatnessFilterConfig | None = None,
    ratio_config: AreaRatioConfig | None = None,
) -> list[AnomalyFilter]:
    """Construct the default filter chain."""
    return [
        MedianZeroFilter(),
        FlatnessFilter(flat_config),
        AreaRatioFilter(ratio_config),
        OutsideRangeFilter(),
    ]
```

### `metrics.py` 内容

从 `compute.py` 和 `comparison.py` 提取的纯计算函数：

```python
# metrics.py

# 来自 compute.py:
#   compute_median_area(df) -> float
#   compute_mean_area(df) -> float
#   compute_flatness_metrics(df, ...) -> dict
#   compute_area_range(df, ...) -> dict
#   _prepare_values(df, ...) -> pd.Series  (内部函数)

# 来自 comparison.py:
#   compute_area_ratio(rs_area, atlas_area) -> ndarray
#   compute_relative_diff(rs_area, atlas_area) -> ndarray
#   compute_log2_ratio(rs_area, atlas_area) -> ndarray
#   classify_agreement(ratio, config) -> Categorical
#   AgreementConfig
#   _percentile_stats(...) -> dict  (内部函数)
#   _agreement_counts(...) -> dict  (内部函数)
#   _direction_counts(...) -> tuple (内部函数)
```

### `comparison.py` 精简后

只保留高层组合逻辑，计算函数从 `metrics` import：

```python
# comparison.py (精简后)

from .metrics import (
    AgreementConfig,
    compute_area_ratio,
    compute_log2_ratio,
    compute_relative_diff,
    classify_agreement,
)

# 保留:
#   summarize_comparison(df, ...) -> dict
#   enrich_comparison_df(df, ...) -> DataFrame
```

### `area_anomalies` 表新 schema

```sql
CREATE TABLE IF NOT EXISTS area_anomalies (
    hylak_id       INTEGER PRIMARY KEY,
    rs_area_mean   DOUBLE PRECISION,
    rs_area_median DOUBLE PRECISION,
    atlas_area     DOUBLE PRECISION,
    is_median_zero  BOOLEAN DEFAULT FALSE,
    is_flat         BOOLEAN DEFAULT FALSE,
    is_area_ratio   BOOLEAN DEFAULT FALSE,
    is_outside_range BOOLEAN DEFAULT FALSE,
    computed_at    TIMESTAMPTZ DEFAULT now()
);
```

### UpSet Plot 设计

使用 `upsetplot` 库（BSD 3-clause，基于 matplotlib），添加为 `lakeviz` 依赖。

```python
# lakeviz/quality/plot.py 新增函数

def plot_anomaly_upset(
    flags_df: pd.DataFrame,
    *,
    min_size: int = 0,
    show_counts: bool = True,
) -> matplotlib.figure.Figure:
    """Plot UpSet diagram of anomaly set intersections.

    Args:
        flags_df: DataFrame with columns
            hylak_id, is_median_zero, is_flat, is_area_ratio, is_outside_range.
        min_size: Minimum intersection size to display.
        show_counts: Whether to annotate intersection sizes.
    """
```

集合名称映射：

| 列名 | 显示名 |
|------|--------|
| `is_median_zero` | 中位数为零 |
| `is_flat` | 序列平坦 |
| `is_area_ratio` | 面积偏差 |
| `is_outside_range` | 记录面积越界 |

## 实施计划

### Phase 1: 重构 quality 模块

| Step | 操作 | 涉及文件 |
|------|------|---------|
| 1.1 | 新建 `metrics.py`，从 `compute.py` 和 `comparison.py` 提取纯计算函数 | `quality/metrics.py`（新建） |
| 1.2 | 新建 `filters/__init__.py`，定义 `AnomalyFilter` Protocol、`AnomalyFlag`、`LakeContext` | `quality/filters/__init__.py`（新建） |
| 1.3 | 新建 `filters/median_zero.py` | `quality/filters/median_zero.py`（新建） |
| 1.4 | 新建 `filters/flatness.py`，含 `FlatnessFilterConfig` | `quality/filters/flatness.py`（新建） |
| 1.5 | 新建 `filters/area_ratio.py`，含 `AreaRatioConfig` | `quality/filters/area_ratio.py`（新建） |
| 1.6 | 新建 `filters/outside_range.py` | `quality/filters/outside_range.py`（新建） |
| 1.7 | 新建 `classify.py`，含 `classify_area_anomaly()` 和 `default_filters()` | `quality/classify.py`（新建） |
| 1.8 | 精简 `comparison.py`，计算函数改为从 `metrics` import | `quality/comparison.py` |
| 1.9 | 更新 `__init__.py`，保持公共 API 不变 | `quality/__init__.py` |
| 1.10 | 删除 `compute.py` | `quality/compute.py` |
| 1.11 | 删除 `run_comparison.py` | `quality/run_comparison.py` |
| 1.12 | 更新下游 import：`run_quality.py`、`migrate_flat_quality_to_anomalies.py`、`run_area_comparison.py` | `scripts/*.py` |
| 1.13 | 运行测试验证重构正确性 | — |

### Phase 2: 扩展 area_anomalies 表 schema

| Step | 操作 | 涉及文件 |
|------|------|---------|
| 2.1 | 修改 `_ensure_area_anomalies_table_sql()`，增加 4 个布尔列 | `lakesource/postgres/lake.py` |
| 2.2 | 修改 `_upsert_area_anomalies_sql()`，写入布尔列 | `lakesource/postgres/lake.py` |
| 2.3 | 修改 `_move_area_quality_to_anomalies_sql()`，写入布尔列 | `lakesource/postgres/lake.py` |
| 2.4 | 同步 Parquet provider 中 `area_anomalies` 相关逻辑 | `lakesource/provider/parquet_provider.py` |
| 2.5 | 同步 Parquet 导出脚本 | `lakesource/scripts/export_to_parquet.py` |

### Phase 3: 更新 pipeline + 迁移脚本

| Step | 操作 | 涉及文件 |
|------|------|---------|
| 3.1 | 更新 `run_quality.py`，使用新 API（`LakeContext` + `default_filters`），anomaly row 增加布尔字段 | `scripts/run_quality.py` |
| 3.2 | CLI 增加 `--area-ratio-min`、`--area-ratio-max` 参数 | `scripts/run_quality.py` |
| 3.3 | 新建 `migrate_area_ratio_to_anomalies.py` 迁移脚本 | `scripts/migrate_area_ratio_to_anomalies.py`（新建） |
| 3.4 | 迁移脚本：扫描 `area_quality`，用 `AreaRatioFilter` 判定 ratio 异常 | 同上 |
| 3.5 | 迁移脚本：回填已有 `area_anomalies` 行的 `is_median_zero`、`is_flat`、`is_outside_range` 标志 | 同上 |
| 3.6 | 迁移脚本：支持 `--dry-run`、`--limit-id`、`--area-ratio-min`、`--area-ratio-max` | 同上 |

### Phase 4: UpSet Plot 可视化

| Step | 操作 | 涉及文件 |
|------|------|---------|
| 4.1 | 添加 `upsetplot >=0.9` 依赖 | `lakeviz/pyproject.toml` |
| 4.2 | 实现 `plot_anomaly_upset()` 函数 | `lakeviz/quality/plot.py` |
| 4.3 | 更新 `lakeviz/quality/__init__.py` 导出 | `lakeviz/quality/__init__.py` |
| 4.4 | 新建 `scripts/plot_anomaly_upset.py` 绘图脚本 | `lakeanalysis/scripts/plot_anomaly_upset.py`（新建） |

### Phase 5: 清理

| Step | 操作 | 涉及文件 |
|------|------|---------|
| 5.1 | 移除 `TRIGGER_WRITE` 常量 | `lakeanalysis/batch/protocol.py` |
| 5.2 | 移除 `WorkerState.WRITING` 状态 | `lakeanalysis/batch/protocol.py` |
| 5.3 | 更新 `worker.py` 文件头状态机注释 | `lakeanalysis/batch/worker.py` |
| 5.4 | 清理 `manager.py` 中对 `TRIGGER_WRITE` 的 import（如有） | `lakeanalysis/batch/manager.py` |

## 执行顺序

```
Phase 1 (重构) → Phase 2 (schema) → Phase 3 (pipeline + 迁移) → Phase 4 (UpSet Plot) → Phase 5 (清理)
```

每个 Phase 完成后运行测试验证，确保无回归。

## 迁移脚本设计

### `migrate_area_ratio_to_anomalies.py`

**目的**：处理存量 `area_quality` 数据，将 ratio 偏差大的湖泊移入 `area_anomalies`。

**流程**：

1. 扫描 `area_quality` 表，对每个湖泊构建 `LakeContext`
2. 运行 `AreaRatioFilter.classify()`，收集 `is_area_ratio = True` 的 hylak_id
3. 将这些湖泊从 `area_quality` 移入 `area_anomalies`（设 `is_area_ratio = TRUE`）
4. 对已有 `area_anomalies` 行，回填 `is_median_zero`、`is_flat`、`is_outside_range` 标志
   - 需要从原始 `lake_area` 数据重新计算（因为 `area_anomalies` 表之前没有这些列）
   - 或者从 `rs_area_median` 和 `rs_area_mean` 推算（`median_zero` 可直接判定，`flat` 需要原始序列）

**回填策略**：

| 标志 | 回填方式 |
|------|---------|
| `is_median_zero` | 直接从 `area_anomalies.rs_area_median == 0` 判定，无需原始序列 |
| `is_flat` | 需要从 `lake_area` 重新计算 flatness metrics |
| `is_outside_range` | 需要从 `lake_area` 重新计算 min/max，结合 `atlas_area` 判定 |
| `is_area_ratio` | 迁移脚本直接设置 |

**CLI 参数**：

```
--dry-run              只检测，不迁移
--limit-id N           只处理 hylak_id < N
--area-ratio-min R     最小 ratio（默认 0.1）
--area-ratio-max R     最大 ratio（默认 10.0）
--skip-backfill        跳过已有行的布尔标志回填
--move-batch-size N    迁移批次大小（默认 5000）
```

## 向后兼容

### 公共 API 保持不变

以下符号仍从 `lakeanalysis.quality` 顶层导出：

- `classify_area_anomaly` — 签名变更（接受 `LakeContext` + `filters`），但旧调用方通过 `default_filters()` 可等价替换
- `FlatnessFilterConfig` — 从 `filters.flatness` 重导出
- `AgreementConfig` — 从 `metrics` 重导出
- `compute_area_ratio`、`compute_relative_diff`、`compute_log2_ratio` — 从 `metrics` 重导出
- `enrich_comparison_df`、`summarize_comparison` — 从 `comparison` 重导出
- `compute_median_area`、`compute_mean_area` — 从 `metrics` 重导出
- `is_anomalous` — 保留为 `metrics.is_anomalous` 的重导出（或标记 deprecated）
- `compute_flatness_metrics`、`compute_area_range` — 从 `metrics` 重导出
- `classify_outside_range` — 从 `filters.outside_range` 重导出
- `classify_agreement` — 从 `metrics` 重导出

### `classify_area_anomaly()` 签名变更

旧签名：
```python
def classify_area_anomaly(
    df: pd.DataFrame,
    rs_area_median: float,
    config: FlatnessFilterConfig,
) -> dict[str, bool | float]:
```

新签名：
```python
def classify_area_anomaly(
    ctx: LakeContext,
    filters: list[AnomalyFilter],
) -> dict[str, bool | float]:
```

调用方迁移：
```python
# 旧
result = classify_area_anomaly(df, rs_area_median, flat_config)

# 新
ctx = LakeContext(df=df, rs_area_median=rs_area_median,
                  rs_area_mean=rs_area_mean, atlas_area=atlas_area)
filters = default_filters(flat_config=flat_config, ratio_config=ratio_config)
result = classify_area_anomaly(ctx, filters)
```

## 风险与缓解

| 风险 | 缓解措施 |
|------|---------|
| 重构导致下游 import 断裂 | Phase 1 完成后立即运行全量测试 |
| `area_anomalies` 表 schema 变更需要 ALTER TABLE | 新增列均有 DEFAULT 值，`ALTER TABLE ADD COLUMN` 不影响现有数据 |
| 迁移脚本对存量数据误操作 | 支持 `--dry-run`，先检测再执行 |
| `upsetplot` 库与现有 matplotlib 版本冲突 | `upsetplot >=0.9` 要求 `matplotlib >=3.5`，项目已满足 |
| `is_anomalous()` 函数被外部直接调用 | 保留为 `metrics.is_anomalous` 的重导出，标记为 deprecated |
