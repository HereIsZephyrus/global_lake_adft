# artificial 模块说明

## 概述

`artificial` 模块统一管理人工湖相关分析，包含三个子模块：

1. **`pfaf`** — Pfafstetter 流域编码关联与最近自然湖泊搜索（配对）
2. **`similarity`** — 配对湖泊时序相似性（Pearson + ACF cosine）
3. **`impact`** — 人类活动对湖泊面积的影响评估（波动性 + 异常事件）

所有子模块共享统一的数据获取层 `fetch.py`，通过 SQL 级质量过滤（排除 `area_anomalies` 中的异常湖）获取配对和时序数据。

---

## 文件结构

```
artificial/
├── __init__.py          # 公共 API 导出（pfaf + similarity + impact）
├── fetch.py             # 统一数据获取（配对 + 时序 + 质量过滤）
├── pfaf/
│   ├── __init__.py      # pfaf 子模块 API
│   ├── lookup.py        # Pfafstetter 编码空间查找（跨数据库两阶段空间 JOIN）
│   ├── nearest.py       # 最近自然湖泊搜索（内存跳表索引）
│   └── store.py         # 结果写回 SERIES_DB（lake_pfaf、af_nearest 表的 DDL + UPSERT）
├── similarity/
│   ├── __init__.py      # similarity 子模块 API
│   └── compute.py       # Pearson 相关系数 + ACF 余弦相似性
└── impact/
    ├── __init__.py      # impact 子模块 API
    ├── metrics.py       # 波动性指标计算（CV、月际变化率、极差比）
    └── events.py        # Z-score 异常事件检测

scripts/
├── run_pfaf.py          # pfaf 查找流水线入口
├── run_nearest.py       # 最近自然湖泊搜索入口
├── run_similarity.py    # 相似性分析入口
└── run_impact.py        # 人类活动影响评估入口
```

---

## 子模块一：pfaf

### 功能

1. **Pfafstetter 流域编码关联**（`lookup` + `store`）：将 ALTAS_DB 中 HydroATLAS 流域的 `pfaf_id` 与 SERIES_DB 中的湖泊记录关联，即通过判断湖泊质心是否落在流域几何范围内，确定每个湖泊所属的 lev11 级 Pfafstetter 编码。

2. **最近自然湖泊搜索**（`nearest` + `store`）：对 SERIES_DB 中每个非自然湖泊（`lake_type > 1`），在流域层级拓扑结构上查找最近的自然湖泊（`lake_type = 1`），并记录拓扑距离（共同前缀层数）。

### 数据库依赖

| 数据库 | 角色 | 关键表/字段 |
|---|---|---|
| `ALTAS_DB` | HydroATLAS 空间数据库（PostGIS）| `BasinATLAS_v10_lev05`、`BasinATLAS_v10_lev11`，各含 `pfaf_id`（double precision）、`geom` |
| `SERIES_DB` | 时序与湖泊信息库 | `lake_info`（`hylak_id`, `lake_type`, `centroid`, `lake_area`）、`lake_pfaf`（写入目标）、`af_nearest`（写入目标）|

两个数据库为独立 PostgreSQL 实例，不支持跨库 JOIN，数据通过 Python 内存中转。

### Pfafstetter 编码关联

Pfafstetter 编码体系按流域层级嵌套编号，HydroATLAS 提供 lev01–lev12 共 12 级。本模块利用其中两级实现两阶段空间查找以提升效率：

- **lev05**（5 位整数，约数千个流域）：全量空间 JOIN，快速定位湖泊质心所在的大流域，得到 5 位前缀。
- **lev11**（11 位整数，百万量级流域）：利用 5 位前缀过滤候选集（数值范围条件），再做精确空间 JOIN。

由于 `pfaf_id` 在 HydroATLAS 中以 `double precision` 存储（Shapefile 格式限制，无整数类型），不能直接做整数除法——`pfaf_id / 1000000` 会得到小数。前缀过滤改用范围条件：

```sql
b11.pfaf_id >= b5.pfaf_id * 1000000.0
AND b11.pfaf_id <  (b5.pfaf_id + 1) * 1000000.0
```

该写法避免对列施函数，B-tree 索引可正常生效。

### 最近自然湖泊搜索

HydroLAKES 按成因将湖泊分为三类：

| `lake_type` | 含义 |
|---|---|
| 1 | 自然湖泊 |
| 2 | 水库 |
| 3 | 受人类活动影响的湖泊 |

对于每个 type 2/3 的湖泊，需要找到拓扑上最近的 type 1 自然湖泊，同时限制两者面积不能相差过大（默认 10 倍以内）。

拓扑距离用 Pfafstetter 编码的共同前缀层数（`topo_level`）衡量，越大说明两湖共同所属的流域级别越细，拓扑上越近。

搜索采用跳表算法：在内存中对全部 type-1 湖泊构建 11 层前缀字典，从最细层开始向粗粒度方向遍历，命中即返回。

面积约束要求：

$$\max\!\left(\frac{A_{\text{cand}}}{A_{\text{query}}},\ \frac{A_{\text{query}}}{A_{\text{cand}}}\right) \leq R$$

其中 $R$ 为 `max_area_ratio`（默认 10.0）。

### 输出表

**`lake_pfaf`**

| 字段 | 类型 | 说明 |
|---|---|---|
| `hylak_id` | INTEGER PK | 湖泊 ID |
| `pfaf_id` | BIGINT | lev11 Pfafstetter 编码，NULL 表示质心不在 HydroATLAS 覆盖范围内 |
| `computed_at` | TIMESTAMPTZ | 计算时间 |

**`af_nearest`**

| 字段 | 类型 | 说明 |
|---|---|---|
| `hylak_id` | INTEGER PK | type>1 湖泊的 ID |
| `lake_type` | SMALLINT | 湖泊类型（2 或 3）|
| `nearest_id` | INTEGER | 最近 type-1 湖泊的 `hylak_id`，NULL 表示未找到 |
| `topo_level` | SMALLINT | 共同前缀层数（1–11），NULL 同上 |
| `computed_at` | TIMESTAMPTZ | 计算时间 |

### 运行方式

```bash
# pfaf 查找
uv run python scripts/run_pfaf.py
uv run python scripts/run_pfaf.py --limit-id 5000

# 最近自然湖搜索
uv run python scripts/run_nearest.py
uv run python scripts/run_nearest.py --max-area-ratio 5.0
```

---

## 公共数据获取层：fetch

`artificial/fetch.py` 提供 `load_pairs_and_areas()` 函数，是 similarity 和 impact 子模块共用的数据入口：

1. 通过 `fetch_impact_pairs()` 从 `af_nearest` 获取 `topo_level > 8` 的配对
2. 在 SQL 层面通过 `LEFT JOIN area_anomalies` 排除异常湖（避免加载 68 万行到 Python）
3. 通过 `fetch_lake_area_by_ids()` 获取配对湖泊的月度时序数据

---

## 子模块二：similarity

### 功能

计算人工湖与配对自然湖的时序相似性：

1. **Pearson 相关系数**：衡量两湖面积时序的线性相关程度
2. **ACF 余弦相似性**（12 个月延迟）：比较两湖自相关结构的相似性

### 数据流

```
af_nearest (topo_level > 8, quality-filtered)
  + lake_area (时序)
  → similarity.csv + 图表
```

### 输出

**CSV**（`data/similarity/similarity.csv`）：每对一行，含 pearson_r、acf_cos_sim、n_common

### 运行方式

```bash
uv run python scripts/run_similarity.py
uv run python scripts/run_similarity.py --limit-pairs 500
uv run python scripts/run_similarity.py --plot
```

---

## 子模块三：impact

### 功能

评估人类活动对湖泊面积的影响，通过对比人工湖与配对自然湖的波动性和异常事件：

1. **波动性指标**：变异系数（CV）、月际变化率 std、极差比
2. **异常事件检测**：Z-score 方法标记 |z| > 阈值的月份为异常事件
3. **配对差异**：ΔCV、Δ月际变化率、Δ极差比、Δ异常月占比、人工湖独有异常事件数

### 数据流

```
af_nearest (topo_level > 8, quality-filtered)
  + lake_area (时序)
  → impact.csv + 图表
```

### 波动性指标

| 指标 | 定义 | 含义 |
|------|------|------|
| CV | std / mean | 归一化波动幅度 |
| pct_change_std | std(month-over-month % change) | 月度变化剧烈程度 |
| range_ratio | (max - min) / mean | 极端变化幅度 |

### Z-score 异常事件

对每个湖泊计算月度 water_area 的均值和标准差，标记 |z| > 阈值（默认 3.0）的月份为异常事件。统计异常月数量、占比，以及人工湖独有异常事件（人工湖异常但自然湖同期正常的月数）。

### 输出

**CSV**（`data/impact/impact.csv`）：每对一行，含双方指标 + 差异指标

**图表**（`data/impact/plot/`）：
- `volatility_comparison.png` — 人工 vs 自然波动性指标箱线图
- `delta_cv_distribution.png` — ΔCV 分布直方图
- `anomaly_ratio_comparison.png` — 异常月占比对比
- `timeline_{hylak_id}_{nearest_id}.png` — ΔCV 最大的典型案例时序对比

### 运行方式

```bash
# 全量运行
uv run python scripts/run_impact.py

# 限制配对数
uv run python scripts/run_impact.py --limit-pairs 500

# 自定义 Z-score 阈值
uv run python scripts/run_impact.py --z-threshold 2.5

# 生成图表
uv run python scripts/run_impact.py --plot

# 仅从已有 CSV 生成图表
uv run python scripts/run_impact.py --plot-only
```

---

## 公共 API

```python
from lakeanalysis.artificial import (
    # 数据获取（共用）
    load_pairs_and_areas,

    # pfaf: Pfafstetter 编码查找
    fetch_lake_centroids,
    fetch_lake_centroids_chunk,
    lookup_pfaf_chunk,
    lookup_pfaf_ids,

    # pfaf: 最近自然湖搜索
    compute_nearest_naturals,

    # pfaf: 数据库写入
    ensure_lake_pfaf_table,
    upsert_lake_pfaf,
    ensure_af_nearest_table,
    upsert_af_nearest,

    # similarity: 相似性
    pearson_correlation,
    acf_cosine_similarity,
    compute_pair_similarity,
    align_series,

    # impact: 波动性指标
    compute_cv,
    compute_pct_change_std,
    compute_range_ratio,
    compute_lake_metrics,
    compute_pair_metrics,

    # impact: 异常事件
    detect_zscore_events,
    compute_event_stats,
    compute_pair_events,
)
```

---

## 日志

所有模块使用标准 `logging`，通过 `scripts/` 入口脚本的 `Logger` 类同时输出到控制台和带时间戳的日志文件。

关键日志级别：
- `INFO`：每个分块的处理进度、匹配数量、UPSERT 行数。
- `DEBUG`：单次查询行数、跳过的分块。
