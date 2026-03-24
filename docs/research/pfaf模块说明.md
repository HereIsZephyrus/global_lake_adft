# pfaf 模块说明

## 概述

`pfaf` 模块实现了两项功能：

1. **Pfafstetter 流域编码关联**（`lookup` + `store`）：将 ALTAS_DB 中 HydroATLAS 流域的 `pfaf_id` 与 SERIES_DB 中的湖泊记录关联，即通过判断湖泊质心是否落在流域几何范围内，确定每个湖泊所属的 lev11 级 Pfafstetter 编码。

2. **最近自然湖泊搜索**（`nearest` + `store`）：对 SERIES_DB 中每个非自然湖泊（`lake_type > 1`），在流域层级拓扑结构上查找最近的自然湖泊（`lake_type = 1`），并记录拓扑距离（共同前缀层数）。

---

## 数据库依赖

| 数据库 | 角色 | 关键表/字段 |
|---|---|---|
| `ALTAS_DB` | HydroATLAS 空间数据库（PostGIS）| `BasinATLAS_v10_lev05`、`BasinATLAS_v10_lev11`，各含 `pfaf_id`（double precision）、`geom` |
| `SERIES_DB` | 时序与湖泊信息库 | `lake_info`（`hylak_id`, `lake_type`, `centroid`, `lake_area`）、`lake_pfaf`（写入目标）、`af_nearest`（写入目标）|

两个数据库为独立 PostgreSQL 实例，不支持跨库 JOIN，数据通过 Python 内存中转。

---

## 文件结构

```
pfaf/
├── __init__.py      # 公共 API 导出
├── lookup.py        # Pfafstetter 编码空间查找（跨数据库两阶段空间 JOIN）
├── store.py         # 结果写回 SERIES_DB（lake_pfaf、af_nearest 表的 DDL + UPSERT）
└── nearest.py       # 最近自然湖泊搜索（内存跳表索引）

scripts/
├── run_pfaf.py      # pfaf 查找流水线入口
└── run_nearest.py   # 最近自然湖泊搜索入口
```

---

## 功能一：Pfafstetter 编码关联

### 原理

Pfafstetter 编码体系按流域层级嵌套编号，HydroATLAS 提供 lev01–lev12 共 12 级。本模块利用其中两级实现两阶段空间查找以提升效率：

- **lev05**（5 位整数，约数千个流域）：全量空间 JOIN，快速定位湖泊质心所在的大流域，得到 5 位前缀。
- **lev11**（11 位整数，百万量级流域）：利用 5 位前缀过滤候选集（数值范围条件），再做精确空间 JOIN。

由于 `pfaf_id` 在 HydroATLAS 中以 `double precision` 存储（Shapefile 格式限制，无整数类型），不能直接做整数除法——`pfaf_id / 1000000` 会得到小数。前缀过滤改用范围条件：

```sql
b11.pfaf_id >= b5.pfaf_id * 1000000.0
AND b11.pfaf_id <  (b5.pfaf_id + 1) * 1000000.0
```

该写法避免对列施函数，B-tree 索引可正常生效。

### 跨数据库流程

```
SERIES_DB                    Python                    ALTAS_DB
lake_info.centroid  ──WKT──▶  临时表 _lake_centroids  ──空间JOIN──▶  lev05 → lev11
                                                                         │
SERIES_DB ◀──UPSERT──── lake_pfaf(hylak_id, pfaf_id) ◀──────────────────┘
```

1. 从 SERIES_DB 的 `lake_info.centroid`（预计算质心，已有几何字段，无需 `ST_Centroid`）读取 WKT。
2. 以 `ST_SetSRID(ST_GeomFromText(wkt), 4326)` 批量插入 ALTAS_DB 临时表（`ON COMMIT DROP`）。
3. 执行两阶段空间 JOIN，结果写回 SERIES_DB 的 `lake_pfaf` 表。

> **注意**：`lake_info.centroid` 存储坐标为 `(lat, lon)` 顺序（Y, X），与 PostGIS 期望的 `(lon, lat)` 相反，读取时需注意坐标含义。WKT 已正确反映数据库存储的坐标顺序，ALTAS_DB 的空间查询可正常命中。

### 分块处理与断点续传

大规模数据集通过 `ChunkedLakeProcessor` 按 `hylak_id` 范围分块处理。每块处理完立即 UPSERT，通过比较 `lake_pfaf` 与 `lake_info` 的行数判断该块是否已完成，支持中断后安全恢复。

### 输出表：`lake_pfaf`

| 字段 | 类型 | 说明 |
|---|---|---|
| `hylak_id` | INTEGER PK | 湖泊 ID |
| `pfaf_id` | BIGINT | lev11 Pfafstetter 编码，NULL 表示质心不在 HydroATLAS 覆盖范围内（如冰盖）|
| `computed_at` | TIMESTAMPTZ | 计算时间 |

### 运行方式

```bash
# 全量运行（分块、可断点续传）
uv run python scripts/run_pfaf.py

# 测试模式（只处理 hylak_id < 5000 的湖泊）
uv run python scripts/run_pfaf.py --limit-id 5000

# 调整分块大小
uv run python scripts/run_pfaf.py --chunk-size 5000
```

---

## 功能二：最近自然湖泊搜索

### 背景

HydroLAKES 按成因将湖泊分为三类：

| `lake_type` | 含义 |
|---|---|
| 1 | 自然湖泊 |
| 2 | 水库 |
| 3 | 受人类活动影响的湖泊 |

对于每个 type 2/3 的湖泊，需要找到拓扑上最近的 type 1 自然湖泊，同时限制两者面积不能相差过大（默认 10 倍以内）。

### 拓扑距离定义

两个湖泊的**拓扑距离**用 Pfafstetter 编码的**共同前缀层数**（`topo_level`）衡量：

```
湖泊 A  pfaf_id = 32263 543 21   (lev11)
湖泊 B  pfaf_id = 32263 543 18   → 前 9 位相同 → topo_level = 9（同 lev09 流域）
湖泊 C  pfaf_id = 32263 111 22   → 前 5 位相同 → topo_level = 5（同 lev05 流域）
湖泊 D  pfaf_id = 41100 000 11   → 前 1 位相同 → topo_level = 1（同大洲级流域）
```

`topo_level` 越大，说明两湖共同所属的流域级别越细，拓扑上越近。

### 跳表搜索算法

类比跳表的多层索引结构，在内存中对全部 type-1 湖泊构建 11 层前缀字典（每层一个 `dict[prefix → [row_indices]]`），每层覆盖一个 Pfafstetter 级别：

```
layer 11 (最细): key = pfaf_id // 1           → 同 lev11 流域
layer 10:        key = pfaf_id // 10          → 同 lev10 流域
layer 9:         key = pfaf_id // 100         → 同 lev09 流域
...
layer 1  (最粗): key = pfaf_id // 10000000000 → 同 lev01 流域
```

搜索时从 layer 11 开始向粗粒度方向遍历，**命中即返回**（短路求值）：

```
对每个 type>1 湖泊:
  for level in [11, 10, 9, ..., 1]:
      candidates = prefix_index[level][pfaf_id // divisor[level]]
      if candidates 为空:
          continue
      过滤面积比超过阈值的候选
      if 过滤后为空:
          continue  ← 本层无满足条件的候选，继续向上
      返回候选中地理距离最近的 (hylak_id, topo_level)
  返回 None  ← 任何层级均无满足条件的候选
```

**效率优势**：
- 全部数据仅加载一次，搜索在内存中完成，无额外数据库往返。
- 大多数湖泊在 lev07–lev10 即可命中，实际迭代层数远少于 11。
- NumPy 向量化面积过滤和距离计算，批量处理候选集。

### 面积约束

同层候选集中，要求两湖面积比满足：

$$\max\!\left(\frac{A_{\text{cand}}}{A_{\text{query}}},\ \frac{A_{\text{query}}}{A_{\text{cand}}}\right) \leq R$$

其中 $R$ 为 `max_area_ratio`（默认 10.0），即任意一方的面积不超过另一方的 10 倍。

若查询湖或候选湖的面积数据缺失（NULL 或 ≤ 0），则跳过面积约束，该候选仍参与地理距离比较。

若当前层所有候选均不满足面积约束，搜索继续向上一层（更粗粒度）查找，而非直接返回 None。

### 输出表：`af_nearest`

| 字段 | 类型 | 说明 |
|---|---|---|
| `hylak_id` | INTEGER PK | type>1 湖泊的 ID |
| `lake_type` | SMALLINT | 湖泊类型（2 或 3）|
| `nearest_id` | INTEGER | 最近 type-1 湖泊的 `hylak_id`，NULL 表示未找到满足条件的候选 |
| `topo_level` | SMALLINT | 共同前缀层数（1–11），NULL 同上 |
| `computed_at` | TIMESTAMPTZ | 计算时间 |

### 运行方式

```bash
# 全量运行
uv run python scripts/run_nearest.py

# 测试模式
uv run python scripts/run_nearest.py --limit-id 5000

# 自定义面积比阈值（默认 10.0）
uv run python scripts/run_nearest.py --max-area-ratio 5.0

# 禁用面积约束
uv run python scripts/run_nearest.py --max-area-ratio inf
```

---

## 公共 API

```python
from pfaf import (
    # Pfafstetter 编码查找
    fetch_lake_centroids,          # 从 SERIES_DB 读取质心列表
    fetch_lake_centroids_chunk,    # 按 hylak_id 范围分块读取
    lookup_pfaf_chunk,             # 对一批质心执行两阶段空间 JOIN
    lookup_pfaf_ids,               # 全量查找（内部调用 lookup_pfaf_chunk）

    # 最近自然湖搜索
    compute_nearest_naturals,      # 内存跳表搜索，返回结果列表

    # 数据库写入
    ensure_lake_pfaf_table,        # 建表（lake_pfaf）
    upsert_lake_pfaf,              # 写入 pfaf_id 映射
    ensure_af_nearest_table,       # 建表（af_nearest）
    upsert_af_nearest,             # 写入最近自然湖结果
)
```

---

## 日志

所有模块使用标准 `logging`，通过 `scripts/` 入口脚本的 `Logger` 类同时输出到控制台和带时间戳的日志文件（`logs/run_pfaf_YYYYMMDD_HHMMSS.log`、`logs/run_nearest_YYYYMMDD_HHMMSS.log`）。

关键日志级别：
- `INFO`：每个分块的处理进度、匹配数量、UPSERT 行数。
- `DEBUG`：单次查询行数、跳过的分块。
