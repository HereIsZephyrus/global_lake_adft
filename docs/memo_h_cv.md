# Memo: H×CV Flat-Series Detection

## 背景

当前 PV (Penalized Volatility) filter 使用 `std_pct_change / sqrt(n_zero_delta)` 检测 flat series，存在两个误判路径：

1. **n_zero_delta 放大**：高纬度湖非 frozen 月跨 gap 后面积恰好相同，sqrt(n_zero_delta) 惩罚过重
   - 例：Lake 799843 — 91 个非 frozen 月，49 次 delta=0，PV=0.00127 < 0.002 阈值，但实际是正常小湖
2. **std_pct_change 本身极低**：变幅小的正常湖，pct_change 天然就小
   - 例：Lake 105936 — 112 个非 frozen 月，25 个不同值，变幅 ±0.6%，PV=0.00171 < 0.002，完全正常

## 方案：H×CV 替代 PV

### 指标定义

- **H**：离散 Shannon 熵，`H = -Σ p_i * log2(p_i)`，衡量值分布的离散程度
  - flat series（值少且集中）→ H 低
  - 正常湖（值多且均匀）→ H 高
- **CV**：变异系数，`CV = std(water_area) / mean(water_area)`
  - 变幅小 → CV 低
  - 变幅大 → CV 高
- **H×CV**：熵加权变异系数
  - flat series：H 低 × CV 低 → 极低
  - 正常小变幅湖（如 105936）：H 高 × CV 低 → 中等，不会被误判
  - 正常大变幅湖：H 高 × CV 高 → 高

### 区分度验证

30 个 pv 湖 vs 30 个正常湖的采样结果：

| 指标 | 分离度 (normal/pv) |
|------|-------------------|
| H    | 2.30x             |
| CV   | 4.81x             |
| old PV | 9.63x           |
| **H×CV** | **10.80x**    |

关键案例：
- Lake 799843：H=1.14, CV=0.0068, H×CV=0.0077 → 被标记（正确，值少且集中）
- Lake 105936：H=4.27, CV=0.0031, H×CV=0.0133 → 不被误判（值多且均匀，熵拉回来了）

## 实现计划

### Phase 1: 数据库表与写入函数

#### 1.1 新增表 `area_entropy_cv`

```sql
CREATE TABLE IF NOT EXISTS area_entropy_cv (
    hylak_id       INTEGER PRIMARY KEY,
    n_obs          INTEGER,
    n_distinct     INTEGER,
    dominant_ratio DOUBLE PRECISION,
    cv             DOUBLE PRECISION,
    H              DOUBLE PRECISION,
    h_cv           DOUBLE PRECISION,
    n_frozen       INTEGER,
    computed_at    TIMESTAMPTZ DEFAULT now()
);
```

#### 1.2 新增 `lake.py` 函数

- `_ensure_area_entropy_cv_table_sql(tc)` → CREATE TABLE
- `_upsert_area_entropy_cv_sql(tc)` → COPY + INSERT SELECT（同 `upsert_area_quality` 模式）
- `ensure_area_entropy_cv_table(conn)` → 调用 ensure SQL
- `upsert_area_entropy_cv(conn, rows)` → 调用 upsert SQL

#### 1.3 注册到 `postgres/__init__.py`

在 `_LAKE_SYMBOLS` 中添加 `"ensure_area_entropy_cv_table"` 和 `"upsert_area_entropy_cv"`。

在 `__all__` 中添加对应导出。

### Phase 2: Provider 层集成

#### 2.1 `PostgresLakeProvider.persist`

在 `_get_upsert_fn` 的 `_FNS` 字典中添加：

```python
"area_entropy_cv": "upsert_area_entropy_cv",
```

#### 2.2 `PostgresLakeProvider.ensure_schema`

添加分支：

```python
elif algorithm == "area_entropy_cv":
    from lakesource.postgres import ensure_area_entropy_cv_table
    ensure_area_entropy_cv_table(conn)
```

### Phase 3: 探索脚本 `explore_entropy_cv.py`

#### 3.1 架构

- 走 `lakesource` provider 抽象层：`SourceConfig` → `create_provider()` → `provider`
- 分 chunk 遍历 `lake_info`（用 `ChunkedLakeProcessor`，`done_table="area_entropy_cv"`）
- 每个 chunk 执行一条 SQL 计算指标
- 通过 `provider.persist({"area_entropy_cv": rows})` 写入 DB
- 全部 chunk 完成后，从 `area_entropy_cv` 表读全量数据画图

#### 3.2 Chunk SQL

所有表扫描限制在 `[chunk_start, chunk_end)` 范围内，走索引：

```sql
WITH non_frozen AS (
    SELECT la.hylak_id, la.water_area
    FROM lake_area la
    WHERE la.hylak_id >= %(chunk_start)s AND la.hylak_id < %(chunk_end)s
      AND NOT EXISTS (
          SELECT 1 FROM anomaly a
          WHERE a.hylak_id = la.hylak_id
            AND a.year_month = la.year_month
            AND a.anomaly_type = 'frozen'
      )
),
value_counts AS (
    SELECT hylak_id, water_area, COUNT(*) AS cnt
    FROM non_frozen GROUP BY hylak_id, water_area
),
lake_totals AS (
    SELECT hylak_id, SUM(cnt) AS n_obs FROM value_counts GROUP BY hylak_id
),
entropy AS (
    SELECT vc.hylak_id,
           -SUM((vc.cnt::float8/lt.n_obs) * LN(vc.cnt::float8/lt.n_obs)/LN(2.0)) AS H
    FROM value_counts vc JOIN lake_totals lt ON lt.hylak_id = vc.hylak_id
    GROUP BY vc.hylak_id
),
stats AS (
    SELECT vc.hylak_id, lt.n_obs,
           AVG(vc.water_area) AS mean_area, STDDEV(vc.water_area) AS std_area,
           COUNT(*) AS n_distinct, MAX(vc.cnt) AS dominant_count
    FROM value_counts vc JOIN lake_totals lt ON lt.hylak_id = vc.hylak_id
    GROUP BY vc.hylak_id, lt.n_obs
),
frozen_counts AS (
    SELECT hylak_id, COUNT(*) AS n_frozen
    FROM anomaly
    WHERE anomaly_type = 'frozen'
      AND hylak_id >= %(chunk_start)s AND hylak_id < %(chunk_end)s
    GROUP BY hylak_id
)
SELECT s.hylak_id, s.n_obs, s.n_distinct,
       s.dominant_count::float8/s.n_obs AS dominant_ratio,
       CASE WHEN s.mean_area>0 THEN s.std_area/s.mean_area ELSE NULL END AS cv,
       e.H,
       CASE WHEN s.mean_area>0 THEN e.H*(s.std_area/s.mean_area) ELSE NULL END AS h_cv,
       COALESCE(fc.n_frozen,0) AS n_frozen
FROM stats s
JOIN entropy e ON e.hylak_id = s.hylak_id
LEFT JOIN frozen_counts fc ON fc.hylak_id = s.hylak_id
ORDER BY s.hylak_id
```

SQL 优化要点：
- `non_frozen` 用 `NOT EXISTS` 替代 `LEFT JOIN ... WHERE IS NULL`，chunk 范围下推
- `frozen_counts` 直接从 `anomaly` 表 COUNT，不 JOIN `lake_area`，chunk 范围下推
- 不需要 `frozen` CTE，减少中间结果集
- 所有表扫描走 `(hylak_id, ...)` 索引范围扫描

#### 3.3 脚本结构

```python
def main():
    load_env()
    source = SourceConfig()
    provider = create_provider(source)
    client = provider._client

    provider.ensure_schema("area_entropy_cv")

    processor = ChunkedLakeProcessor(
        series_db, chunk_size=chunk_size, done_table="area_entropy_cv"
    )

    def process_chunk(chunk_start, chunk_end):
        df = client.query_df(H_CV_SQL, params={...})
        return df.to_dict("records")

    def upsert_chunk(rows):
        provider.persist({"area_entropy_cv": rows})

    processor.run(process_fn=process_chunk, upsert_fn=upsert_chunk)

    # 画图
    df = client.query_df("SELECT * FROM area_entropy_cv")
    _plot_cdf(df, output_dir)
    _plot_distributions(df, output_dir)
    _plot_scatter_h_cv(df, output_dir)
    _print_summary(df)
```

#### 3.4 输出

1. **DB 表** `area_entropy_cv`：全量 ~140 万行
2. **CDF 图**：`figure/h_cv_cdf.png` — H×CV 累积分布 + 阈值线
3. **分布面板**：`figure/h_cv_distributions.png` — 2×3：H, CV, H×CV, n_distinct, dominant_ratio, n_frozen
4. **散点图**：`figure/h_vs_cv_scatter.png` — H vs CV，颜色=H×CV
5. **终端**：各指标 summary + H×CV 阈值分析

### Phase 4: 更新 PV Filter（待 CDF 确定阈值后）

1. 更新 `compute_penalized_volatility()` 在 `metrics.py` 中，改用 H×CV
2. 更新 `PenalizedVolatilityFilter` 阈值
3. 重新运行 `run_quality.py`

## 不做的事

- 不写 parquet 缓存，全量写入 DB
- 不采样
- 不 JOIN `area_anomalies` / `area_quality`
- 不标记 label（探索阶段不需要）
- 不修改 `LakeProvider` ABC（`persist` 和 `ensure_schema` 已是通用接口）

## 文件变更清单

| 文件 | 变更 |
|------|------|
| `packages/lakesource/src/lakesource/postgres/lake.py` | +4 函数：ensure table, upsert SQL, ensure fn, upsert fn |
| `packages/lakesource/src/lakesource/postgres/__init__.py` | +2 导出 |
| `packages/lakesource/src/lakesource/provider/postgres_provider.py` | +2 分支：persist _FNS, ensure_schema |
| `packages/lakeanalysis/scripts/explore_entropy_cv.py` | 新建，替换当前草稿 |
