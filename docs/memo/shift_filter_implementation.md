# shift_filter 单独运行实现计划

## 背景

当前 `shift_filter` 尚未运行：
- `area_shift_labels` 表为空（0 行记录）
- `area_anomalies` 表中没有任何记录设置 shift 标志位（`anomaly_flags & 32 = 0`）

需要全量运行 shift_filter，计算 `area_shift_labels` 表，并同步更新 `area_quality` 和 `area_anomalies` 表。

## 数据流

```
parquet lake_area + frozen_months
    ↓
ShiftLabelsCalculator (batch Engine)
    ↓
area_shift_labels.parquet
    ↓
upsert_shift_labels_from_parquet()  [TRUNCATE + 全量插入 DB]
    ↓
sync_shift_to_anomalies()          [同步 quality ↔ anomalies]
```

## 数据库表结构

### area_shift_labels 表

```sql
CREATE TABLE area_shift_labels (
    hylak_id                      INTEGER PRIMARY KEY,
    shift_label                   TEXT NOT NULL,  -- 'stable' / 'degraded' / 'intermittent'
    udmax                         DOUBLE PRECISION,
    udmax_p_value                 DOUBLE PRECISION,
    udmax_break_index             INTEGER,
    wdmax                         DOUBLE PRECISION,
    wdmax_p_value                 DOUBLE PRECISION,
    wdmax_break_index             INTEGER,
    used_deseasoned               BOOLEAN,
    seasonality_dominance_ratio   DOUBLE PRECISION,
    computed_at                   TIMESTAMPTZ DEFAULT now()
);
```

## sync_shift_to_anomalies() 逻辑

### 对 `shift_label='degraded'` 的湖

| 当前状态 | 操作 |
|---------|------|
| 在 `area_quality` | 查 `rs_area_mean/median/atlas_area` → 插入 `area_anomalies`（`anomaly_flags=FLAG_SHIFT(32)`）→ 从 `area_quality` 删除 |
| 在 `area_anomalies`，无 `FLAG_SHIFT` | `anomaly_flags \|= FLAG_SHIFT` |
| 在 `area_anomalies`，已有 `FLAG_SHIFT` | 跳过 |

### 对 `shift_label='intermittent'/'stable'` 的湖

| 当前状态 | 操作 |
|---------|------|
| 在 `area_anomalies`，仅有 `FLAG_SHIFT`（无其他 flag） | 移回 `area_quality` |
| 其他情况 | 跳过 |

## 实现文件清单

### 1. config.toml 修改

**文件**: `packages/lakesource/config.toml`

在 `[tables.series_db]` 和 `[tables.parquet]` 中添加：

```toml
area_shift_labels = "area_shift_labels"
```

### 2. lakesource/postgres/area_quality.py 修改

**新增函数**：

- `_ensure_area_shift_labels_table_sql(tc)` - 建表 SQL
- `_upsert_area_shift_labels_sql(tc)` - 批量 upsert SQL
- `_truncate_area_shift_labels_sql(tc)` - truncate SQL
- `ensure_area_shift_labels_table(conn, tc)` - 确保表存在
- `upsert_area_shift_labels(conn, rows, tc)` - 批量 upsert
- `truncate_area_shift_labels(conn, tc)` - 清空表

### 3. lakeanalysis/batch/task_spec.py 修改

**新增 TaskSpec**：

```python
@dataclass(frozen=True)
class ShiftLabelsSpec(TaskSpec):
    name: str = "shift_labels"
    done_table: str = "area_shift_labels"
    done_requires_status: bool = False
    ensure_tables: tuple[str, ...] = ()
```

### 4. lakeanalysis/quality/shift_labels_calculator.py 新建

**ShiftLabelsCalculator** (实现 `Calculator` 接口)：

```python
class ShiftLabelsCalculator(Calculator):
    def _compute_lake(self, task: LakeTask) -> dict:
        """调用 ShiftFilter.classify()，返回 AnomalyFlag.detail + label"""

    def result_to_rows(self, result: dict) -> dict[str, list[dict]]:
        """返回 {"area_shift_labels": [ {...} ]}"""
```

### 5. lakeanalysis/quality/shift_labels_runner.py 新建

**核心函数**：

- `upsert_shift_labels_from_parquet(parquet_path, provider)` - 读取 parquet，truncate + 全量插入 DB
- `sync_shift_to_anomalies(provider)` - 根据 area_shift_labels 表同步更新 area_quality / area_anomalies

**Sync 伪逻辑**：

```python
def sync_shift_to_anomalies(provider):
    # 1. 获取所有 area_shift_labels 中 label='degraded' 的湖
    degraded_ids = fetch_degraded_lake_ids()

    # 2. 获取当前 area_quality 和 area_anomalies 的状态
    all_status = provider.fetch_area_statuses()  # {hylak_id: (table, flags)}

    # 3. 对 degraded 湖处理
    for hylak_id in degraded_ids:
        table, flags = all_status[hylak_id]
        if table == "quality":
            # 移入 area_anomalies
            row = fetch_quality_row(hylak_id)
            delete_from_quality(hylak_id)
            upsert_area_anomalies({**row, "anomaly_flags": FLAG_SHIFT})
        elif table == "anomalies":
            if flags & FLAG_SHIFT == 0:
                update_anomaly_flags(hylak_id, flags | FLAG_SHIFT)
        # else: 已有 FLAG_SHIFT，跳过

    # 4. 对 intermittent/stable 且只有 FLAG_SHIFT 的湖移回 quality
    shift_only_ids = fetch_shift_only_anomalies()  # 只有 FLAG_SHIFT 的湖
    for hylak_id in shift_only_ids:
        row = fetch_anomaly_row(hylak_id)
        delete_from_anomalies(hylak_id)
        upsert_quality(row)  # anomaly_flags 不需要写入 quality
```

### 6. lakeanalysis/scripts/compute_shift_labels.py 新建

**CLI 入口**：

```bash
uv run python scripts/compute_shift_labels.py \
    --parquet-dir data/parquet \
    --output-parquet data/parquet/area_shift_labels.parquet \
    --chunk-size 10000 \
    --limit-id 1400000 \
    --reset
```

**流程**：

1. 构建 `ShiftLabelsCalculator`
2. 用 `Engine` 运行 → 生成 `area_shift_labels.parquet`
3. 调用 `upsert_shift_labels_from_parquet()` → 写入 DB
4. 调用 `sync_shift_to_anomalies()` → 同步更新 quality/anomalies

## 涉及的文件列表

| 文件 | 操作 |
|------|------|
| `packages/lakesource/config.toml` | 修改 |
| `packages/lakesource/src/lakesource/postgres/area_quality.py` | 修改 |
| `packages/lakeanalysis/src/lakeanalysis/batch/task_spec.py` | 修改 |
| `packages/lakeanalysis/src/lakeanalysis/quality/shift_labels_calculator.py` | 新建 |
| `packages/lakeanalysis/src/lakeanalysis/quality/shift_labels_runner.py` | 新建 |
| `packages/lakeanalysis/scripts/compute_shift_labels.py` | 新建 |

## 依赖关系

```
config.toml
    ↓
area_quality.py (SQL functions)
    ↓
task_spec.py (ShiftLabelsSpec)
    ↓
shift_labels_calculator.py (ShiftLabelsCalculator)
    ↓
shift_labels_runner.py (upsert + sync functions)
    ↓
compute_shift_labels.py (CLI script)
```

## 常量

- `FLAG_SHIFT = 32`
- shift_label 值：`'stable'`, `'degraded'`, `'intermittent'`
