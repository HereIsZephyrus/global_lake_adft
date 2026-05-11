# Sync 命令实施计划

> **Status: DRAFT**

> 日期: 2026-05-11
> 背景: `/data/parquet` 下大量算法计算结果（EOT、Quantile、PWM 等）未同步到 PostgreSQL。当前仅有一个简陋的 `lake_adft export tables` 命令（只导出 area_quality/area_anomalies），缺乏通用的双向同步机制。需要创建统一的 `lake_adft sync` 子命令组，支持 status 查看、push（parquet→postgres）和 pull（postgres→parquet）。

---

## 0. 目标

1. 删除现有 `export.py`，替换为 `sync.py`
2. 实现 `lake_adft sync status` — 对比 parquet 与 postgres 的行数/时间戳
3. 实现 `lake_adft sync push` — parquet 全量覆盖到 postgres（TRUNCATE + COPY）
4. 实现 `lake_adft sync pull` — postgres 全量导出到 parquet
5. 安全检查：防止误用小数据集覆盖大数据集

---

## 1. 表范围

### Push & Pull 共享表（算法结果数据）

| 逻辑表名 | Parquet 路径 | 格式 |
|----------|-------------|------|
| eot_results | `eot_results.parquet` | 单文件 |
| eot_extremes | `eot_extremes.parquet` | 单文件 |
| quantile_labels | `quantile_labels.parquet` | 单文件 |
| quantile_extremes | `quantile_extremes.parquet` | 单文件 |
| quantile_abrupt_transitions | `quantile_abrupt_transitions.parquet` | 单文件 |
| pwm_extreme_thresholds | `pwm_extreme_thresholds.parquet` | 单文件 |
| hawkes_results | `hawkes_results.parquet` | 单文件 |
| hawkes_lrt | `hawkes_lrt.parquet` | 单文件 |
| hawkes_transition_monthly | `hawkes_transition_monthly.parquet` | 单文件 |
| area_shift_labels | `area_shift_labels.parquet` | 单文件 |
| area_quality | `area_quality/` | 目录（chunked） |
| area_anomalies | `area_anomalies/` | 目录（chunked） |

### 排除

| 排除项 | 原因 |
|--------|------|
| `*_run_status` | 状态数据，非结果数据 |
| `comparison_*` | 视为 view，仅 parquet 存在 |
| `lake_area` / `lake_info` / `anomaly` | 基础数据，体量大且不需要 sync |

---

## 2. 命令设计

### 2.1 `lake_adft sync status`

```
lake_adft sync status [--table TABLE]
```

- 无参数时遍历所有 12 个表
- `--table` 指定单表
- 输出：表名、parquet 行数、postgres 行数（估算）、postgres MAX(computed_at)、同步状态标记

**实现要点：**
- Parquet 行数：`pyarrow.parquet.read_metadata(path).num_rows`（目录格式需累加）
- Postgres 行数：`pg_class.reltuples` 估算（避免全表 COUNT 超时）
- Postgres 时间戳：`MAX(computed_at)`（仅对小表执行，大表跳过或用 run_status 代替）

### 2.2 `lake_adft sync push`

```
lake_adft sync push --table TABLE [--chunk-size 50000] [--force] [--dry-run]
lake_adft sync push --all [--chunk-size 50000] [--force] [--dry-run]
```

**流程：**

```
1. 解析 parquet 路径（单文件或目录）
2. 读取 parquet 行数
3. 查询 postgres 当前行数（pg_class.reltuples）
4. 安全检查：
   - 如果 parquet_rows < postgres_rows * 0.1 且 postgres_rows > 100
     → 跳过 + 警告（除非 --force）
5. ensure_table(table)  # DDL 确保表存在
6. BEGIN TRANSACTION
7. TRUNCATE table
8. 读 parquet → 分 chunk → COPY FROM STDIN (CSV format)
   - 列名从 parquet schema 获取
   - COPY table(col1, col2, ...) FROM STDIN WITH (FORMAT csv)
   - computed_at 列不包含在 COPY 中（由 DEFAULT now() 填充）
9. COMMIT
10. 输出 "table: N rows synced in X.Xs"
```

**COPY 实现（psycopg3）：**

```python
import io
import csv

with conn.cursor() as cur:
    cur.execute(f"TRUNCATE {table_name}")
    
    columns = [c for c in parquet_columns if c != "computed_at"]
    copy_sql = f"COPY {table_name}({', '.join(columns)}) FROM STDIN WITH (FORMAT csv, NULL '')"
    
    with cur.copy(copy_sql) as copy:
        for chunk_df in read_parquet_chunks(path, chunk_size):
            buf = io.StringIO()
            chunk_df[columns].to_csv(buf, index=False, header=False)
            copy.write(buf.getvalue())
```

**分 chunk 读取 parquet：**

```python
def read_parquet_chunks(path: Path, chunk_size: int):
    """Yield DataFrame chunks from a parquet file or directory."""
    if path.is_dir():
        for f in sorted(path.glob("*.parquet")):
            df = pd.read_parquet(f)
            yield from _split_df(df, chunk_size)
    else:
        # 使用 pyarrow 的 batch reader 避免一次性加载
        pf = pq.ParquetFile(path)
        for batch in pf.iter_batches(batch_size=chunk_size):
            yield batch.to_pandas()
```

### 2.3 `lake_adft sync pull`

```
lake_adft sync pull --table TABLE [--chunk-size 200000]
lake_adft sync pull --all [--chunk-size 200000]
```

**流程：**

```
1. 查询 MAX(hylak_id) 确定范围
2. 确定输出路径：
   - 目录格式（area_quality, area_anomalies）→ 清空目录后按 chunk 写入
   - 单文件格式 → 直接覆盖
3. 按 chunk_size 分段 SELECT * FROM table WHERE hylak_id >= ? AND hylak_id < ?
4. 写入 parquet
5. 输出 "table: N rows exported in X.Xs"
```

**目录格式 vs 单文件判断：**

```python
CHUNKED_TABLES = {"area_quality", "area_anomalies"}
```

---

## 3. 文件变更

| 操作 | 文件 | 说明 |
|------|------|------|
| 删除 | `cli/export.py` | 被 sync 完全替代 |
| 新建 | `cli/sync.py` | sync 子命令组 |
| 修改 | `cli/__init__.py` | 去掉 export 注册，添加 sync 注册 |

### 3.1 `cli/sync.py` 结构

```python
"""CLI commands for data synchronisation between parquet and PostgreSQL."""

from __future__ import annotations

import typer

from ._common import setup_logging

app = typer.Typer(help="Parquet ↔ PostgreSQL data sync", no_args_is_help=True)

# ── 表注册 ──────────────────────────────────────────────────────────────────

SYNC_TABLES: list[str] = [
    "eot_results",
    "eot_extremes",
    "quantile_labels",
    "quantile_extremes",
    "quantile_abrupt_transitions",
    "pwm_extreme_thresholds",
    "hawkes_results",
    "hawkes_lrt",
    "hawkes_transition_monthly",
    "area_shift_labels",
    "area_quality",
    "area_anomalies",
]

CHUNKED_TABLES: set[str] = {"area_quality", "area_anomalies"}

# ── ensure_table 映射 ───────────────────────────────────────────────────────
# 复用 postgres_provider 的 _ENSURE_DISPATCH key
# 部分表共享同一个 ensure（如 quantile_labels → "quantile"）

TABLE_TO_ENSURE_KEY: dict[str, str] = {
    "eot_results": "eot",
    "eot_extremes": "eot",
    "quantile_labels": "quantile",
    "quantile_extremes": "quantile",
    "quantile_abrupt_transitions": "quantile",
    "pwm_extreme_thresholds": "pwm_extreme",
    "hawkes_results": "hawkes",
    "hawkes_lrt": "hawkes",
    "hawkes_transition_monthly": "hawkes",
    "area_shift_labels": "area_shift_labels",
    "area_quality": "area_quality",
    "area_anomalies": "area_anomalies",
}


@app.command()
def status(...): ...

@app.command()
def push(...): ...

@app.command()
def pull(...): ...
```

### 3.2 `cli/__init__.py` 变更

```diff
- from . import export as _export
+ from . import sync as _sync

- app.add_typer(_export.app, name="export", help="Data export utilities")
+ app.add_typer(_sync.app, name="sync", help="Parquet ↔ PostgreSQL data sync")
```

---

## 4. 依赖

| 包 | 用途 | 是否已有 |
|----|------|----------|
| `psycopg` | COPY FROM STDIN | 已有 |
| `pyarrow` | parquet metadata / batch reader | 已有 |
| `pandas` | DataFrame 操作 | 已有 |
| `typer` | CLI 框架 | 已有 |

无需新增依赖。

---

## 5. 安全机制

| 场景 | 处理 |
|------|------|
| parquet 行数远小于 postgres（< 10%，且 postgres > 100 行） | 跳过 + 警告，需 `--force` 覆盖 |
| 表不存在 | `ensure_table()` 自动创建 DDL |
| COPY 中途失败 | 事务回滚，postgres 数据不变 |
| `--dry-run` | 只打印计划（表名、行数、操作），不执行任何写入 |
| `--all` 时某表失败 | 该表回滚，继续处理下一个表，最后汇总报告 |

---

## 6. 性能预估

| 表 | 行数 | COPY 预估时间 |
|----|------|--------------|
| eot_results | 1,079,742 | ~5-10s |
| eot_extremes | 958,707 | ~5-8s |
| quantile_labels | 1,079,649 | ~5-10s |
| quantile_extremes | 1,079,649 | ~5-10s |
| quantile_abrupt_transitions | 248,496 | ~2s |
| pwm_extreme_thresholds | 679,084 | ~5s |
| hawkes_results | 187 (smoke) | <1s |
| hawkes_lrt | 51 (smoke) | <1s |
| hawkes_transition_monthly | 51 (smoke) | <1s |
| area_shift_labels | 1,401,802 | ~8-15s |
| area_quality | ~1,049,800 | ~5-10s |
| area_anomalies | ~318,143 | ~2-3s |

全量 push（`--all`）预计 1-2 分钟完成。

---

## 7. 验收标准

- [ ] `lake_adft sync --help` 显示 status/push/pull 三个子命令
- [ ] `lake_adft sync status` 输出所有 12 个表的对比信息
- [ ] `lake_adft sync push --table eot_results` 成功将 parquet 数据写入 postgres
- [ ] `lake_adft sync push --all --dry-run` 打印计划不执行
- [ ] 安全检查：hawkes smoke 数据（187 行）不会覆盖 postgres 中 95 万行数据（除非 --force）
- [ ] `lake_adft sync pull --table eot_results` 成功从 postgres 导出到 parquet
- [ ] 旧 `lake_adft export` 命令不再存在
- [ ] 全量测试套件 pass

---

## 8. 不在本计划范围内

- Comparison 系列的 postgres DDL/upsert 实现（视为 view，仅 parquet）
- `*_run_status` 表的同步（状态数据由 batch engine 管理）
- 基础数据表（lake_area/lake_info/anomaly）的 pull（体量大、耗时长、不需要）
- 增量同步 / CDC 机制（当前全量覆盖即可满足需求）
- `computed_at` 索引优化（可后续按需添加）
