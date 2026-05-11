# CLI 重构与 Fat Script 迁移实施计划

> **Status: DONE** (2026-05-11)

> 日期: 2026-05-11
> 背景: 当前 `scripts/` 目录包含 51 个独立脚本，存在 ~1000 行重复样板代码、7 个 fat script（>200 行含业务逻辑）、以及 7 个可归档的一次性脚本。需要统一 CLI 入口、提取可复用逻辑到 library、归档废弃脚本。

---

## 0. 目标

1. 统一 CLI 入口：`lake <group> <command> [options]`（typer）
2. 消除 ~1000 行重复样板代码
3. 将 fat script 中的业务逻辑提取到 library
4. 新建 `EOTHawkesCalculator` 接入 batch engine
5. 归档 7 个废弃脚本

---

## 1. typer CLI 结构设计

### 1.1 目录结构

```
src/lakeanalysis/cli/
├── __init__.py          # app = typer.Typer(); 注册所有子组
├── _common.py           # 共享参数回调、Logger 初始化、DATA_DIR
├── quality.py           # lake quality ...
├── hawkes.py            # lake hawkes ...
├── pwm.py              # lake pwm ...
├── eot.py              # lake eot ...
├── comparison.py        # lake comparison ...
├── spatial.py           # lake spatial ...
├── shift.py            # lake shift ...
├── entropy.py          # lake entropy ...
├── plot.py             # lake plot ...
└── export.py           # lake export ...
```

### 1.2 pyproject.toml 入口

```toml
[project.scripts]
lake = "lakeanalysis.cli:app"
```

### 1.3 共享基础设施 (`_common.py`)

```python
from pathlib import Path
import typer
from lakeanalysis.logger import Logger

DATA_DIR = Path(__file__).resolve().parents[3] / "data"

def setup_logging(name: str) -> None:
    """统一 Logger 初始化，替代每个脚本的 Logger("xxx") 调用。"""
    Logger(name)

# 共享参数类型
LimitId = typer.Option(None, "--limit-id", "-l", help="只处理前 N 个湖")
ChunkSize = typer.Option(10_000, "--chunk-size", "-c", help="每批处理湖数")
DryRun = typer.Option(False, "--dry-run", help="只打印计划，不执行")
```

### 1.4 子命令分组（10 组）

| 组 | 命令示例 | 对应脚本 |
|----|---------|----------|
| `quality` | `lake quality run`, `lake quality interpolation`, `lake quality recheck` | run_quality, run_interpolation_detect, recheck_zero_quantile, recompute_pv |
| `hawkes` | `lake hawkes run`, `lake hawkes eot-batch`, `lake hawkes mining`, `lake hawkes qc` | run_hawkes, run_pwm_hawkes, run_hawkes_mining, run_hawkes_qc |
| `pwm` | `lake pwm run`, `lake pwm diag` | run_pwm_extreme, run_pwm_hawkes_diag |
| `eot` | `lake eot run`, `lake eot basemodel`, `lake eot quantile` | run_eot, run_basemodel, run_quantile |
| `entropy` | `lake entropy run` | run_entropy |
| `comparison` | `lake comparison run`, `lake comparison area`, `lake comparison grid` | run_algorithm_comparison, run_area_comparison, comparison_grid_agg |
| `spatial` | `lake spatial pfaf`, `lake spatial nearest`, `lake spatial similarity`, `lake spatial impact` | run_pfaf, run_nearest, run_similarity, run_impact |
| `shift` | `lake shift compute`, `lake shift sample`, `lake shift inspect` | compute_shift_labels, sample_shift_degraded_candidates, inspect_shift_lakes |
| `plot` | `lake plot upset`, `lake plot global-eot`, `lake plot extremes`, ... | 17 个 plot 脚本 |
| `export` | `lake export tables` | export_area_tables |

---

## 2. 分阶段实施

### Phase 0: 归档废弃脚本（0.5h）

**操作**：移动到 `scripts/_archived/`

| 脚本 | 行数 | 归档原因 |
|------|------|----------|
| `run_hawkes_batch.py` | 1090 | 将被 EOTHawkesCalculator 取代 |
| `explore_penalized_volatility.py` | 348 | 已被 H×CV 取代 |
| `explore_entropy_cv.py` | 315 | 探索完毕，逻辑已在 library |
| `migrate_outside_range_fix.py` | 220 | 一次性迁移已执行 |
| `migrate_flat_quality_to_anomalies.py` | 246 | 一次性迁移已执行 |
| `migrate_area_ratio_to_anomalies.py` | 280 | 一次性迁移已执行 |
| `test_db.py` | 36 | 非真正测试 |

**释放**: 2,535 行

---

### Phase 1: 新建 `EOTHawkesCalculator`（3-4h）

**目标**：将 `run_hawkes_batch.py` 的 EOT-based Hawkes 路径正式接入 batch engine。

**新建文件**：`src/lakeanalysis/batch/calculator/eot_hawkes.py`

**实现路径**：
```python
class EOTHawkesCalculator(Calculator):
    def run(self, task: LakeTask) -> dict:
        # 1. task.df → MonthlyTimeSeries
        series = MonthlyTimeSeries.from_frame(task.df)
        # 2. build EOT events (已有: hawkes/bridge.py)
        events = build_events_from_eot(task.df, threshold_quantile=0.90,
                                        frozen_year_months=task.frozen_ym)
        # 3. fit Hawkes (已有: hawkes/fit.py)
        fit_result = fit_hawkes(events)
        # 4. QC metrics (已有: hawkes/pipeline.py)
        qc = compute_qc_metrics(fit_result, events)
        # 5. build result rows (已有: hawkes/pipeline.py)
        return build_hawkes_result_row(task.hylak_id, fit_result, qc)

    def result_to_rows(self, result: dict) -> list[dict]:
        return [result]  # 或按 pipeline.py 的 row shaper

    def error_to_rows(self, error: Exception, task: LakeTask) -> list[dict]:
        return [make_error_row(task.hylak_id, error)]
```

**依赖**（全部已存在）：
- `hawkes/bridge.py` → `build_events_from_eot`
- `hawkes/fit.py` → `fit_hawkes`
- `hawkes/pipeline.py` → `compute_qc_metrics`, `build_hawkes_result_row`

**注册到 factory**：
```python
# batch/calculator/factory.py
_REGISTRY["eot_hawkes"] = "lakeanalysis.batch.calculator.eot_hawkes:EOTHawkesCalculator"
```

**验证**：用 `SingleProcessRunner` 跑 5 个湖确认输出格式正确。

---

### Phase 2: 提取 `hawkes/mining.py`（2h）

**新建文件**：`src/lakeanalysis/hawkes/mining.py`

**提取函数**：

```python
# hawkes/mining.py

def load_hawkes_summary(path: Path) -> pd.DataFrame:
    """加载并验证 Hawkes batch summary CSV。"""

def select_transition_lakes(
    summary: pd.DataFrame,
    *,
    p_threshold: float = 0.05,
    alpha_min: float = 0.01,
    min_events: int = 5,
    quarter_window: float = 0.25,
    mass_threshold: float = 0.5,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """筛选短记忆转换湖 (D→W, W→D, union)。"""

def build_overall_stats(summary: pd.DataFrame) -> dict:
    """计算拟合质量聚合统计。"""

def load_events_from_case(case_dir: Path) -> pd.DataFrame:
    """加载单个 case 的 events.csv 并转换为 EOT 格式。"""
```

**脚本瘦身**：`run_hawkes_mining.py` 从 449 行 → ~150 行（仅保留 CLI 解析 + 绘图 + 编排）。

---

### Phase 3: 提取 `entropy/service.py`（1.5h）

**目标**：拆分 `entropy/runner.py`（219 行）的职责混杂问题。

**新建文件**：`src/lakeanalysis/entropy/service.py`

```python
# entropy/service.py

def run_entropy_pipeline(
    provider: LakeProvider,
    config: EntropyRunConfig,
) -> pd.DataFrame:
    """编排 AE 计算管道：fetch → compute_annual_ae → compute_trend → 返回结果 df。"""
```

**`entropy/runner.py` 瘦身为**：
```python
def main():
    args = parse_args()
    config = EntropyRunConfig(...)
    provider = create_provider(...)
    result = run_entropy_pipeline(provider, config)
    # IO: write parquet, generate plots
```

---

### Phase 4: typer CLI 框架搭建（2h）

**操作**：
1. `uv add typer[all]`
2. 新建 `src/lakeanalysis/cli/` 目录结构
3. 实现 `_common.py` 共享基础设施
4. 实现 `__init__.py` 注册所有子组
5. 在 `pyproject.toml` 添加 `[project.scripts]`
6. 验证 `lake --help` 输出

---

### Phase 5: 迁移 thin wrapper 脚本（4-5h）

**按组迁移，每组一个 commit**：

| 组 | 脚本数 | 预估时间 |
|----|--------|----------|
| spatial | 4 | 30min |
| entropy | 1 | 10min |
| quality | 4 | 40min |
| eot | 4 | 40min |
| pwm | 3 | 30min |
| comparison | 4 | 40min |
| shift | 3 | 30min |
| hawkes | 4 | 40min |
| export | 1 | 10min |
| plot | 17 | 1.5h |

**迁移模板**（以 `run_entropy.py` 为例）：

Before (102 行):
```python
import argparse
from lakeanalysis.logger import Logger
...
def parse_args(): ...
def main():
    args = parse_args()
    Logger("entropy")
    ...
```

After (~20 行):
```python
# cli/entropy.py
import typer
from ._common import setup_logging, LimitId, ChunkSize

app = typer.Typer()

@app.command()
def run(limit_id: int = LimitId, chunk_size: int = ChunkSize):
    """Run apportionment entropy pipeline."""
    setup_logging("entropy")
    from lakeanalysis.entropy.service import run_entropy_pipeline
    ...
```

**旧脚本处理**：迁移完成后移入 `scripts/_legacy/`，保留 6 个月后删除。

---

### Phase 6: 提取 `run_area_comparison.py` 管道（2h，可选）

**新建**：`src/lakeanalysis/quality/comparison_pipeline.py`

提取多步分析管道（~300 行）为可复用函数，脚本瘦身为 thin CLI。

---

## 3. 时间线与优先级

| Phase | 内容 | 工时 | 优先级 |
|-------|------|------|--------|
| 0 | 归档废弃脚本 | 0.5h | P0 |
| 1 | EOTHawkesCalculator | 3-4h | P0 |
| 2 | hawkes/mining.py 提取 | 2h | P1 |
| 3 | entropy/service.py 拆分 | 1.5h | P1 |
| 4 | typer CLI 框架搭建 | 2h | P1 |
| 5 | thin wrapper 迁移 | 4-5h | P2 |
| 6 | area_comparison 提取 | 2h | P3 |
| **总计** | | **15-17h** | |

---

## 4. 验收标准

- [ ] `lake --help` 显示 10 个子命令组
- [ ] `lake hawkes eot-batch --help` 可用
- [ ] `EOTHawkesCalculator` 通过 `SingleProcessRunner` 跑 5 湖无报错
- [ ] `hawkes/mining.py` 有对应单元测试
- [ ] `entropy/service.py` 有对应单元测试
- [ ] 全量测试套件 pass（564+ tests）
- [ ] 旧脚本归档到 `scripts/_archived/` 和 `scripts/_legacy/`
- [ ] 无 `import argparse` 出现在 `src/lakeanalysis/cli/` 中

---

## 5. 风险与缓解

| 风险 | 缓解 |
|------|------|
| typer 引入新依赖 | typer 仅依赖 click，已广泛使用，无安全风险 |
| 用户习惯 `python scripts/X.py` | Phase 5 保留旧脚本 6 个月，添加 deprecation warning |
| EOTHawkesCalculator 输出格式与旧脚本不一致 | 复用 `hawkes/pipeline.py` 的 row shaper，保证格式兼容 |
| plot 组 17 个脚本迁移量大 | plot 组可最后迁移，或保留为独立脚本（优先级最低） |

---

## 6. 不在本计划范围内

- Service/orchestration 层的集成测试（当前 batch 框架级测试已足够）
- `lakesource` 包的重构
- MPI 相关的改动（Engine 已支持，Calculator 只需实现 `run`）
- Shell 脚本（`.sh`）的迁移
