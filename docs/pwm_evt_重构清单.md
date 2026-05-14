# PWM-EVT-Hawkes 算法重构清单

**依据：`docs/algorithm.md` v2.0  |  状态：部分完成（2026-05-14）**

## 当前进度

已完成：
- Phase 1：`compute_decay_index()` 已改为 `phi` 驱动的指数记忆累积，旧 z-score 递推已移除
- Phase 2：Route A (`evt_index.py`) 与 Route B (`evt_amplitude.py`) 均已实现
- Phase 2：`phi.py` 已实现，支持 `identity / log1p / normalize`
- Phase 3：`pwm_evt_compare.py` 已实现，可输出 Route A/B 对比 CSV
- Phase 4：`pwm_hawkes.py`、`run_pwm_hawkes.py` 已接入 `evt_route / phi_method`
- Phase 5：`pwm_extreme_return_levels` 表、row shaper、provider dispatch、sync 支持已打通

未完成：
- Phase 3：单湖实验脚本 `pwm_evt_single_lake.py`
- Phase 5：return level 诊断图与可视化
- Phase 6：legacy 路径清理与归档

---

## Phase 1：S_k 计算修正（events.py）

### 1.1 重写 `compute_decay_index()`

**文件**：`packages/lakeanalysis/src/lakeanalysis/pwm/events.py`

当前错误公式：
```text
f_i = ln(|z_i - 0.5| + 1)
S_k = S_{k-1} * e^{-λ} + phi_i      // 线性递推式
```

目标公式（见 `algorithm.md §5.2`）：
```text
S_k = Σ_{i≤k, extreme} phi_i · exp(-a · (month_k - month_i))
```

改动：
- 移除 `f_i` 和 `z_i` 的当前计算
- 改用线性 `S_k` 递推；normal 月不新增 `phi_i`，仅衰减既有记忆
- `phi_i` 暂用 `exceedance_i`（`|index_value - threshold|`）作为 fallback，后续 Phase 2 切换到 EVT strength
- 保留 high/low mask 用于 `has_high`/`has_low` 列

### 1.2 复查 `extract_segments()` 阈值

当前用 `S_k > 1` 作为单个 bridge normal 月的活跃基线。

- 在小样本湖泊上观察 active segment 数量/长度分布
- 如果过长或过多，可引入 `θ` 参数化为 `S_k > θ`
- 记录对比：`θ = 1` vs `θ = 0.5` vs `θ = 2.0`

### 1.3 `std = 0` 处理

当前代码已有 `sigma < 1e-15` 跳过逻辑（`events.py:159`）。
保留该逻辑，且在 `phi_i` 映射时对不可分事件统一赋固定基准值（如 `phi=1.0`）。

---

## Phase 2：EVT 强度估计（新模块）

### 2.1 Route A：`index_value` 空间 EVT

**文件**：`packages/lakeanalysis/src/lakeanalysis/pwm/evt_index.py`

```
输入：labeled_df (含 index_value, threshold_low/high, extreme_label)
输出：events_df (含 event_strength, return_levels)

流程：
1. 取 high extreme 的 exceedance = index_value - threshold_high
2. 取 low extreme 的 exceedance = threshold_low - index_value
3. 分别在 high/low exceedance 上：
   a. fit GPD (scipy.stats.genpareto.fit)
   b. 记录 params (shape ξ, scale σ)
   c. 计算 return level: z_T = u + σ/ξ * ((T*n_u/n)^ξ - 1)
   d. 输出 event_strength = return level 或 exceedance quantile
4. 输出 return_levels 表行
```

优先方案：先用最简单的 GPD fit，不做 NHPP，降低 Phase 2 复杂度。

### 2.2 Route B：连续幅值空间 EVT

**文件**：`packages/lakeanalysis/src/lakeanalysis/pwm/evt_amplitude.py`

```
输入：series_df (含 water_area), labels_df (PWM 判定), stl_result
输出：同 Route A 结构

流程：
1. 用 PWM labels 定位 extreme months
2. 在 STL residual R_t 或 log_area 上定义 exceedance
3. 可选：重构幅值空间的 threshold（从 index_value threshold 映射回来）
4. fit GPD / NHPP on amplitude exceedances
5. 输出 event_strength, return_levels
```

### 2.3 共享 `phi` 映射

**文件**：`packages/lakeanalysis/src/lakeanalysis/pwm/phi.py`

```python
def map_strength_to_phi(strength: np.ndarray, method: str = "identity") -> np.ndarray:
    """
    method:
      - "identity"   : phi = strength
      - "log1p"      : phi = ln(1 + strength)
      - "normalize"  : phi = strength / reference
    """
```

---

## Phase 3：实验脚本（A/B 对比）

### 3.1 单湖实验脚本

**新文件**：`packages/lakeanalysis/scripts/experiment/pwm_evt_single_lake.py`

```
功能：对单个 hylak_id 跑完整 PWM-EVT 流程
参数：--hylak-id, --csv, --route A|B, --decay-rate
输出：单个 CSV + 诊断图（S_k timeline, exceedance hist, return level plot）
```

### 3.2 小批量对比脚本

**新文件**：`packages/lakeanalysis/scripts/experiment/pwm_evt_compare.py`

```
功能：对一批湖（50-100 个）同时跑 Route A 和 B
输出：comparison.csv
  - hylak_id
  - n_extreme_high_A, n_extreme_high_B
  - mean_strength_high_A, mean_strength_high_B
  - n_segments_A, n_segments_B
  - n_transition_A, n_transition_B
  - mean_duration_A, mean_duration_B
  - hawkes_qc_pass_A, hawkes_qc_pass_B
  - hawkes_converge_A, hawkes_converge_B
  - lrt_p_D_to_W_A, lrt_p_W_to_D_A, ...
```

### 3.3 评估标准

| 指标 | 评估目标 |
|------|---------|
| extreme 数量 | 两路线是否一致 |
| strength 分布 | 是否合理（无零值过度、无极端离群） |
| segment 数量 | 是否过多/过少 |
| transition 比例 | 是否合理 |
| Hawkes QC pass rate | 稳定性和可解释性 |
| LRT p 值分布 | 是否显著 |
| 与 quantile baseline 的重叠度 | 一致性检查 |

---

## Phase 4：正式入口改造

### 4.1 `pwm_hawkes.py` 切换到 EVT path

**文件**：`packages/lakeanalysis/src/lakeanalysis/batch/calculator/pwm_hawkes.py`

改动：
```python
# Before (当前)
decay_df = compute_decay_index(pwm_result.labels_df, decay_rate=...)
segments_df = extract_segments(decay_df)
events_df = extract_hawkes_events_from_segments(...)

# After (目标)
evt_strength = compute_evt_strengths(pwm_result, route="A")     # Phase 2
phi_df = map_strength_to_phi(evt_strength, method="identity")   # Phase 2.3
decay_df = compute_decay_index(labels_df, phi_df, decay_rate=...)# Phase 1
segments_df = extract_segments(decay_df)
events_df = extract_hawkes_events_from_segments(...)
```

### 4.2 新增 CLI 参数

`run_pwm_hawkes.py` 新增：
```bash
--evt-route A|B             # EVT 路线选择
--phi-method identity|log1p|normalize
--decay-rate 1.0            # 已有
```

### 4.3 新 Calculator（可选）

如果 Phase 2-3 验证 Route A 和 B 差异很小，可直接修改 `PWMExtremeHawkesCalculator`。
如果差异大，可新增 `PWM_EVT_Hawkes_Calculator` 作为独立算法入口。

---

## Phase 5：Return Level 输出与可视化

### 5.1 存储表

**新表**：`pwm_extreme_return_levels`

| 列 | 类型 | 说明 |
|----|------|------|
| hylak_id | INT | 湖泊 ID |
| tail | TEXT | `high` / `low` |
| return_period | INT | 重现期（年） |
| return_level | FLOAT | 重现水平 |
| shape | FLOAT | GPD ξ |
| scale | FLOAT | GPD σ |
| threshold | FLOAT | 阈值 |
| n_exceedances | INT | 超阈值事件数 |

### 5.2 可视化

可选新增诊断图：
- Return level plot (log-return-period vs return-level)
- S_k timeline with segment overlay
- GPD QQ plot per lake per tail

---

## Phase 6：Legacy 路径清理

以下模块可在 EVT A/B 验证通过后标记 deprecated 或归档：

| 文件/模块 | 说明 |
|-----------|------|
| `pwm_extreme/events.py:run_runs_declustering()` | 旧版 hard-threshold declustering |
| `pwm_extreme/compute.py:compute_monthly_thresholds()` | deprecated raw water_area 路径 |
| `pwm_extreme/compute.py:_extract_events_raw()` | legacy compat |
| `pwm_extreme/compute.py:_detect_transitions_raw()` | legacy compat |
| `decomposition/monthly_climatology.py` | legacy 分解方法 |

---

## 实施建议

### 优先级

| Phase | 优先级 | 预估工作量 | 依赖 |
|-------|--------|-----------|------|
| **Phase 1** | P0（必须） | 1-2 小时 | 无 |
| **Phase 2** | P0（必须） | 3-5 小时 | 无 |
| **Phase 3** | P1（验证） | 2-3 小时 | Phase 1, 2 |
| **Phase 4** | P1（验证后） | 1-2 小时 | Phase 3 |
| **Phase 5** | P2（增强） | 2-3 小时 | Phase 2 |
| **Phase 6** | P3（收尾） | 1 小时 | Phase 4 |

### 风险点

1. GPD fit 在小样本下可能不收敛 → 需要 fallback 到原始 exceedance
2. Route B 的 threshold 映射可能引入误差 → 需要仔细选择映射方法
3. 若保留线性 S_k，需要关注极长序列上的累计尺度漂移

### 测试策略

| 文件 | 测试内容 |
|------|---------|
| `test_decay_index.py` | 修正后的 S_k 计算 |
| `test_evt_index.py` | Route A GPD fit + return level |
| `test_evt_amplitude.py` | Route B GPD fit + return level |
| `test_phi.py` | phi 映射函数 |
| `test_pwm_hawkes_evt.py` | 端到端 PWM-EVT-Hawkes |
