# PWM-Hawkes 指数衰减急转识别算法

## 1. 背景

当前 PWM-Hawkes 管线做 runs declustering（`run_length=1`）会损失两个关键信息：

1. **事件强度不累积**：只取 cluster 内 severity 最大月作为代表，丢弃了同类型连续极端月的累积效应
2. **gap 不可跨**：两个同类型极端月中间只要有一个 normal 月就断开，不区分"一个弱事件被一个 normal 月隔开"和"一个强事件虽然隔了几个月但衰减后仍有残余激发"

本算法用指数衰减累积指数 `C_k` 替代硬阈值 runs，在整个月时间序列上建模，同时保留 Hawkes 互激判定。

---

## 2. 输入

### 2.1 index_value — 共享基础层

`index_value` 是 quantile 和 PWM 共享的标准输入，由 decomposition 层产出：

```
DecompositionResult (STL / monthly_climatology)
       │
       ├── quantile/compute.py   → index_value 用于分位数判定
       └── pwm_extreme/compute.py → index_value 用于 PWM 交叉熵阈值
```

`index_value` 的定义因 decomposition 方法而异，当前 STL 方案将其定为
月度百分位排名（∈ [0, 100]）。

### 2.2 PWM 极端事件

对每个湖泊，通过 **index_value 路径** 得到极端事件：

- 阈值拟合：`compute_pooled_pwm_thresholds(result, ...)`
  （`packages/lakeanalysis/src/lakeanalysis/pwm_extreme/compute.py:292`）
- 标签分配：`assign_pwm_extreme_labels(index_df, thresholds)`
  （`:251`，要求 `index_value` 列）
- 事件提取：`_extract_events_index(labeled_df)`
  （`:554`，输出含 `index_value`、`threshold`、`severity`）

```text
E = {(t_1, m_1, s_1), (t_2, m_2, s_2), ..., (t_n, m_n, s_n)}
```

- `t_i = year + (month - 1) / 12`：月份对应的连续时间
- `m_i ∈ {high, low}`：极端类型
- `s_i = |index_value - threshold|`：事件 severity（基于 index_value，百分位单位）

> **注意**：不应使用 deprecated 的 raw `water_area` 路径
> （`compute_monthly_thresholds` / `_extract_events_raw`）。
> decay 算法建立在 index_value 上，与 quantile/PWM 的标准化输入一致。

### 2.3 完整时间轴

对每个湖，有一条完整的月时间轴 `T = {t_1, t_2, ..., t_N}`，覆盖所有年月（包括 normal 月）。

---

## 3. 算法

### 3.1 z-score 标准化

对每个湖、每种极端类型（high / low）分别标准化。

输入 `s_i` 已经基于 `index_value`（见 §2.2），是百分位单位的 severity：

```text
z_i = (s_i - μ_type) / σ_type
```

- `μ_type`：该类型在该湖所有极端事件 severity 的均值
- `σ_type`：该类型在该湖所有极端事件 severity 的标准差（若 `σ_type = 0`，取 `z_i = 0`）

### 3.2 极端指数变换

```text
f_i = ln(|z_i - 0.5| + 1)
```

性质：

| `z_i` | `|z_i - 0.5|` | `f_i` | 含义 |
|-------|--------------|-------|------|
| 0.5 | 0.0 | 0.0 | 极端程度接近类型中位数，对累积指数无贡献 |
| 3.0 | 2.5 | 1.25 | 3σ 强极端事件，贡献约 +1.25 |
| 0.0 | 0.5 | 0.41 | 极端程度为 0（不超过阈值），贡献约 +0.41 |
| -2.0 | 2.5 | 1.25 | 极端程度远低于平均值（阈值以上但严重度很低），贡献约 +1.25 |

**设计理由：**

`|z_i - 0.5|` 让 `f_i` 对"偏离中位数"对称响应。无论 severity 处于分布的上尾还是下尾，只要足够**极端**（远离中位数），就对 `C_k` 产生正贡献。"刚好接近中位数"的 mild 事件贡献趋近于 0。`+1` 确保对数参数严格为正。

### 3.3 指数衰减累积指数 C_k

对整个月时间轴（包括 normal 月），逐月计算：

```text
C_k = Σ_{i ≤ k} f_i × e^{-λ × (month_k - month_i)}
```

其中 `i` 遍历所有已发生的极端月（high 或 low），不分类型求和。
**λ 是可调衰减率**（decay rate），控制历史极端事件的记忆强度。

**λ 的物理含义：**

| λ | 半衰期 | 含义 |
|---|--------|------|
| 0.3 | ~2.3 个月 | 缓慢衰减，允许较长的记忆窗口 |
| 0.5 | ~1.4 个月 | 慢衰减 |
| 0.7 | ~1.0 个月 | 中等衰减 |
| 1.0 | ~0.7 个月 | 标准衰减（默认值） |
| 1.5 | ~0.5 个月 | 快速衰减 |
| 2.0 | ~0.35 个月 | 更快衰减，仅最近几星期的事件有实质贡献 |

**建议默认值**：λ = 1.0。实际最优值应通过调参确定（以急转段数量、QC 通过率、LRT 显著率为指标）。

**λ 的选择逻辑**：
- λ 越大 → 记忆越短 → 连续极端月越容易断开 → 急转段更短、更多
- λ 越小 → 记忆越长 → 远距离事件仍耦合在同一 C_k 段内 → 急转段更长、更少

**对 normal 月**：`f_i = 0`，但不重置衰减——旧事件的历史贡献仍通过 `e^{-λ × gap}` 传导到 `C_k`。

**关键性质：**

| 情景 | C_k 行为 |
|------|----------|
| 孤立极端月 | `C_k = f_1`（仅自己贡献） |
| 连续 3 个极端月，无间隔 | `C_3 = f_3 + f_2 × e^{-λ} + f_1 × e^{-2λ}` |
| 一个 4σ 极端 + 中间 3 个 normal 月 + 一个 3σ（λ=1） | 第一个的贡献从 `f_1` 衰减到 `f_1 × e^{-3} ≈ 0.05 × f_1` |
| 全部 normal 月（无极端） | `C_k = 0` |

**全序列计算方式**（非仅极端月）：

1. 扫描整个时间轴（所有月）
2. 遇到 high 或 low 月时度量 `f_i`，带入衰减求和
3. 每月过后衰减历史值 `× e^{-λ}`
4. C_k = 衰减后的残余 + 当月贡献

### 3.4 急转段提取

在 `C_k` 序列上：
1. 找 `C_k > 0` 的连续月段（run-length encoding）
2. 对每段判断其中是否同时存在 high 和 low 极端事件

分段结果：

```text
Segment = {
  start_month, end_month,          // 该段的起止月
  duration_months,                 // 持续月数
  has_high, has_low,              // 是否包含 high / low 极端事件
  type: "transition" | "unilateral",  // 急转段 / 单边极端段
  max_C, mean_C, integral_C,      // 段内 C_k 的统计量
  n_extreme_events,               // 段内极端事件总月数
}
```

### 3.5 Hawkes 互激

**保持不变**。

所有原始极端月（不去从化）直接作为 Hawkes 输入：
- 事件时间 `t_i`
- 事件类型 `m_i → {D=0, W=1}`
- 事件强度标注（mark）`C_k`（在 `events_table.severity` 中记录）

互激判定方式不变：
- `fit_full_model()` → 4 条边全打开的 Hawkes MLE
- `fit_restricted_model(disabled_edges=[(WET, DRY)])` → 禁用 D→W
- `fit_restricted_model(disabled_edges=[(DRY, WET)])` → 禁用 W→D
- LRT 检验 `alpha_WD` 是否显著 → `lrt_p_D_to_W`

---

## 4. 输出

### 4.1 Hawkes 标准产出（不变）

| 表 | 说明 |
|----|------|
| `hawkes_results` | fit 参数 + QC + LRT p 值 |
| `hawkes_lrt` | 每条边的 LRT 统计量 |
| `hawkes_transition_monthly` | 月度交叉激发显著月 |

### 4.2 新增产出

| 输出 | 说明 |
|------|------|
| `pwm_hawkes_segments` | 急转段 / 单边极端段定义 |
| 每个 event 的 `severity` 字段 | 替换为 `C_k`（指数衰减累积强度） |

### 4.3 segments 表字段

```
hylak_id         INTEGER
segment_id       INTEGER       -- 湖内 segment 序号
start_year       INTEGER
start_month      INTEGER
end_year         INTEGER
end_month        INTEGER
duration_months  INTEGER       -- 持续月数
segment_type     TEXT          -- "transition" | "unilateral"
has_high         BOOLEAN       -- 包含 high 极端事件
has_low          BOOLEAN       -- 包含 low 极端事件
max_C            FLOAT         -- 段内 C_k 最大值
mean_C           FLOAT         -- 段内 C_k 平均值
integral_C       FLOAT         -- 段内 C_k 积分
n_extreme_events INTEGER       -- 段内极端事件月数
first_extreme_type TEXT        -- 段内第一个极端事件的类型（high/low）
last_extreme_type  TEXT        -- 段内最后一个极端事件的类型
workflow_version TEXT
computed_at      TIMESTAMPTZ
```

---

## 5. 与旧版 runs declustering 对比

| 维度 | 旧版（runs declustering） | 新版（指数衰减） |
|------|------------------------|----------------|
| 事件聚合 | 同类型连续极端 → 1 个代表点 | 不聚合，每个极端月独立 |
| 强度 | 单月 severity | 指数衰减累积 C_k |
| gap 处理 | run_length=1 硬断开 | 衰减递减，强度够则可跨 |
| 急转段 | 不识别 | C_k > 0 连续段 + 类型交集判定 |
| Hawkes 输入事件数 | 少（约 5-30） | 多（约 10-100） |
| 自激膨胀风险 | 低 | 可能有提升（需验证） |
| 互激判定 | 不变 | 不变 |

---

## 6. 实现文件规划

| 文件 | 动作 | 新增/修改 |
|------|------|----------|
| `pwm_extreme/events.py` | 新增 `compute_decay_index(decay_rate=...)` | ~80 行 |
| `pwm_extreme/events.py` | 新增 `extract_segments()` | ~60 行 |
| `batch/calculator/pwm_hawkes.py` | 切换到 `compute_pooled_pwm_thresholds` + `_extract_events_index`（index_value 路径），替换 decluster 为 C_k 计算 | ~30 行改 |
| `postgres/lake_pwm.py` | 新增 `pwm_hawkes_segments` 表 DDL + upsert | ~60 行 |
| `pwm_extreme/store.py` | 新增 segments 行 shaper | ~30 行 |
| `batch/calculator/pwm_hawkes.py` | 新增 segments 输出到 `result_to_rows` | ~10 行改 |
| `scripts/run_pwm_hawkes.py` | 新增 `--decay-rate` CLI 参数 | ~5 行改 |
| `scripts/plot_pwm_hawkes_lake.py` | 覆盖 C_k 曲线 + seg 标注 | ~40 行改 |
| 测试 | `test_decay_index.py` | ~100 行 |

总计约 400 行。

---

## 7. C_k 计算伪代码

```
function compute_decay_index(labels_df, decay_rate=1.0):

  # labels_df must contain index_value column (from DecompositionResult)

  # Step 1: z-score (per-lake, per-type)
  for each event_type in {high, low}:
    type_events = labels_df[labels_df.extreme_label == type]
    severities = abs(type_events.index_value - type_events.threshold)
    μ = mean(severities)
    σ = std(severities)  or 1 if σ=0
    for i in type_events:
      z_i = (severities[i] - μ) / σ

  # Step 2: f_i = ln(|z_i - 0.5| + 1)

  # Step 3: rolling decay over full timeline
  C = 0.0
  result = []
  for each month in full_timeline:
    # decay existing accumulations by 1 month
    C = C * exp(-decay_rate)

    # event at this month?
    if month has extreme event (high or low):
      C = C + f_i    # undecayed contribution of current month

    result.append(C)

  return DataFrame(year, month, C_k, has_high, has_low)
```

**注意点：**

- 采用"先衰减再累加"：`C = C * e^{-λ}; C = C + f_i`。
  物理含义：**"从当前月回头看，所有已知事件在当前的衰减总和"**。
- `decay_rate` 即 λ，默认 1.0，通过 CLI 参数 `--decay-rate` 调参。
- 若月没有任何极端事件，步骤仅为 `C = C * e^{-λ}`（纯衰减）。
- `index_value` 来自 `DecompositionResult`（STL 百分位排名 ∈ [0, 100]），
  不应使用 raw `water_area`。

---

## 8. 风险与注意事项

1. **z-score 计算需要至少 2 个事件**：若某类型只有 0 或 1 个事件，σ = 0 时取 z_i = 0
2. **C_k 不分类型求和**：一个极涝事件和一个极旱事件都会增加 C_k，不会对消
3. **Hawkes 事件数增加可能导致自激核拟合膨胀**：需通过 spectral_radius 和 LRT 监控
4. **`f_i` 对 high 和 low 事件一视同仁**：两者的 severity 量级差异完全由 z-score 标准化解决
5. **λ 需要调参**：不同 λ 值会显著改变急转段的数量和长度。建议以急转段数量、QC 通过率、LRT 显著率为指标做网格搜索
6. **必须使用 index_value 路径**：不应使用 deprecated 的 `compute_monthly_thresholds`（raw water_area）。`index_value` 来自 `DecompositionResult`，是 quantile/PWM 共享的标准化输入
7. **long gap + fast decay → C_k 快速归零**：`e^{-λ × gap}` 在 λ=2, gap=5 时衰减至 ~4.5×10^{-5}，浮点下近似 0

---

## 9. 待验证假设

1. `f_i = ln(|z_i - 0.5| + 1)` 在真实数据上的数值范围
2. 急转段数量是否合理（是否过多或过少）
3. C_k > 0 这个阈值是否需要调（θ 参数化）
4. 不做去从化后 Hawkes 收敛率、spectral_radius 稳定性是否改变
