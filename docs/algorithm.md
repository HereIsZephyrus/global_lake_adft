# 全球湖泊旱涝急转事件识别与建模算法

**版本：2.0  |  最后修订：2026-05-14**

本文档是项目算法体系的唯一权威规范，整合并替代 `docs/research/` 及 `docs/memo*.md` 中分散的算法文档。

---

## 1. 分层架构

```
                    ┌─────────────────────────┐
                    │  Layer 3: Hawkes 互激    │
                    │  双变量点过程 + LRT      │
                    ├─────────────────────────┤
                    │  Layer 2: EVT 强度估计   │
                    │  PWM阈值 + exceedance    │
                    │  -> GPD/NHPP return level│
                    │  -> phi mapping -> C_k   │
                    ├─────────────────────────┤
                    │  Layer 1: Baseline       │
                    │  Quantile 经验分位数     │
                    │  (快速筛查 / 对照基线)   │
                    └─────────────────────────┘
                            ↑
                    STL 分解 + 质量筛选
                    (共享前置层)
```

- **Quantile**：baseline 方法，简单、直观、无分布假设，用于快速筛查和对照验证。
- **PWM + EOT/EVT**：主方法，PWM 在小样本下稳健估计阈值，EOT 在 exceedance 上做 EVT 建模，输出客观事件强度。
- **Hawkes**：双变量自激点过程，利用事件序列拟合互激强度，检验旱涝互激显著性。

---

## 2. 共享前置层：STL 分解与 `index_value`

### 2.1 输入

逐月湖泊面积序列 `water_area(y, m)`，覆盖 2001-2023（共 276 个月）。

### 2.2 对数变换

```text
log_area = ln(water_area)
```

对数变换将乘性方差结构转为加性，稳定方差量级。

### 2.3 STL 分解

```text
log_area = T_t + S_t + R_t
```

参数：`period=12, seasonal=11, trend>=13, robust=True`

- 趋势项 `T_t`：LOESS 局部加权回归，自适应跟踪多年扩张/萎缩
- 季节项 `S_t`：逐月周期性模式，允许幅度随年份缓慢漂移
- 残差项 `R_t`：去除趋势+季节后的纯信号

**与直接减同月均值的对比**：直接减同月历史均值 (`R_t = y_t - μ_m`) 隐含假设无趋势、季节幅度每年相同。STL 残差通过自适应趋势估计避免了"早年值偏低、晚年值偏高"的系统性伪异常。

### 2.4 `index_value`：同月经验百分位秩

对每个月 `k ∈ {1..12}`：

```text
p_t = count(R_{k,i} ≤ R_t) / (n_k + 1) × 100
```

- `index_value ∈ [0, 100]`，表示当前月残差在历史同月残差分布中的位置
- 不依赖分布假设，对厚尾和离群值鲁棒
- 跨月份可比：1 月 p=95 和 7 月 p=95 表示同等程度的"同月历史异常"

**备选方案**（z-score 标准化）：

```text
z_t = (R_t - R̄_k) / s_k
```

z-score 在理论上 ≈ N(0,1)，更符合正态直觉，但对厚尾敏感。本项目默认使用百分位秩，但两条路线可作为实验对照。

---

## 3. Baseline：Quantile 经验分位数法

### 3.1 核心流程

```text
1. 计算各月气候态：μ_m = mean(x_{y,m})
2. 计算月距平：a_{y,m} = x_{y,m} - μ_m
3. 取全湖距平序列的 Q(0.10) 和 Q(0.90) 作为低/高阈值
4. 标记：a_{y,m} ≤ q_low → extreme_low，a_{y,m} ≥ q_high → extreme_high
5. 检测急转：相邻月 low→high 或 high→low 且 month_ordinal 差值为 1
```

### 3.2 特点

| 维度 | 说明 |
|------|------|
| 理论基础 | 经验分位数，无分布假设 |
| 计算复杂度 | 极低 |
| 小样本稳定性 | 差（n<20 时分位数估计不稳定） |
| 外推能力 | 无（无法估计超越观测范围的分位数） |
| 时变适应性 | 无（固定阈值，不随季节/趋势变化） |

### 3.3 输出

- `quantile_labels`：逐月极端标签
- `quantile_extremes`：极端事件记录
- `quantile_abrupt_transitions`：急转事件记录

### 3.4 定位

**对照基线**。用于快速筛查和验证主方法的合理性，不承担极值强度估计和重现水平推断任务。

---

## 4. 主方法：PWM + EVT 统一极值框架

### 4.1 设计理念

POT (Peaks Over Threshold) 在小样本下阈值选择不稳定。PWM (Probability Weighted Moments) 通过矩估计提取分布尾部信息，在小样本下比直接的分位数估计更稳健。

**PWM 不是替代 EVT，而是替代"阈值选择"这一步**。确定阈值后，exceedance 仍然是标准 EVT 建模对象。

### 4.2 PWM 交叉熵阈值估计

#### 4.2.1 归一化

```text
z_i = index_value_i / mean(index_value)   # 归一化到均值=1
z_sorted = sort(z_i)
ε = z_sorted[0]
```

#### 4.2.2 PWM 矩（Type-2, unbiased）

```text
b_k = (1/n) · Σ_{i=1..n} [C(i-1, k) / C(n-1, k)] · z_{(i)}
```

其中 `k = 0..K`，`C(n,k)` 为组合数，`K` 为 PWM 阶数（默认 K=4）。

#### 4.2.3 移位指数先验

```text
y(u) = ε - (1-ε) · ln(1-u)
```

#### 4.2.4 交叉熵最优分位数函数

```text
x(u) = y(u) · exp(-Σ_{j=0..K} λ_j · u^j)
```

约束条件：`∫₀¹ u^k · x(u) du = b_k`，`k=0..K`

其中 `λ_j` 通过 L-BFGS-B 最小化约束残差平方和求得。

#### 4.2.5 阈值

```text
threshold_high = mean(index_value) · x(1 - p_high)
threshold_low  = mean(index_value) · x(p_low)
```

其中 `p_high ∈ (0, 1)` 为高尾概率（默认 ~0.05），`p_low` 为低尾概率。

**关键设计**：PWM 拟合是 **pooled** 的——将全湖所有月的 `index_value` 合并后做一次拟合，然后广播到 12 个月。这避免了逐月样本量不足导致的不稳定。

### 4.3 Exceedance 定义

```text
对 extreme_high：exceedance_i = index_value_i - threshold_high
对 extreme_low ：exceedance_i = threshold_low - index_value_i
```

**Event strength** 的基准定义：

```text
strength_i = exceedance_i
```

这是一个客观量——不是 relative rank，不是 z-score，而是"超过 extreme 门槛多少"。这与 EOT 的 exceedance `x_i - u(t_i)` 在理论上一致。

### 4.4 EVT 强度估计与 Return Level

在 PWM 确定的阈值之上，有两种 EVT 建模路线：

#### Route A：`index_value` 空间 EVT

在 `index_value` 空间的 exceedance 上拟合 GPD / NHPP：

```text
y_i = index_value_i - threshold_high   (high tail)
y_i = threshold_low - index_value_i     (low tail)
```

- EVT 建模对象：`y_i ∈ [0, 100 - threshold]`（有界）
- Return level 单位：`index_value` 百分位
- 优点：与当前 PWM 实现完全一致，改动最小
- 注意：`index_value` 有上界 100，尾部呈有限上界分布

#### Route B：连续幅值空间 EVT

将 PWM 判定结果映射回连续幅值空间后建模：

```text
y_i = R_t (STL residual) 高于某映射阈值
或 y_i = log_area 的原始尺度 excess
```

- EVT 建模对象：在原始幅值/残差空间的 exceedance
- Return level 单位：面积 km² 或 residual 单位
- 优点：更接近经典 EVT，强度解释更直观
- 代价：需定义 threshold 从 `index_value` 到连续空间的映射

**实验策略**：两条路线均实现为实验脚本，在同一批湖泊上对比输出，选定后进行正式入口改造。

### 4.5 Event Strength → `phi` 映射

EVT 给出的 `event_strength` 通过 `phi` 映射为 `C_k` 累积权重：

```text
phi_i = transform(strength_i)
```

候选映射（实验阶段三选一）：
- `phi(x) = x`（直接传递）
- `phi(x) = ln(1 + x)`（对数压缩）
- `phi(x) = x / s_ref`（参考尺度归一化）

`phi` 的角色是纯数值映射层，不背理论负担。`phi_i` 必须为非负值。

### 4.6 输出

| 表 | 说明 |
|----|------|
| `pwm_extreme_thresholds` | PWM 月度阈值 |
| `pwm_extreme_labels` | 逐月极端标签 |
| `pwm_extreme_extremes` | 极端事件（含 exceedance 和 EVT strength） |
| `pwm_extreme_return_levels` | EVT 重现水平（Route A 或 B） |
| `pwm_extreme_run_status` | 批处理运行状态 |

---

## 5. C_k 指数衰减累积指数

### 5.1 定位

替代旧版 hard-threshold runs declustering，在整个月时间序列上建模"极值事件链的记忆强度"。

### 5.2 数学定义（修正版）

```text
C_k = ln( Σ_{i ≤ k, i ∈ extreme} phi_i · exp(-a · gap(i, k)) )
```

其中：
- `phi_i`：第 i 个极端月的事件强度（来自 EVT，经 phi 映射后）
- `gap(i, k)`：事件月 `i` 到当前月 `k` 的月数间隔
- `a`：衰减率参数（可调，默认通过实验确定）
- normal 月：不新增 phi 项，仅旧项按 gap 衰减后重新求和

### 5.3 物理含义

- `C_k > 0`：当前累积衰减强度超过 1，视为处于"事件记忆活跃期"
- `C_k = 0`：累积强度正好为 1
- `C_k < 0`：累积强度不足 1，记忆接近消退

### 5.4 Segment 提取

在 `C_k` 序列上找连续活跃段：

```text
active_month = C_k > 0
```

对每个连续活跃段：
- 若同时包含 high 和 low 极端事件 → `transition`（急转段）
- 若仅包含单一类型 → `unilateral`（单边极端段）

### 5.5 Segment 字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `segment_id` | INT | 湖内递增序号 |
| `segment_type` | TEXT | `transition` / `unilateral` |
| `start_year/month`, `end_year/month` | INT | 段起止时间 |
| `duration_months` | INT | 持续月数 |
| `has_high`, `has_low` | BOOL | 是否包含高/低极端事件 |
| `max_C`, `mean_C`, `integral_C` | FLOAT | C_k 统计量 |
| `n_extreme_events` | INT | 段内极端月数 |
| `first_extreme_type`, `last_extreme_type` | TEXT | 首/末极端类型 |

### 5.6 `std = 0` 处理

当某类 extreme 事件 severity 完全相同时（σ = 0），该类所有事件的 phi 值设为固定基准值（如 1.0 或 0.5），不给额外排序权重。

---

## 6. Hawkes 双变量自激点过程

### 6.1 事件映射

```text
extreme_high → WET (event_type = 1)
extreme_low  → DRY (event_type = 0)
```

仅 **transition segment** 内的 extreme 事件进入 Hawkes 拟合。

### 6.2 模型结构

双变量点过程，条件强度函数：

```text
λ_W(t|H_t) = μ_W + Σ α_WW·β_WW·exp(-β_WW·(t-t_W)) + Σ α_DW·β_DW·exp(-β_DW·(t-t_D))
λ_D(t|H_t) = μ_D + Σ α_DD·β_DD·exp(-β_DD·(t-t_D)) + Σ α_WD·β_WD·exp(-β_WD·(t-t_W))
```

- `μ_W, μ_D`：外生基准强度（baseline intensity）
- `α_ij`：激发强度（excitation）
- `β_ij`：核衰减率（kernel decay rate）
- `spectral_radius = α / β`：稳定性条件（需 < 1）

### 6.3 LRT 互激检验

- **Full model**：4 条边全开通
- **Restricted (disabled W→D)**：禁 α_WD → 0
- **Restricted (disabled D→W)**：禁 α_DW → 0
- LRT 检验：-2(ℓ_restricted - ℓ_full) ∼ χ²(2)，检验 α_WD 和 α_DW 是否显著

### 6.4 QC 标准

| 条件 | 阈值 | 说明 |
|------|------|------|
| `min_events` | ≥ 10 | 最少事件数 |
| `min_event_rate` | ≥ 0.01 | 事件数/总月数 |
| `max_event_rate` | ≤ 0.30 | 避免过度标记 |
| `min_relative_amplitude` | ≥ 0.05 | α 相对 μ 的最小比值 |
| `min_median_severity` | ≥ 1.0 | severity 中位数 |
| `spectral_radius` | < 1 | 平稳性条件 |

### 6.5 事件强度标记

Hawkes 输入的 `events_table.severity` 字段可选用：
- 超阈值量 `exceedance`
- C_k 累积强度（当前实验阶段）

### 6.6 输出

| 表 | 说明 |
|----|------|
| `hawkes_results` | 拟合参数 + QC + LRT p 值 |
| `hawkes_lrt` | 各边 LRT 统计量 |
| `hawkes_transition_monthly` | 月度交叉激发显著月 |
| `pwm_hawkes_segments` | 急转段定义 |
| `pwm_hawkes_run_status` | 批处理运行状态 |

---

## 7. 质量筛选层

### 7.1 Frozen Month 过滤

冻结月（冰封/云覆盖/数据缺失）标记为 `frozen_year_months`，不参与计算。

### 7.2 湖泊质量标记

`area_quality` 表存储每湖的质量标记，含：
- `area_anomalies`：融合多个异常检测器的综合标记
- `PV / H×CV`：平序列检测（Shannon Entropy × Coefficient of Variation）
- Shift Filter：结构性退化/间歇性湖泊分类

---

## 8. 模块架构

### 8.1 包结构

```
packages/
├── lakesource/                     # 数据层
│   └── src/lakesource/
│       ├── config.py               # SourceConfig (YAML驱动)
│       ├── provider/               # 数据后端 (postgres/parquet)
│       │   ├── base.py
│       │   ├── parquet_provider.py
│       │   └── postgres_provider.py
│       └── pwm_extreme/
│           ├── schema.py           # PWMExtremeConfig, PWMExtremeResult
│           └── store.py            # row shaper (result_to_*_rows)
│
└── lakeanalysis/                   # 计算层
    └── src/lakeanalysis/
        ├── decomposition/          # STL 分解
        │   ├── base.py             # DecompositionMethod, DecompositionResult
        │   ├── stl_percentile.py   # STLPercentileMethod (推荐)
        │   └── monthly_climatology.py  # MonthlyClimatologyMethod (legacy)
        ├── pwm_extreme/            # PWM 阈值估计
        │   ├── compute.py          # PWM + 交叉熵 + 阈值 + 标签
        │   ├── events.py           # C_k decay + segment extraction
        │   └── service.py          # run_single_lake_service
        ├── eot/                    # EOT/NHPP
        │   ├── preprocess.py       # QuantileThresholdModel, declustering
        │   ├── estimation.py       # NHPPFitter, EOTEstimator
        │   ├── diagnostics.py      # Return levels
        │   └── likelihood.py       # NHPPLogLikelihood
        ├── quantile/               # Quantile baseline
        │   ├── compute.py
        │   └── service.py
        ├── hawkes/                 # Hawkes 互激
        │   ├── types.py            # HawkesEventSeries
        │   └── bridge.py           # build_events_from_pwm/eot
        ├── batch/                  # 批处理框架
        │   ├── engine.py           # Engine (路由)
        │   ├── domain.py           # Calculator, LakeTask
        │   ├── lake_dataset.py     # LakeDataset dense container
        │   ├── lake_dataset_factory.py
        │   ├── calculator/         # 各算法 calculator
        │   │   ├── quantile.py
        │   │   ├── pwm_extreme.py
        │   │   ├── eot.py
        │   │   ├── pwm_hawkes.py
        │   │   └── eot_hawkes.py
        │   ├── worker.py           # MPI Worker
        │   └── manager.py          # MPI Manager
        └── quality/                # 质量筛选
            └── filters/
```

### 8.2 脚本入口

| 脚本 | 算法 | 输出 |
|------|------|------|
| `run_quantile.py` | Quantile baseline | labels, extremes, transitions |
| `run_pwm_extreme.py` | PWM Extreme | thresholds, labels, extremes |
| `run_eot.py` | EOT/NHPP | results, extremes |
| `run_pwm_hawkes.py` | PWM + C_k + Hawkes | hawkes_results, segments |
| `run_hawkes_qc.py` | EOT + Hawkes QC | hawkes_results |

---

## 9. 当前 C_k 实现与修正目标

### 9.1 当前实现（需修正）

`packages/lakeanalysis/src/lakeanalysis/pwm_extreme/events.py:compute_decay_index()`：

```python
f_i = ln(|z_i - 0.5| + 1)          # 错误：双层对数
C_k = Σ f_i · exp(-λ · gap)        # 错误：log域外乘衰减
```

### 9.2 目标公式

```text
C_k = ln( Σ phi_i · exp(-a · gap(i,k)) )
```

其中 `phi_i` 来自 EVT event strength。

### 9.3 修正影响范围

| 文件 | 需修改 |
|------|--------|
| `pwm_extreme/events.py:compute_decay_index()` | 重写为 log-sum-exp 形式 |
| `pwm_extreme/events.py:extract_segments()` | 复查 C_k > 0 阈值 |
| `pwm_extreme/events.py:z_i 计算` | 替换为 EVT-based phi_i |
| `batch/calculator/pwm_hawkes.py` | phi_i 来源切换到 EVT path |
| `docs/memo_pwm_hawkes_decay_algorithm.md` | 由本文档 §5 替代 |

---

## 10. 引用规范

本文档是算法体系的唯一权威来源。以下历史文档已被本文档替代：

### 已替代（内容已整合）
- `docs/research/monthly-anomaly-transition-spec.md` → §3
- `docs/research/EOT模块算法说明.md` → §4, §6
- `docs/research/EOT模块说明.md` → §8
- `docs/research/基于最小交叉熵原理和次序统计量的极端分位数估计.md` → §4.2
- `docs/research/lake_area_heteroscedasticity_analysis.md` → §2
- `docs/research/极端事件识别算法对比.md` → §1, §3, §4
- `docs/research/点过程视角下的极值阈值检验.md` → §4, §6
- `docs/memo_pwm_hawkes_decay_algorithm.md` → §5
- `docs/memo_pwm_event_and_hawkes_plan.md` → §8, §9

### 保留（非算法规范）
- `docs/research/研究方法.md`：研究方法论与贝叶斯同化方案（前瞻性，未全部实现）
- `docs/research/EOT_Hawkes_隐函数贝叶斯同化数学方案.md`：SPAC-gated Hawkes + Bayesian assimilation 数学方案（前瞻性）
- `docs/research/双频同化方案.md`：异步双频 EnKF 同化方案（前瞻性）
- `docs/research/Global_dominance_of_seasonality_in_shaping_lake-surface-extent_dynamics_content.md`：数据源论文
- `docs/research/artificial模块说明.md`：人工湖泊分析模块
- `docs/research/EntropyCalculation.md`：AE 熵计算方法
- `docs/research/Introductiontostatisticalmodelingof extremevalues.md`：EVT 教材参考
- `docs/architecture/`：架构文档（保留）
- `docs/quality-filter-refactor-memo.md`：质量筛选方案
- `docs/change_point_detect.md`：Bai-Perron 变点检测
- `docs/pettitt.md`：Pettitt 变点检测
- `docs/memo_h_cv.md`：HxCV 平序列检测
- `docs/memo/shift_filter_implementation.md`：ShiftFilter 实现备忘
- `docs/lakesource-sql-refactor-notes.md`：SQL 重构笔记
- `docs/reviews/`：代码审查报告（保留）
- `docs/ralph.md`：Ralph 循环编排器文档
