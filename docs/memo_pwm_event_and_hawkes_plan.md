## PWM Event And Hawkes Plan

### 背景

当前 `pwm_extreme` 已经具备“月尺度极端识别”的核心计算能力，但结果只落了月阈值与 run status，没有像 `quantile` 和 `eot` 那样形成事件级数据资产。

现状可以概括为：

1. `compute_monthly_thresholds()` 已经返回 `labels_df`，其中包含 `extreme_label`
2. `extreme_high -> wet event`、`extreme_low -> dry event` 的语义已经明确
3. `pwm_extreme` 的 batch calculator 目前只写 `pwm_extreme_thresholds` 和 `pwm_extreme_run_status`
4. `run_hawkes_batch.py` 仍是脚本式旧流程，且强耦合 EOT `NoDeclustering()` 事件来源

因此，比较稳妥的落地路径不是直接把 PWM 塞进现有 `run_hawkes_batch.py`，而是先把 PWM 的事件级识别补齐到和 `quantile` 对齐，再在统一 batch 架构中新增 `pwm_hawkes`。

### 总体原则

1. 优先复用 `quantile` 事件识别链路，不重复设计 labels/extremes/transitions 的数据流
2. 优先复用统一 `Engine -> Calculator -> ProviderBatchWriter` 批处理架构，不继续扩大脚本式 `run_hawkes_batch.py`
3. 优先复用现有 Hawkes 拟合、LRT、monthly transition 输出逻辑，不重写 Hawkes 内核
4. PWM-Hawkes 的输入必须是去从化后的事件序列，不采用 `NoDeclustering()`

### 两阶段实施

建议拆成两个阶段：

1. 第一阶段：为 PWM 增加事件级识别、存储、聚合、可视化
2. 第二阶段：在统一 batch 架构下新增 PWM-Hawkes 建模

这样做的原因是：

1. 第一阶段完成后，PWM 的事件结果就可以独立检查、统计和作图
2. 第二阶段只需要消费“已经成型的事件表”，不必在 Hawkes 流程里重复做阈值识别
3. 这条路径与 `quantile -> quantile_extremes / quantile_abrupt_transitions` 的项目风格一致

---

## 第一阶段：PWM 事件级识别

### 目标

让 `pwm_extreme` 像 `quantile` 一样，形成完整的：

1. `labels`
2. `extremes`
3. `abrupt_transitions`
4. global map queries
5. CLI 作图入口

这一步不直接做 Hawkes，但它是 PWM-Hawkes 的必要前置。

### A. 计算层

参考模板：

1. `packages/lakeanalysis/src/lakeanalysis/quantile/compute.py`
2. `extract_extreme_events()`
3. `detect_abrupt_transitions()`

目标文件：

`packages/lakeanalysis/src/lakeanalysis/pwm_extreme/compute.py`

建议新增函数：

1. `extract_pwm_extreme_events(labeled_df: pd.DataFrame) -> pd.DataFrame`
2. `detect_pwm_abrupt_transitions(labeled_df: pd.DataFrame) -> pd.DataFrame`

逻辑直接对齐 quantile：

1. 从 `labels_df` 中筛选 `extreme_label != "normal"`
2. 把 `extreme_high` 映射为 `event_type="high"`
3. 把 `extreme_low` 映射为 `event_type="low"`
4. threshold 字段使用 PWM 每月阈值：
   high 对应 `threshold_high`
   low 对应 `threshold_low`
5. abrupt transition 仍按 `month_ordinal` 差值为 1 判定相邻
6. 仍只检测：
   `extreme_low -> extreme_high`
   `extreme_high -> extreme_low`

与 quantile 的一个重要差异是字段来源：

1. quantile 事件依赖 `anomaly/q_low/q_high`
2. PWM 事件依赖 `water_area/threshold_low/threshold_high`

建议事件表字段至少包含：

1. `hylak_id`
2. `year`
3. `month`
4. `event_type`
5. `water_area`
6. `mean_area`
7. `threshold`
8. `extreme_label`

如果希望为第二阶段 Hawkes 预留信息，可以在第一阶段就一并加上：

1. `severity`
   high: `water_area - threshold_high`
   low: `threshold_low - water_area`

建议同步扩展 dataclass：

目标文件：

`packages/lakesource/src/lakesource/pwm_extreme/schema.py`

把 `PWMExtremeResult` 从当前：

1. `hylak_id`
2. `month_results`
3. `labels_df`

扩展为：

1. `hylak_id`
2. `month_results`
3. `labels_df`
4. `extremes_df`
5. `transitions_df`

同时，`compute_monthly_thresholds()` 在返回前补充：

1. `extremes_df = extract_pwm_extreme_events(labeled_df)`
2. `transitions_df = detect_pwm_abrupt_transitions(labeled_df)`

### B. 存储层

参考模板：

`packages/lakesource/src/lakesource/postgres/lake_quantile.py`

目标文件：

`packages/lakesource/src/lakesource/postgres/lake_pwm.py`

建议新增三张表：

1. `pwm_extreme_labels`
2. `pwm_extreme_extremes`
3. `pwm_extreme_abrupt_transitions`

与 quantile 对齐关系：

1. `pwm_extreme_labels` 对齐 `quantile_labels`
2. `pwm_extreme_extremes` 对齐 `quantile_extremes`
3. `pwm_extreme_abrupt_transitions` 对齐 `quantile_abrupt_transitions`

建议字段设计：

`pwm_extreme_labels`

1. `hylak_id`
2. `year`
3. `month`
4. `workflow_version`
5. `water_area`
6. `mean_area`
7. `threshold_low`
8. `threshold_high`
9. `extreme_label`
10. `computed_at`

`pwm_extreme_extremes`

1. `hylak_id`
2. `year`
3. `month`
4. `workflow_version`
5. `event_type`
6. `water_area`
7. `mean_area`
8. `threshold`
9. `severity`
10. `computed_at`

`pwm_extreme_abrupt_transitions`

1. `hylak_id`
2. `from_year`
3. `from_month`
4. `to_year`
5. `to_month`
6. `workflow_version`
7. `transition_type`
8. `from_label`
9. `to_label`
10. `from_water_area`
11. `to_water_area`
12. `computed_at`

说明：

1. 第一阶段的 transitions 可直接存水域面积，不必强行模仿 quantile 的 `from_anomaly/to_anomaly`
2. 如果希望与 quantile 字段更统一，也可以用 `from_threshold_gap/to_threshold_gap`，但不是必需
3. 优先保证“可读、可查、可画图”

### C. Store + Upsert + Dispatch

这一层基本是机械性延伸，直接复用 quantile 模板。

目标文件：

1. `packages/lakesource/src/lakesource/pwm_extreme/store.py`
2. `packages/lakesource/src/lakesource/postgres/repositories/algorithm_writes.py`
3. `packages/lakesource/src/lakesource/provider/postgres_provider.py`
4. `packages/lakeanalysis/src/lakeanalysis/batch/calculator/pwm_extreme.py`
5. `packages/lakesource/src/lakesource/provider/ports/algorithms.py`

需要新增的 store row shaper：

1. `result_to_label_rows()`
2. `result_to_extreme_rows()`
3. `result_to_transition_rows()`

需要新增的 repository 方法：

1. `upsert_pwm_extreme_labels()`
2. `upsert_pwm_extreme_extremes()`
3. `upsert_pwm_extreme_abrupt_transitions()`

需要新增的 provider dispatch：

1. `_UPSERT_DISPATCH["pwm_extreme_labels"]`
2. `_UPSERT_DISPATCH["pwm_extreme_extremes"]`
3. `_UPSERT_DISPATCH["pwm_extreme_abrupt_transitions"]`

如果 `ensure_pwm_extreme_tables()` 一次性创建所有 PWM 相关表，则 `_ENSURE_DISPATCH` 无需新增多个入口，继续保留：

1. `"pwm_extreme": ("pwm", "ensure_pwm_extreme_tables")`

batch calculator 需要扩展：

目标文件：

`packages/lakeanalysis/src/lakeanalysis/batch/calculator/pwm_extreme.py`

当前 `result_to_rows()` 只返回：

1. `pwm_extreme_thresholds`
2. `pwm_extreme_run_status`

扩展后应返回：

1. `pwm_extreme_thresholds`
2. `pwm_extreme_labels`
3. `pwm_extreme_extremes`
4. `pwm_extreme_abrupt_transitions`
5. `pwm_extreme_run_status`

这样可以完全复用现有 `Engine -> ProviderBatchWriter.persist()` 分发表名写库的机制。

### D. 网格聚合

参考模板：

1. `packages/lakesource/src/lakesource/quantile/grid_queries.py`
2. `packages/lakesource/src/lakesource/provider/base.py`

目标文件：

1. `packages/lakesource/src/lakesource/pwm_extreme/reader.py`
2. `packages/lakesource/src/lakesource/pwm_extreme/grid_queries.py`
3. `packages/lakesource/src/lakesource/provider/base.py`

建议新增 query key：

1. `pwm.extremes`
2. `pwm.extremes_by_type`
3. `pwm.transitions`
4. `pwm.transitions_by_type`

聚合源表：

1. `pwm.extremes` 基于 `pwm_extreme_extremes JOIN lake_info`
2. `pwm.extremes_by_type` 在上面基础上按 `event_type` 分组
3. `pwm.transitions` 基于 `pwm_extreme_abrupt_transitions JOIN lake_info`
4. `pwm.transitions_by_type` 在上面基础上按 `transition_type` 分组

建议在 `LakeProvider` 上新增便捷方法：

1. `fetch_pwm_extremes_grid_agg()`
2. `fetch_pwm_extremes_by_type_grid_agg()`
3. `fetch_pwm_transitions_grid_agg()`
4. `fetch_pwm_transitions_by_type_grid_agg()`

说明：

1. 这层不需要新机制，只需沿用当前 grid query registry 模式
2. `postgres_provider.py` 已经自动导入 `lakesource.pwm_extreme.grid_queries`，因此只需要在该模块注册新增 query 类

### E. 可视化

参考模板：

1. `lakeviz` 的 quantile global map 体系
2. 现有 `lakeviz/pwm_extreme/global_map.py`

目标文件：

`packages/lakeviz/src/lakeviz/pwm_extreme/global_map.py`

建议新增 8 张图，与 quantile 对称：

1. 极端事件密度，按每湖平均
2. 极端事件密度，按总数平滑
3. 高值极端事件分布
4. 低值极端事件分布
5. 突变密度，按每湖平均
6. 突变密度，按总数平滑
7. 低转高突变分布
8. 高转低突变分布

建议渲染策略：

1. `make_grid_map` 用于基于聚合栅格的 pcolormesh 图
2. `make_density_map` 用于事件总数的平滑密度图
3. `by_type` 图直接通过 query 结果中过滤 `event_type` 或 `transition_type` 生成

建议命名与现有 PWM 图保持一致，例如：

1. `plot_pwm_extremes_density_map()`
2. `plot_pwm_extremes_count_density_map()`
3. `plot_pwm_high_extremes_map()`
4. `plot_pwm_low_extremes_map()`
5. `plot_pwm_transitions_density_map()`
6. `plot_pwm_transitions_count_density_map()`
7. `plot_pwm_low_to_high_transitions_map()`
8. `plot_pwm_high_to_low_transitions_map()`

如果现有模块已经有更贴近项目风格的命名，应优先复用现有前缀与后缀习惯。

### F. CLI

目标文件：

`packages/lakeanalysis/scripts/plot_pwm_extreme_global.py`

在现有 threshold/exceedance 图的基础上，新增事件级图调用。

建议：

1. 保留现有 CLI 参数，不额外扩大复杂度
2. 先直接 import 新 plot 函数并加入 `plot_fns`
3. 如有必要，后续再拆 `--events-only` 或 `--transitions-only`

### 第一阶段验收标准

1. `run_pwm_extreme.py` 执行后，除阈值表外还能写出 labels/extremes/transitions 表
2. `plot_pwm_extreme_global.py` 可以生成 PWM 事件级和突变级全球图
3. 事件/突变表能支持单湖排查与后续 Hawkes 输入检查

---

## 第二阶段：PWM-Hawkes 接入

### 目标

在第一阶段输出的 PWM 事件数据基础上，构建去从化后的 dry/wet 点过程，并接入 Hawkes 建模。

### 核心判断

`run_hawkes_batch.py` 当前是旧脚本式流程，不建议直接扩展为 PWM 和 EOT 的双分支总控。

依据：

1. 它不走统一 `Engine/Calculator`
2. 它直接在脚本内部 new `EOTEstimator(declustering_strategy=NoDeclustering())`
3. 它同时承担读库、并发、EOT、QC、Hawkes、绘图、写文件和写库职责

因此，PWM-Hawkes 应采用新 algorithm，而不是扩展旧脚本。

### 事件定义

PWM-Hawkes 采用以下固定映射：

1. `extreme_high -> wet event`
2. `extreme_low -> dry event`

并且必须做去从化。

### 去从化规则

默认规则已经确认：

1. `run_length = 1`
2. 只对同类型极端做 runs declustering
3. `normal` 月天然切断 cluster
4. `high` 与 `low` 不跨类型合并
5. cluster 代表点取 `severity` 最大的月，而不是首月
6. 进入 Hawkes 的是去从化后的事件，而不是原始极端月标签

建议把 PWM 去从化做成独立函数，不写死在 calculator 内部。

### 模块边界

建议新增模块：

1. `packages/lakeanalysis/src/lakeanalysis/pwm_extreme/events.py`
2. `packages/lakeanalysis/src/lakeanalysis/batch/calculator/pwm_hawkes.py`
3. `packages/lakeanalysis/scripts/run_pwm_hawkes.py`

其中：

`pwm_extreme/events.py` 负责：

1. 从 `pwm_extreme_labels` 或 `PWMExtremeResult.labels_df` 抽取 raw extreme months
2. 执行 PWM 专属 runs declustering
3. 生成 declustered event table
4. 构造 `HawkesEventSeries`

`pwm_hawkes.py` 负责：

1. 读取单湖时序
2. 调用 `run_single_lake_service()` 或直接消费已入库 PWM 事件
3. 生成去从化后的 Hawkes 输入
4. 执行 QC、full/restricted fit、LRT、monthly transition
5. 返回 `hawkes_results`、`hawkes_lrt`、`hawkes_transition_monthly`、run status

### Hawkes 逻辑复用

不重写 Hawkes 内核，只复用以下现有能力：

1. `HawkesEventSeries`
2. `fit_full_model()`
3. `fit_restricted_model()`
4. `LikelihoodRatioTest`
5. `run_model_comparison()`
6. `evaluate_intensity_decomposition()`

建议从 `run_hawkes_batch.py` 抽取以下公共函数到新的 `lakeanalysis.hawkes.pipeline` 或类似模块：

1. QC 计算函数
2. `hawkes_results` row 组装函数
3. `hawkes_transition_monthly` row 组装函数

这样：

1. PWM-Hawkes 复用它们
2. 旧 `run_hawkes_batch.py` 后续也可以反向复用

### 统一 batch 接入

建议在统一 batch 架构中新增 algorithm：

1. `pwm_hawkes`

需要修改：

1. `packages/lakeanalysis/src/lakeanalysis/batch/task_spec.py`
2. `packages/lakeanalysis/src/lakeanalysis/batch/calculator/factory.py`
3. 新增 `packages/lakeanalysis/src/lakeanalysis/batch/calculator/pwm_hawkes.py`

建议为 `pwm_hawkes` 增加独立 run status 表，例如：

1. `pwm_hawkes_run_status`

这样可以避免与 `pwm_extreme_run_status` 混淆任务完成边界。

### 第一版输入来源选择

有两种实现路径：

1. `pwm_hawkes` 每次运行时先现场调用 `run_single_lake_service()` 生成 labels，再去从化，再 Hawkes
2. `pwm_hawkes` 直接读取第一阶段已经入库的 `pwm_extreme_labels` 或 `pwm_extreme_extremes`

建议第一版采用路径 1。

原因：

1. 统一 batch reader 当前主要读 `lake_area` 与 frozen months
2. 这样不需要在第二阶段一开始就扩展更多读取接口
3. 可以先跑通建模，再决定是否增加“直接从 PWM 事件表读取”的优化路径

当第一版稳定后，再评估是否增加数据库读取复用，减少重复计算。

### 第二阶段验收标准

1. `run_pwm_hawkes.py` 能在统一 batch 架构下跑通
2. 输入事件来自 PWM labels，经过去从化
3. 输出写入现有 `hawkes_results`、`hawkes_lrt`、`hawkes_transition_monthly`
4. 可以稳定区分 QC fail、fit fail、success 三类状态

---

## 推荐开发顺序

### Step 1

补 `PWMExtremeResult.extremes_df` 和 `transitions_df`

### Step 2

在 `compute.py` 中实现：

1. `extract_pwm_extreme_events()`
2. `detect_pwm_abrupt_transitions()`

### Step 3

扩展 `lake_pwm.py`、`store.py`、repository、provider dispatch，使 PWM batch 能写：

1. `pwm_extreme_labels`
2. `pwm_extreme_extremes`
3. `pwm_extreme_abrupt_transitions`

### Step 4

补 PWM grid queries 与 global maps

### Step 5

为 PWM event extraction 和 row shaping 补测试

### Step 6

抽取 `run_hawkes_batch.py` 中的可复用 Hawkes pipeline 逻辑

### Step 7

实现 `pwm_extreme/events.py` 的去从化与 `HawkesEventSeries` 组装

### Step 8

实现 `pwm_hawkes` calculator 与 `run_pwm_hawkes.py`

### Step 9

跑小样本湖泊，检查：

1. 事件数
2. cluster_size 分布
3. QC 通过率
4. LRT 输出
5. monthly transition 输出

---

## 风险与注意事项

1. 第一阶段的 `pwm_extreme_extremes` 是“逐月极端事件”，不是 Hawkes 最终输入
2. Hawkes 真正输入必须是第二阶段去从化后的事件序列
3. 不建议把去从化逻辑混进第一阶段的 `pwm_extreme_extremes` 表，否则会把“事件识别”和“点过程建模预处理”耦合在一起
4. 不建议直接在旧 `run_hawkes_batch.py` 中增加 PWM 分支，这会扩大脚本耦合并绕开新 batch 架构
5. 若后续发现 PWM-Hawkes 需要长期保存去从化事件，可在第二阶段追加一张 `pwm_hawkes_events` 或 `pwm_extreme_declust_events` 表，但不建议在第一版就引入

---

## 最终建议

本任务应按如下口径推进：

1. 第一阶段先把 PWM 做成和 quantile 对齐的事件级资产
2. 第二阶段再把这些资产接入 Hawkes
3. Hawkes 接入走统一 batch 新 algorithm，不继续扩展脚本式旧架构
4. 去从化是 PWM-Hawkes 的默认前提，不可省略

这条路径改动最小、复用最多，也最符合当前仓库的演进方向。
