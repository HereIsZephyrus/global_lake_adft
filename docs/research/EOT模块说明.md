# EOT 模块说明

## 概述

`eot` 模块用于对湖泊 `water_area` 的 23 年逐月序列做极值识别与点过程建模。它面向两类任务：

1. 识别高水位极值与低水位极值。
2. 在考虑季节性的前提下，对极值事件做非齐次泊松点过程拟合。

与传统固定阈值 + 静态 GPD 的做法相比，`eot` 更强调两点：

- 月尺度数据具有明显季节性，同一个绝对面积值在不同月份的异常程度不同。
- 连续月份常出现相依的极值簇，需要通过去丛化降低对 NHPP 独立性假设的破坏。

---

## 适用数据

模块默认输入为一个 `pandas.DataFrame`，至少包含：

- `year`
- `month`
- `water_area`

数据源通常来自 `SERIES_DB.lake_area`，在当前仓库中可通过 `dbconnect.fetch_lake_area_by_ids()` 或 `dbconnect.fetch_lake_area_chunk()` 获取。

---

## 文件结构

```text
eot/
├── __init__.py       # 公共 API 导出
├── preprocess.py     # 时间轴转换、阈值诊断、去丛化
├── estimation.py     # 位置模型、NHPP 似然、MLE 拟合、高层入口
├── diagnostics.py    # 返回水平与 PP/QQ 诊断
└── plot.py           # 可视化

scripts/
└── run_eot.py        # 单湖 EOT 调试入口
```

---

## 模块分层

### 1. `preprocess.py`

负责拟合前的数据整理与诊断。

核心对象：

- `MonthlyTimeSeries`
  - 将 `(year, month)` 转为连续时间轴 `time`
  - 保留 `value` 和 `original_value`
  - 支持 `for_tail("high" | "low")`

- `ThresholdSelector`
  - 计算平均残余生命图数据
  - 计算 GPD 参数稳定性图数据
  - 提供基于分位数的默认阈值

- `DeclusteringStrategy`
  - 去丛化策略接口
  - 让估计器依赖抽象而非具体实现

- `RunsDeclustering`
  - 标准 runs declustering
  - 连续超阈值点按运行长度规则合并为同一簇

- `NoDeclustering`
  - 不做去丛化
  - 所有超阈值点原样保留
  - 适合对比 runs 去丛化前后的拟合差异

### 2. `estimation.py`

负责非齐次泊松点过程拟合。

核心对象：

- `LocationModel`
  - 定义时变位置参数 `mu(t)`
  - 默认包含截距、线性趋势和一阶季节 Fourier 项

- `NHPPLogLikelihood`
  - 实现非齐次泊松点过程负对数似然
  - 对阈值以上极值点进行联合建模

- `NHPPFitter`
  - 封装数值优化
  - 使用多初值 + 回退优化方法提高收敛稳健性

- `FitResult`
  - 保存拟合参数、协方差矩阵、阈值、极值点和收敛状态

- `EOTEstimator`
  - 高层入口
  - 串联阈值选择、去丛化和 NHPP 拟合

### 3. `diagnostics.py`

负责拟合后的定量诊断。

核心对象：

- `ReturnLevelEstimator`
  - 计算多个返回期对应的返回水平
  - 支持 Delta 法近似置信区间

- `ModelChecker`
  - 构造 PP 图和 QQ 图使用的数值数据
  - 将非平稳模型下的超额变换到理论分布尺度

### 4. `plot.py`

只负责把数值结果转换为 `matplotlib` 图对象。

包含：

- `plot_mrl()`
- `plot_parameter_stability()`
- `plot_extremes_timeline()`
- `plot_location_model()`
- `plot_pp()`
- `plot_qq()`
- `plot_return_levels()`

---

## 关键设计

### 1. 高尾与低尾统一

模块内部统一按“上尾”处理：

- 高水位：直接建模 `water_area`
- 低水位：建模 `-water_area`

这样低水位问题会转化为极大值问题，从而复用同一套阈值选择、去丛化和 NHPP 拟合代码。

### 2. 去丛化使用依赖注入

`EOTEstimator` 不依赖固定的去丛化实现，而是依赖 `DeclusteringStrategy` 接口。

例如：

```python
from eot import EOTEstimator, NoDeclustering, RunsDeclustering

estimator_a = EOTEstimator(
    declustering_strategy=RunsDeclustering(run_length=1),
)

estimator_b = EOTEstimator(
    declustering_strategy=NoDeclustering(),
)
```

这样可以非常方便地比较：

- 使用 runs 去丛化后的极值点数
- 不去丛化时的参数估计偏移
- 返回水平结果的变化

### 3. 单湖调试优先

当前阶段 `eot` 主要提供的是单湖调试与建模能力，而不是全量批处理流水线。原因是：

- 阈值与去丛化设置需要先在少量样本上调试
- 高尾和低尾的拟合稳定性往往需要人工检查
- PP/QQ 图和返回水平图更适合针对单个湖泊逐一判断

因此仓库中提供了 `scripts/run_eot.py` 作为调试入口。

---

## 调试脚本

### 脚本位置

`scripts/run_eot.py`

### 功能

给定单个 `hylak_id`：

1. 从 `SERIES_DB.lake_area` 读取月序列
2. 选择高尾、低尾或两者都做
3. 选择 runs 去丛化或不去丛化
4. 拟合 NHPP
5. 输出 JSON/CSV 结果
6. 可选保存图表到 `data/eot/<hylak_id>/<tail>/`

### 主要参数

| 参数 | 说明 |
|---|---|
| `--hylak-id` | 目标湖泊 ID，必填 |
| `--tail high|low|both` | 调试高尾、低尾或同时调试 |
| `--threshold` | 手动指定阈值 |
| `--threshold-quantile` | 自动阈值分位数，默认 `0.90` |
| `--run-length` | runs 去丛化运行长度，默认 `1` |
| `--no-decluster` | 不去丛化，保留全部超阈值点 |
| `--return-periods` | 返回期列表，如 `10 25 50 100` |
| `--plot` | 保存诊断图 |

### 示例

```bash
# 高尾调试
uv run python scripts/run_eot.py --hylak-id 1023 --plot

# 低尾调试
uv run python scripts/run_eot.py --hylak-id 1023 --tail low --plot

# 不去丛化，方便对照
uv run python scripts/run_eot.py --hylak-id 1023 --no-decluster --plot

# 同时调试高尾和低尾
uv run python scripts/run_eot.py --hylak-id 1023 --tail both --plot

# 手动阈值 + 更长 runs 间隔
uv run python scripts/run_eot.py --hylak-id 1023 --threshold 120.0 --run-length 2 --plot
```

---

## 输出目录

运行 `scripts/run_eot.py` 后，结果会写到：

```text
data/eot/<hylak_id>/<tail>/
```

包含：

- `summary.json`
  - 拟合结果摘要
- `mrl.csv`
  - MRL 图数据
- `stability.csv`
  - 参数稳定性图数据
- `return_levels.csv`
  - 返回水平估计
- `mrl.png`
- `stability.png`
- `extremes_timeline.png`
- `location_model.png`
- `probability_plot.png`
- `quantile_plot.png`
- `return_levels.png`

---

## 推荐调试流程

建议按如下顺序调试一个湖泊：

1. 用 `--plot` 先看高尾结果。
2. 再用 `--tail low` 看低尾结果。
3. 使用 `--no-decluster` 与默认 runs 去丛化做对照。
4. 比较两种设定下：
   - `n_extremes`
   - `threshold`
   - 参数 `sigma`、`xi`
   - 返回水平
5. 若参数不稳定，再尝试：
   - 调整 `--threshold-quantile`
   - 改成手动 `--threshold`
   - 调整 `--run-length`

---

## 与算法文档的关系

当前仓库中有两份与 EOT 相关的文档：

- `docs/research/EOT模块说明.md`
  - 偏工程实现、模块结构和调试方法

- `docs/research/EOT模块算法说明.md`
  - 偏数学背景、建模流程和公式说明

如果你想快速上手调试，看本文件即可；如果你想回到极值点过程的数学定义和推导，看算法说明更合适。
