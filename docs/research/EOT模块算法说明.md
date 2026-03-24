## EOT 模块算法说明

本文档说明 `eot/` 模块如何基于月尺度 `water_area` 序列识别高水位与低水位极值，并使用非齐次泊松点过程（NHPP, Non-Homogeneous Poisson Process）进行参数估计、诊断与重现水平计算。

---

## 1. 模块目标

`eot/` 的目标是为每一个湖泊的 23 年逐月 `water_area` 序列提供一套统一的极值识别框架：

1. 识别高水位极值。
2. 识别低水位极值。
3. 通过**时变阈值 u(t)** 解决固定阈值无法适配季节差异的问题——尤其是捕捉"低水位月份中异常偏高"等跨季节极值。
4. 对存在短程依赖的月度极值序列进行去丛化。
5. 估计重现水平并提供模型诊断数据。

---

## 2. 模块结构

`eot/` 被拆为四层：

1. `preprocess.py`
   负责时间轴构造、时变阈值拟合（`QuantileThresholdModel`）与去丛化。
2. `estimation.py`
   负责位置模型、NHPP 对数似然与参数估计。
3. `diagnostics.py`
   负责重现水平计算和残差诊断。
4. `plot.py`
   负责绘制阈值诊断图、PP 图、QQ 图和重现水平图。

高层入口类为 `EOTEstimator`，它将阈值选择、去丛化和 NHPP 拟合串联起来。

---

## 3. 数据表示

### 3.1 输入格式

模块默认输入为一个 `pandas.DataFrame`，至少包含三列：

- `year`
- `month`
- `water_area`

### 3.2 连续时间轴

`MonthlyTimeSeries` 会将离散年月转换为连续时间：

```text
t_i = (year_i - start_year) + (month_i - 1) / 12
```

例如：

- 起始年 2001 年 1 月对应 `t = 0`
- 2001 年 7 月对应 `t = 0.5`
- 2023 年 12 月对应 `t = 22.9167`

这样可以直接将时间带入季节性位置函数 `mu(t)` 和时变阈值 `u(t)`。

### 3.3 高尾与低尾

极值理论通常针对"上尾"建模。为统一高水位和低水位实现，模块采用以下约定：

- 高水位：直接对 `water_area` 建模。
- 低水位：对 `-water_area` 建模，将极小值问题转化为极大值问题。

`MonthlyTimeSeries.for_tail("low")` 会完成这一转换，但仍保留原始值，以便后续绘图和结果输出。

---

## 4. 时变阈值（QuantileThresholdModel）

### 4.1 动机

月度 `water_area` 序列具有强烈的季节性和长期趋势。使用固定阈值时：

- **旱季月份的异常高值** 永远低于丰水期的正常值，因此不会被当作极值。
- 长期增长趋势会使早年的观测更难超过相同的固定阈值，造成样本不均衡。

时变阈值 `u(t)` 通过分位数回归随时间追踪季节和趋势，使得每个时间点的极值判断都基于"相对于当地背景的偏离程度"，从而同时捕捉丰水期和枯水期的极端事件。

### 4.2 QuantileThresholdModel 模型结构

`QuantileThresholdModel` 使用与 `LocationModel` 完全相同的设计矩阵（截距 + 线性趋势 + 三角谐波），对给定分位数 `q` 拟合：

```text
u(t) = a0 + a1 * t + b1 * sin(2πt) + c1 * cos(2πt) + ...
```

拟合准则为 **pinball 损失（check function）**：

```text
L(a) = mean[ q * max(y - X a, 0) + (1-q) * max(X a - y, 0) ]
```

使用 L-BFGS-B 最小化，以普通最小二乘结果作为初始值。

拟合结果满足：

```text
P(X(t) > u(t)) ≈ 1 - q
```

即 `u(t)` 是时间 t 处观测值的局部 `q` 分位数，既跟随长期趋势，也跟随年内季节变化。

### 4.3 参数接口

```python
model = QuantileThresholdModel(include_trend=True, n_harmonics=1)
params = model.fit(times, values, quantile=0.90)   # 返回参数向量
u_obs  = model.evaluate(times, params)             # 返回每个观测点的阈值
u_grid = model.evaluate(integration_grid, params)  # 返回积分格点上的阈值
```

### 4.4 阈值的诊断用途

MRL 图和参数稳定性图中仍使用一组固定候选阈值，主要用于视觉检查尾部行为是否稳定。这两个诊断工具是对时变阈值选择的补充参考，不替代分位数回归的结果。

---

## 5. 去丛化（Declustering）

### 5.1 为什么需要去丛化

月度 `water_area` 极值常具有短程依赖。例如某次持续数月的高水位过程，可能在相邻月份都超过阈值。如果将这些点全部视作独立极值，会破坏泊松点过程的独立增量假设。

### 5.2 DI 设计

模块通过 `DeclusteringStrategy` 协议定义去丛化接口，支持标量或向量阈值：

```python
class DeclusteringStrategy(Protocol):
    def decluster(
        self, series: MonthlyTimeSeries, threshold: float | np.ndarray
    ) -> pd.DataFrame:
        ...
```

默认实现是 `RunsDeclustering`，但 `EOTEstimator` 并不依赖具体实现，而是通过依赖注入接收一个去丛化策略对象：

```python
estimator = EOTEstimator(
    declustering_strategy=RunsDeclustering(run_length=1)
)
```

这意味着后续若需要替换为更复杂的聚类判定规则，不需要修改 `EOTEstimator` 本身。

### 5.3 RunsDeclustering 规则

设运行长度为 `r`，时变阈值向量为 `u(t_i)`：

1. 找出全部满足 `x_i > u(t_i)` 的超阈值观测。
2. 若两个相邻超阈值点之间插入的非超阈值点数小于 `r`，则视为同一簇。
3. 若间隔达到或超过 `r`，则认为前一个簇结束。
4. 每个簇只保留一个代表值（簇内最大值）。

每个代表值对应的 `threshold` 字段记录该时间点的 `u(t_i)`，方便后续使用。

默认 `run_length = 1`，表示只要出现一个非超阈值月，就视为新簇开始。

---

## 6. 非齐次泊松点过程模型

### 6.1 位置参数模型

模块使用 `LocationModel` 表示时变位置参数：

```text
mu(t) = beta0 + beta1 * t + sum_k [a_k sin(2πkt) + b_k cos(2πkt)]
```

默认配置：

- 含线性趋势项 `beta1`
- 使用 1 阶傅里叶季节项

因此默认参数为：

```text
beta0, beta1, sin_1, cos_1
```

可以通过 `LocationModel(include_trend=False, n_harmonics=2)` 等方式切换模型复杂度。

### 6.2 NHPP 强度函数（时变阈值版）

对 declustered exceedances 使用非齐次点过程似然。阈值 `u(t)` 现在是时变的，积分项需在整个观测期的 `u(t)` 网格上积分：

- 时变位置：`mu(t)`
- 时变阈值：`u(t)`（由 `QuantileThresholdModel` 提供）
- 固定尺度：`sigma > 0`
- 固定形状：`xi`

当 `xi != 0` 时，对数似然为：

```text
l(theta)
= - ∫ [1 + xi * (u(t) - mu(t)) / sigma]^(-1/xi) dt
  + Σ log{ (1/sigma) * [1 + xi * (x_j - mu(t_j)) / sigma]^(-1/xi - 1) }
```

注意：积分项中使用 `u(t)`（随时间变化），密度项只使用 `x_j - mu(t_j)`（不直接涉及阈值）。

当 `xi -> 0` 时，使用 Gumbel 极限形式：

```text
∫ exp(-(u(t) - mu(t)) / sigma) dt
```

以及对应的指数型密度。

### 6.3 数值实现

`NHPPLogLikelihood` 接受 `u_grid` 参数（在积分格点上预计算的阈值向量），将其直接用于积分项：

```python
objective = NHPPLogLikelihood(
    series=series, extremes=extremes, threshold=rep_threshold,
    location_model=location_model, u_grid=u_grid
)
```

若 `u_grid=None`，则退回到常数阈值（向量广播为标量），保持向后兼容。

优化参数顺序为：

```text
[location parameters..., sigma, xi]
```

约束：

- `sigma > 0`
- `xi ∈ [-0.95, 0.95]`
- 所有观测点和积分点均需满足支持域条件

---

## 7. 参数初始化

### 7.1 位置参数初始化

对整个变换后的月序列做最小二乘回归：

```text
value(t) ~ intercept + trend + seasonal harmonics
```

所得系数作为 `mu(t)` 的初值。

### 7.2 尺度与形状初始化

对 declustered exceedances 的超额 `x - u_median`（用阈值中位数代替固定阈值）拟合一个静态 GPD，得到：

- `sigma`
- `xi`

若 GPD 拟合失败，则退回到基于样本标准差的保底初始化。

---

## 8. 高层入口：EOTEstimator

`EOTEstimator` 是模块的主入口，负责把各步骤连接起来。

### 8.1 工作流程

给定一个湖泊月序列，`fit()` 的流程为：

1. 构造 `MonthlyTimeSeries`
2. 根据 `tail` 选择高尾或低尾变换
3. 使用 `QuantileThresholdModel` 拟合时变阈值 `u(t)`（或使用用户指定的固定阈值）
4. 将 `u(t)` 向量传入注入的 `DeclusteringStrategy`，识别每个时间点相对于局部背景的极端事件
5. 在积分格点上预计算 `u(t_grid)`，传入 `NHPPFitter` 的似然函数
6. 使用 `NHPPFitter` 拟合点过程参数
7. 返回 `FitResult`，其中保存了 `threshold_model` 和 `threshold_params`，可在任意时间点重建 `u(t)`

### 8.2 主要方法

- `estimate_threshold()`：返回时变阈值中位数（标量摘要）
- `threshold_diagnostics()`：返回 MRL 和稳定性图数据
- `prepare_extremes()`：返回处理后的序列、代表阈值、去丛化极值、threshold_params 和 u_grid
- `fit()`：完成完整拟合

---

## 9. 拟合结果对象

`FitResult` 用于封装拟合结果，包含：

- 参数向量 `theta`
- 参数名 `param_names`
- 协方差矩阵 `covariance`
- 代表阈值 `threshold`（时变阈值的中位数，用于参考和初始化）
- 极值方向 `tail`
- 对数似然 `log_likelihood`
- 是否收敛 `converged`
- 优化器消息 `message`
- 原始 `series`
- 去丛化后的 `extremes`
- 时变阈值模型 `threshold_model`（`QuantileThresholdModel` 实例，或 None）
- 时变阈值参数 `threshold_params`（参数向量，或 None）

并提供：

- `params`：字典形式参数
- `sigma`
- `xi`
- `mu(times)`：在任意时间点评估位置函数
- `threshold_at(times)`：在任意时间点评估 u(t)，无时变模型时返回常数数组

---

## 10. 重现水平

### 10.1 定义

设 `z_T` 为 `T` 年重现水平，则其定义为：

```text
T * ∫ intensity exceedance above z_T over one-year-equivalent rate = 1
```

在本实现中，使用与拟合相同的时间强度结构，通过数值求根获得 `z_T`。

### 10.2 数值求解

`ReturnLevelEstimator` 使用 `brentq` 求解：

```text
T * ∫ [1 + xi * (z - mu(t)) / sigma]^(-1/xi) dt - 1 = 0
```

对于 `xi -> 0`，改用指数极限形式。

### 10.3 低水位结果

低水位模型是在 `-water_area` 上拟合的，因此求得的返回值会在输出阶段乘以 `-1` 变回原始尺度。

---

## 11. 模型诊断

### 11.1 残差变换

非平稳模型下，直接对原始超额做 QQ/PP 图并不方便，因此 `ModelChecker` 会将每个超额点变换为理论上应服从单位指数分布的残差：

- 若 `xi = 0`，残差与超额成比例
- 若 `xi != 0`，使用标准极值变换得到指数残差

### 11.2 PP 图

`probability_plot_data()` 返回：

- 经验概率
- 模型概率

若模型良好，散点应靠近对角线。

### 11.3 QQ 图

`quantile_plot_data()` 将变换残差与单位指数分布的理论分位作比较。

若模型拟合合理，点应接近直线。

---

## 12. 绘图函数

`plot.py` 提供以下图形接口：

- `plot_mrl()`
- `plot_parameter_stability()`
- `plot_extremes_timeline(series, extremes, threshold, fit_result=None)`
  — 当 `fit_result` 携带时变阈值参数时，绘制 u(t) 曲线代替水平虚线。
- `plot_pp()`
- `plot_qq()`
- `plot_return_levels()`
- `plot_location_model(fit_result)`
  — 当 `fit_result` 携带时变阈值参数时，同时绘制 mu(t) 曲线和 u(t) 曲线，便于直观对比。

绘图层仅负责将数值结果转成 `matplotlib` 图对象，不承担统计计算逻辑。

---

## 13. 使用示例

### 13.1 拟合高水位（时变阈值，默认）

```python
import pandas as pd
from eot import EOTEstimator

df = pd.read_csv("lake_123.csv")
estimator = EOTEstimator()
fit_result = estimator.fit(df, tail="high", threshold_quantile=0.90)
print(fit_result.params)

# 在任意时间点评估阈值 u(t)
import numpy as np
t_grid = np.linspace(0, 23, 300)
u_curve = fit_result.threshold_at(t_grid)
```

### 13.2 拟合低水位

```python
from eot import EOTEstimator

fit_result = EOTEstimator().fit(df, tail="low")
print(fit_result.params)
```

### 13.3 注入自定义去丛化策略

```python
from eot import EOTEstimator, RunsDeclustering

estimator = EOTEstimator(
    declustering_strategy=RunsDeclustering(run_length=2)
)
fit_result = estimator.fit(df, tail="high", threshold_quantile=0.92)
```

### 13.4 阈值诊断

```python
diagnostics = estimator.threshold_diagnostics(df, tail="high")
mrl_df = diagnostics["mrl"]
stability_df = diagnostics["stability"]
```

### 13.5 使用固定阈值（跳过时变拟合）

```python
fit_result = estimator.fit(df, tail="high", threshold=1500.0)
```

---

## 14. 当前实现边界

当前版本的实现有以下边界：

1. 默认假设尺度参数 `sigma` 与形状参数 `xi` 不随时间变化（非平稳仅体现在 `mu(t)` 和 `u(t)`）。
2. 去丛化默认使用 runs rule，不含更复杂的水文事件边界识别。
3. 时变阈值通过 L-BFGS-B 优化 pinball 损失得到，与 LocationModel 使用相同设计矩阵但独立拟合。
4. 返回水平置信区间使用 Delta 法的数值梯度近似，对极少样本或边界形状参数时可能不稳定。
5. 返回水平计算中的积分使用代表阈值（中位数），而非完整的时变 u(t)，是一种近似。

---

## 15. 推荐落地流程

针对每个湖泊，推荐采用如下流程：

1. 读取 23 年逐月 `water_area` 数据。
2. 对高尾和低尾分别做阈值诊断（MRL、稳定性图）作为背景参考。
3. 使用 `threshold_quantile=0.90`（或经诊断图调整的分位数）拟合时变阈值 `u(t)`。
4. 设定合适的 `RunsDeclustering(run_length=1 or 2)`。
5. 拟合 NHPP。
6. 查看 `mu(t)` + `u(t)` 曲线图、极值时间轴图、PP 图、QQ 图和重现水平图。
7. 对模型异常或不收敛的湖泊做单独复核。

这样可以在保持统一理论框架的同时，兼顾湖泊季节性、长期趋势和高低水位极值的差异，尤其能捕捉"低水位月份中的异常高值"等跨季节极端事件。
