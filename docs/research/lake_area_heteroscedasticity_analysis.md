# 湖泊月面积时间序列条件异方差分析算法文档

**适用场景：** 多年逐月卫星遥感湖泊面积序列（典型如 23 年 × 12 月 = 276 个观测值），目标是识别季节性异方差、刻画条件波动结构，并实现跨月份可比的异常程度量化。

---

## 符号约定

| 符号 | 含义 |
|------|------|
| \( y_t \) | 原始月面积序列，\( t = 1, 2, \dots, N \)，\( N = 12 \times Y \)（Y 为年数） |
| \( m(t) \in \{1,\dots,12\} \) | \( t \) 时刻对应的月份 |
| \( \tilde{y}_t = \ln y_t \) | 对数变换后的序列 |
| \( T_t, S_t, R_t \) | STL 分解的趋势、季节、残差项 |
| \( \sigma_t^2 \) | 条件方差（ARCH/GARCH 估计） |
| \( z_t \) | 最终标准化异常指数 |

---

## 第一步：诊断季节性异方差

### 1.1 对数变换

湖泊面积的变动具有**乘性结构**（丰水期面积大且绝对波动大），取对数可将其转化为近似加性结构，同时稳定方差量级：

\[
\tilde{y}_t = \ln(y_t)
\]

取对数后，方差的季节性差异会显著缩小，但通常不能完全消除。

### 1.2 按月计算历史统计量

将序列按月份分组，对每个月 \( k = 1, \dots, 12 \) 计算历史均值和标准差：

\[
\bar{\tilde{y}}_k = \frac{1}{Y} \sum_{i=1}^{Y} \tilde{y}_{(i-1) \times 12 + k}
\]

\[
\hat\sigma_k^{\text{raw}} = \sqrt{\frac{1}{Y-1} \sum_{i=1}^{Y} \left(\tilde{y}_{(i-1) \times 12 + k} - \bar{\tilde{y}}_k\right)^2}
\]

### 1.3 异方差判断准则

计算各月标准差的极差比（ratio）：

\[
\text{VR} = \frac{\max_k \hat\sigma_k^{\text{raw}}}{\min_k \hat\sigma_k^{\text{raw}}}
\]

- \( \text{VR} > 2 \)：存在明显季节性异方差，后续分析必须考虑；
- \( \text{VR} > 5 \)：强异方差，直接使用全局标准差做标准化将严重失真。

可视化建议：绘制 12 个月的箱线图（box-whisker plot）和标准差柱状图，直观确认异方差形态。

---

## 第二步：STL 分解提取残差

### 2.1 STL 模型

STL（Seasonal-Trend decomposition using LOESS）将对数序列分解为三个加性成分：

\[
\tilde{y}_t = T_t + S_t + R_t
\]

其中：
- **趋势项** \( T_t \)：由 LOESS 局部加权回归估计，能自适应跟踪序列的缓慢长期变化（如气候驱动的多年扩张或萎缩）；
- **季节项** \( S_t \)：捕捉逐月的系统性周期模式，可允许季节幅度随年份缓慢漂移；
- **残差项** \( R_t \)：去除趋势和季节后剩余的"不可解释成分"，是后续分析的核心对象。

### 2.2 算法核心：LOESS 局部回归

LOESS 回归在每个时间点 \( t \) 利用其邻域内的数据加权拟合局部多项式（通常为 1 次或 2 次），权重函数为三次方核函数：

\[
w_i = \left(1 - \left|\frac{t_i - t}{h}\right|^3\right)^3, \quad |t_i - t| < h
\]

其中 \( h \) 为带宽（由 `trend` 窗口参数控制）。

### 2.3 关键参数设置

| 参数 | 含义 | 建议值（月度数据） |
|------|------|-----------------|
| `period` | 季节周期长度 | 12 |
| `seasonal` | 季节项 LOESS 窗口（奇数） | 7–13（越大越固定） |
| `trend` | 趋势项 LOESS 窗口（奇数） | ≥ 13，通常 23–37 |
| `robust` | 是否对离群值降权迭代 | `True`（推荐，防极端年份污染基线） |

### 2.4 与"直接减同月均值"的对比

直接减同月历史均值等价于：

\[
R_t^{\text{naive}} = \tilde{y}_t - \bar{\tilde{y}}_{m(t)}
\]

其隐含假设是：(1) 序列无长期趋势；(2) 季节幅度每年完全相同。若这两个假设不成立（23 年中多数湖泊面积存在可见趋势），则 \( R_t^{\text{naive}} \) 中会系统性残留趋势信号，前期值偏低、后期偏高，产生"伪异常"。

STL 残差 \( R_t \) 通过自适应趋势估计避免了这个问题，是更可靠的残差来源。

---

## 第三步：ARCH 效应检验与 GARCH 建模

### 3.1 ARCH-LM 检验

目的：验证 STL 残差 \( R_t \) 是否存在"条件异方差/波动聚集"效应，即残差的方差是否依赖于历史残差的大小。

**步骤：**

1. 计算残差平方序列：\( u_t = R_t^2 \)

2. 对 \( u_t \) 拟合辅助 OLS 回归（阶数 \( q \)，通常取 \( q = 4 \) 或 \( 12 \)）：

\[
u_t = a_0 + a_1 u_{t-1} + \dots + a_q u_{t-q} + \nu_t
\]

3. 计算 LM 统计量：\( \text{LM} = N \cdot R^2 \sim \chi^2(q) \)，其中 \( R^2 \) 为辅助回归的决定系数；

4. 判断：p 值 < 0.05 则拒绝原假设（无 ARCH 效应），认为存在条件异方差。

### 3.2 GARCH(1,1) 模型

ARCH/GARCH 的核心思想是给"方差"本身建立一个时间序列方程。实践中，GARCH(1,1) 通常已足够描述水文序列的波动聚集结构：

**误差分解：**

\[
R_t = \sigma_t \cdot z_t, \quad z_t \overset{\text{i.i.d.}}{\sim} D(0, 1)
\]

**方差方程（GARCH(1,1)）：**

\[
\sigma_t^2 = \omega + \alpha \cdot R_{t-1}^2 + \beta \cdot \sigma_{t-1}^2
\]

| 参数 | 含义 | 约束 |
|------|------|------|
| \( \omega > 0 \) | 长期基础波动水平 | 必须为正 |
| \( \alpha \geq 0 \) | ARCH 项：对历史冲击的即时响应 | 非负 |
| \( \beta \geq 0 \) | GARCH 项：历史方差的持续性 | 非负 |
| \( \alpha + \beta < 1 \) | 平稳性条件 | 必须满足 |

**长期无条件方差：**

\[
\sigma_\infty^2 = \frac{\omega}{1 - \alpha - \beta}
\]

\( \alpha + \beta \) 越接近 1，说明波动持续性越强——一次大扰动对方差的影响消退得越慢。

**误差分布选择：**

- 若标准化残差 \( \hat{z}_t = R_t / \hat\sigma_t \) 的峰度明显大于 3，建议将 \( z_t \) 假设为**学生 t 分布**（而非正态），以更好匹配厚尾特性。

### 3.3 参数估计

ARCH/GARCH 参数通过**极大似然估计（MLE）** 求解。以正态 \( z_t \) 为例，对数似然函数为：

\[
\ell(\theta) = -\frac{1}{2} \sum_{t=1}^{N} \left[\ln(2\pi) + \ln(\sigma_t^2) + \frac{R_t^2}{\sigma_t^2}\right]
\]

使用数值优化（如 BFGS）最大化 \( \ell(\theta) \)，得到参数估计 \( \hat\omega, \hat\alpha, \hat\beta \)，以及每时刻的条件标准差序列 \( \hat\sigma_t \)。

### 3.4 诊断：模型充分性检验

拟合 GARCH(1,1) 后，对标准化残差 \( \hat{z}_t = R_t / \hat\sigma_t \) 做以下检验：

1. **Ljung-Box 检验**：检验 \( \hat{z}_t \) 的自相关，p 值应 > 0.05（均值方程充分）；
2. **再次 ARCH-LM**：检验 \( \hat{z}_t^2 \) 的自相关，p 值应 > 0.05（方差方程充分）；
3. **正态 / t 分布 QQ 图**：检验分布假设是否合理。

若以上检验仍显著，需增加 GARCH 阶数或更换误差分布。

---

## 第四步：跨月份可比的异常指数

### 4.1 方法选择原则

GARCH 拟合得到的 \( \hat\sigma_t \) 是"当前时刻的条件波动状态"，适合描述波动的动态演化，但**不适合直接作为跨月比较的标准化基准**（因为它会随极端事件实时波动，含义不稳定）。跨月比较应以**历史同月统计量**为基准。

### 4.2 方法一：同月残差标准化（参数方法）

基于 STL 残差，按月计算历史统计量，再标准化：

\[
\bar{R}_k = \frac{1}{Y} \sum_{i=1}^{Y} R_{(i-1)\times12+k}, \quad k = 1,\dots,12
\]

\[
s_k = \sqrt{\frac{1}{Y-1} \sum_{i=1}^{Y} \left(R_{(i-1)\times12+k} - \bar{R}_k\right)^2}
\]

\[
z_t = \frac{R_t - \bar{R}_{m(t)}}{s_{m(t)}}
\]

此时 \( z_t \) 的含义是：**当前月的残差偏离历史同月残差均值多少个同月标准差**，不同月份之间可直接比较大小。

若 STL 残差具有零均值性质（理论上 \( \bar{R}_k \approx 0 \)），可简化为：

\[
z_t = \frac{R_t}{s_{m(t)}}
\]

### 4.3 方法二：同月非参数百分位数（鲁棒方法，推荐）

对每个月 \( k \) 的历史残差序列 \( \{R_{k,1}, \dots, R_{k,Y}\} \) 做升序排列，计算当前值 \( R_t \) 的经验百分位秩：

\[
p_t = \frac{\#\left\{R_{k,i} \leq R_t\right\}}{Y + 1} \times 100
\%
\]

- 不依赖任何分布假设，对厚尾和离群值完全鲁棒；
- \( p_t > 90 \) 表示面积异常偏高，\( p_t < 10 \) 表示异常偏低；
- 美国 USGS 河流流量的实时异常监测即采用此方法。

### 4.4 GARCH 条件方差的辅助作用

GARCH 拟合得到的 \( \hat\sigma_t \) 作为**辅助解释变量**，与 \( z_t \) 或百分位数结合使用：

- 高异常值（\( |z_t| \) 大）+ 高 \( \hat\sigma_t \)：异常可能是"高波动期的极端事件"；
- 高异常值 + 低 \( \hat\sigma_t \)：异常更显著，是"平静背景下的突发偏离"，更值得关注；
- 这种组合可以构造"条件异常显著性"指标，区分不同背景噪声水平下的异常事件。

---

## 完整流程总结

```
原始月面积序列  y_t
        |
        ▼
  [Step 1]  对数变换  ỹ_t = ln(y_t)
            按月计算标准差 σ̂_k
            计算极差比 VR → 确认季节性异方差
        |
        ▼
  [Step 2]  STL 分解（robust=True, period=12）
            ỹ_t = T_t + S_t + R_t
            得到自适应残差序列 R_t
        |
        ▼
  [Step 3]  ARCH-LM 检验 R_t
            ├─ 显著 → 拟合 GARCH(1,1)，得 σ̂_t
            └─ 不显著 → 直接进入 Step 4
        |
        ▼
  [Step 4]  按月标准化（参数法 z_t 或非参数百分位数 p_t）
            可结合 GARCH σ̂_t 构造"条件异常显著性"
            → 跨月份可比的异常指数
```

---

## 实现参考（Python）

```python
import numpy as np
import pandas as pd
from statsmodels.tsa.stl._stl import STL
from statsmodels.stats.diagnostic import het_arch
from arch import arch_model

# Step 1: 对数变换 + 季节性异方差诊断
log_area = np.log(area_series)
monthly_std = log_area.groupby(log_area.index.month).std()
VR = monthly_std.max() / monthly_std.min()
print(f"季节方差极差比 VR = {VR:.2f}")

# Step 2: STL 分解
stl = STL(log_area, period=12, seasonal=11, robust=True)
result = stl.fit()
R = result.resid  # 残差序列

# Step 3: ARCH-LM 检验
lm_stat, lm_pval, f_stat, f_pval = het_arch(R, nlags=12)
print(f"ARCH-LM p值 = {lm_pval:.4f}")

if lm_pval < 0.05:
    am = arch_model(R, vol='GARCH', p=1, q=1, dist='t')
    res = am.fit(disp='off')
    cond_vol = res.conditional_volatility  # σ̂_t

# Step 4: 跨月标准化
monthly_res_std = R.groupby(R.index.month).std()
z_score = R / R.groupby(R.index.month).transform('std')
```

---

*文档版本：1.0 | 适用数据：月度卫星遥感湖泊面积时间序列（≥10年）*
