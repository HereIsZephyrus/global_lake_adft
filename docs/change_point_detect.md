<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# 我已经彻底理解了，将上述提到的UDmax / WDmax（HAC标准误）和supF(1)和Seasonality Dominance Ratio等总结为一份算法文档

下面是一份可以直接写进代码/论文方法部分的算法文档，整合了 Seasonality Dominance Ratio、UDmax/WDmax（HAC 标准误）、supF(1) 和 supF(2|1)，面向你的湖泊面积批量筛选任务。

***

## 0 目标与总体思路

**目标**：在大批量湖泊月度面积序列中，自动区分三类湖泊：

1. `stable`：无明显结构断点
2. `degraded`：存在单向不可逆的结构退化（类似 170137）
3. `intermittent`：存在间歇性干涸/蓄水循环（类似 170009），应保留做旱涝急转分析

**核心工具**：

- Seasonality Dominance Ratio（SDR）用于判断是否需要去季节（可选）[^1][^2]
- Bai–Perron 框架下的 UDmax/WDmax + supF(1) + supF(2|1)，残差方差使用 HAC 估计以处理自相关 [^3][^4]

***

## 1 符号与输入

- 湖泊 $i$ 的月度面积序列：$\{A_t\}_{t=1}^T$，时间步长为 1 个月
- 对应时间序列：$\{t\}_{t=1}^T$，有年月信息
- 设最小段长为 $h$（例如 24 个月）

***

## 2 Seasonality Dominance Ratio（SDR）

### 2.1 定义

对面积序列 $A_t$ 做 STL 分解或月气候态分解：

$$
A_t = T_t + S_t + R_t
$$

其中 $T_t$ 为趋势项，$S_t$ 为季节项，$R_t$ 为残差。定义季节性主导比率：

$$
\text{SDR} = \frac{\mathrm{Var}(S_t)}{\mathrm{Var}(A_t)}
$$

### 2.2 使用原则

- 若 $\text{SDR} \le \theta_s$（建议 $\theta_s = 0.3$），说明季节性对总方差贡献较小，可**直接在原始序列上做断点检测**。
- 若 $\text{SDR} > \theta_s$，说明季节性主导变化，应**先去季节**后再做断点检测 。[^2][^1]


### 2.3 去季节方法（可选）

推荐方法：减去月气候态（保留趋势 + 年际变化）：

$$
\tilde{A}_t = A_t - \mu_{\text{month}(t)} + \bar{\mu}
$$

其中 $\mu_m$ 是历史上月份 $m$ 的平均面积，$\bar{\mu}$ 是全序列均值，用于保持量纲。

**后续检验统一在 $\tilde{A}_t$ 上进行。**

***

## 3 Bai–Perron 框架与 HAC 标准误

### 3.1 回归模型形式

采用分段均值模型（湖泊面积的结构断点对应均值跳变）：

$$
\tilde{A}_t = \mu_j + \varepsilon_t, \quad t = T_{j-1}+1,\ldots,T_j,\quad j=1,\ldots,m+1
$$

其中 $\mu_j$ 为第 $j$ 个结构段的均值， $T_0=0, T_{m+1}=T$。

如需控制线性趋势，可扩展为分段趋势模型：

$$
\tilde{A}_t = \mu_j + \beta_j t + \varepsilon_t
$$

后续统计量的形式不变，只是回归的参数维度 $q$ 增大 。[^5]

### 3.2 残差方差的 HAC 估计

为处理月度数据中的自相关与异方差，对每个候选断点组合 $\boldsymbol{\lambda}$ 进行 OLS 回归后，采用 Newey–West 等 HAC 估计残差协方差矩阵 $\hat{\Omega}_{\text{HAC}}$，构造 F 统计量的分母 。[^4]

***

## 4 UDmax / WDmax：是否存在断点

### 4.1 supF(k)

对给定断点个数 $k$，在所有满足最小段长约束的断点组合 $\boldsymbol{\lambda}_k$ 上，定义：

$$
\sup F(k) = \sup_{\boldsymbol{\lambda}_k} F(T; \boldsymbol{\lambda}_k)
$$

其中

$$
F(T; \boldsymbol{\lambda}_k)
= \frac{\big[S_T^{\text{restricted}} - S_T(\boldsymbol{\lambda}_k)\big]/(kq)}{\hat{\sigma}^2_{\text{HAC}}(\boldsymbol{\lambda}_k)}
$$

- $S_T^{\text{restricted}}$：无断点模型残差平方和
- $S_T(\boldsymbol{\lambda}_k)$：给定断点组合下分段 OLS 的残差平方和
- $q$：每段参数维度（均值模型时 $q=1$，趋势模型时 $q=2$）


### 4.2 UDmax 统计量

$$
\text{UDmax} = \max_{1 \le k \le M} \sup F(k)
$$

- 原假设 $H_0$：无结构断点
- 备择假设 $H_1$：至少存在一个断点
- 使用 Bai–Perron 给出的渐近临界值（如 5%：8.88）判断显著性 。[^3]

若 UDmax 不显著 → 判为 `stable`，不再继续。

### 4.3 WDmax 统计量

$$
\text{WDmax} = \max_{1 \le k \le M} w(k)\sup F(k)
$$

其中 $w(k)$ 选择使 $w(k)\sup F(k)$ 在不同 $k$ 下具有统一的临界值（消除对大 $k$ 的偏好）。5% 水平临界值约为 9.91 。[^3]

实践中：

- 通常同时报告 UDmax 和 WDmax
- 一般以 WDmax 为主判断是否有断点，UDmax 用作参考 。[^6]

***

## 5 supF(1) 与 supF(2|1)：定位与区分退化 vs 间歇

### 5.1 supF(1)：定位主断点

在确认存在至少一个断点后，对 $k=1$ 的情况遍历所有候选断点 $t$（满足最小段长约束），计算：

$$
F(t) = \frac{\big[S_T^{\text{restricted}} - S_T(t)\big]/q}{\hat{\sigma}^2_{\text{HAC}}(t)}
$$

取最大值：

$$
\hat{t}_1 = \arg\max_t F(t), \quad \sup F(1) = F(\hat{t}_1)
$$

若 $\sup F(1)$ 显著，则 $\hat{t}_1$ 为最优单断点估计 。[^3]

### 5.2 supF(2|1)：检验是否存在第二个断点

在已有 $\hat{t}_1$ 的前提下，在两个子区间 $[1,\hat{t}_1]$ 和 $[\hat{t}_1+1,T]$ 内分别做 supF(1)，构造条件 supF(2|1) 统计量 [^3]：

$$
\sup F(2 \mid 1) = \max\left\{\sup F_{\text{left}}(1), \sup F_{\text{right}}(1)\right\}
$$

- 若 $\sup F(2|1)$ 不显著 → 接受"恰好一个断点"，无进一步结构切分
- 若显著 → 接受存在第二个断点（适用于间歇性湖泊的干–湿–干结构）

***

## 6 断点后的湖泊类型判别规则

在你的湖泊筛选任务中，supF(1) 和 supF(2|1) 的使用建议如下：

1. **无断点**（UDmax/WDmax 不显著）
→ `stable`（正常湖泊，保留）
2. **仅一个断点且 supF(2|1) 不显著**
→ 进一步检查断点前后均值和极值：
    - $\mu_{\text{after}} / \mu_{\text{before}} < \theta_\mu$（如 0.2）
    - $\max_{\text{after}} / \mu_{\text{before}} < \theta_{\max}$（如 0.3）
→ 满足则标记为 `degraded`（结构退化，类似 170137，过滤）[^7]
3. **supF(2|1) 显著（或存在 ≥2 个断点）**
→ 序列存在多段均值切换，对应间歇性干–湿–干循环
→ 标记为 `intermittent`（类似 170009，保留）[^8]

***

## 7 总结式伪代码

```python
for lake in lakes:
    series = lake.area_series

    # ① 可选：计算 SDR 决定是否去季节
    sdr = compute_SDR(series)  # STL或月气候态
    if sdr > theta_s:
        series_deseason = remove_seasonality(series)
    else:
        series_deseason = series.copy()

    # ② UDmax/WDmax（HAC）检验是否存在断点
    udmax, ud_p = compute_UDmax(series_deseason, hac=True)
    wdmax, wd_p = compute_WDmax(series_deseason, hac=True)

    if wd_p > 0.05:  # 无断点
        lake.label = "stable"
        continue

    # ③ supF(1) 定位主断点
    t1, supF1, p1 = supF_1(series_deseason, hac=True)

    # ④ supF(2|1) 检验是否存在第二个断点
    supF2_cond, p2 = supF_2_given_1(series_deseason, t1, hac=True)

    if p2 < 0.05:
        lake.label = "intermittent"  # 多断点，干湿循环
    else:
        # 单断点 → 检查断点后段是否退化
        if is_degraded_after(series, t1):
            lake.label = "degraded"   # 类170137，过滤
        else:
            lake.label = "single_shift"  # 需要进一步人工核查
```

这份文档就是你整个湖泊结构断点识别模块的"算法规范版"：理论上贴合 Bai–Perron，工程上可直接实现，物理上与你对 170137/170009 的直觉完全一致。

<div align="center">⁂</div>

[^1]: https://www.chinawater.com.cn/zhsl/202506/t20250604_1065243.html

[^2]: https://www.tsinghua.edu.cn/info/1182/119404.htm

[^3]: https://bbs.pinggu.org/thread-2441660-1-1.html

[^4]: https://www.lianxh.cn/details/1460.html

[^5]: https://econ.sufe.edu.cn/_upload/article/files/5d/f9/e25fa98546548a498429c1ece780/d90e441f-6d45-4cc0-a544-d28314c1ef08.pdf

[^6]: https://landeconomics.nccu.edu.tw/upload/29/download_file/5123/06-02-3,%E6%88%BF%E5%83%B9%E7%B5%90%E6%A7%8B%E6%80%A7%E6%94%B9%E8%AE%8A%E4%B9%8B%E6%AA%A2%E6%B8%AC%E2%80%A6_%E6%A5%8A%E9%9B%85%E5%A9%B7,%E5%BD%AD%E5%BB%BA%E6%96%87.pdf

[^7]: lake_170137_chart.jpg

[^8]: lake_170009_chart.jpg

