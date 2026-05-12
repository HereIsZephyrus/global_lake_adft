<img src="https://r2cdn.perplexity.ai/pplx-full-logo-primary-dark%402x.png" style="height:64px;margin-right:32px"/>

# 提供一份详细的pettitt python代码实现的数学算法说明

以下是 Pettitt 检验的完整数学推导与 Python 实现说明。

***

## 数学基础：非参数秩次检验

Pettitt 检验源自 **Wilcoxon 双样本秩次检验**的扩展 。核心思想是：若序列在时刻 $t^*$ 发生均值突变，则将序列以 $t^*$ 为界分为前后两段，两段的**秩次分布**会系统性偏离；U 统计量正是对这种偏离程度的度量。[^1]

***

## 算法逐步推导

### Step 1：计算全局秩次

对长度为 $n$ 的序列 $\{x_1, x_2, \ldots, x_n\}$，计算每个元素的秩次 $R_i$（从小到大排名）：[^2]

$$
R_i = \text{rank}(x_i), \quad R_i \in \{1, 2, \ldots, n\}
$$

使用全局秩次而非原始值，是 Pettitt 对任意分布鲁棒的根本原因——它不假设正态性、等方差性，只利用数值的相对大小关系 。[^3]

### Step 2：构造 U 统计量序列

对每个候选变点位置 $t \in \{1, 2, \ldots, n-1\}$，计算 ：[^2]

$$
U_{t,n} = 2\sum_{i=1}^{t} R_i - t(n+1)
$$

其直觉含义是：

- $\sum_{i=1}^{t} R_i$：前 $t$ 个点的秩次之和
- 期望值 $\mathbb{E}\left[\sum_{i=1}^{t} R_i\right] = \frac{t(n+1)}{2}$（若序列同分布）
- $U_{t,n} > 0$：前段值系统偏大（前涝后旱），$U_{t,n} < 0$：前段值系统偏小（前旱后涝）

**递推计算**可将复杂度从 $O(n^2)$ 降至 $O(n)$ ：[^3]

$$
U_{t,n} = U_{t-1,n} + 2R_t - (n+1)
$$

### Step 3：检验统计量 K

取 $|U_t|$ 的最大值作为检验统计量，最大值对应的时刻就是候选变点 [^3]：

$$
K_n = \max_{1 \le t \le n-1} |U_{t,n}|, \quad t^* = \arg\max_t |U_{t,n}|
$$

### Step 4：p 值计算

基于 Pettitt (1979) 原文的渐近近似公式 ：[^2]

$$
p \approx 2\exp\left(\frac{-6K_n^2}{n^3 + n^2}\right)
$$

该公式在 $n \ge 10$ 时近似效果良好；若 $n$ 较小，建议使用 bootstrap 重采样估计 p 值 。[^2]

***

## 完整 Python 实现

```python
import numpy as np
from scipy import stats

def rolling_smooth(x: np.ndarray, window: int = 12) -> np.ndarray:
    """
    12个月滑动均值平滑，两端用 NaN 填充。
    对季节性湖泊序列，建议在Pettitt检验前先平滑。
    """
    kernel = np.ones(window) / window
    smoothed = np.convolve(x, kernel, mode='valid')
    pad = window // 2
    # 前后各填充 pad 个 NaN，保持长度与原序列一致
    return np.concatenate([np.full(pad, np.nan), smoothed, np.full(pad, np.nan)])[:len(x)]


def pettitt_test(x: np.ndarray, preprocess: bool = True, smooth_window: int = 12):
    """
    Pettitt 突变检验（非参数秩次法）

    参数
    ----
    x             : 原始时间序列（1D numpy array）
    preprocess    : 是否先做滑动均值平滑（推荐True，消除季节性）
    smooth_window : 平滑窗口宽度（月度数据建议12）

    返回
    ----
    result : dict，包含以下字段
        cp        : 变点在原始序列中的索引
        year_frac : 若传入年份数组可计算，此处返回cp/n的比例
        K         : 检验统计量
        p_value   : 近似p值
        U         : 完整U统计量序列（与输入等长，两端NaN）
        before_mean : 变点前段均值（原始序列）
        after_mean  : 变点后段均值（原始序列）
        drop_ratio  : (before - after) / before，判断是否结构退化
    """
    x = np.asarray(x, dtype=float)
    n_orig = len(x)

    # ── Step 0: 平滑预处理 ───────────────────────────
    if preprocess:
        xs = rolling_smooth(x, smooth_window)
    else:
        xs = x.copy()

    # 去除 NaN（平滑端点）
    valid_mask = ~np.isnan(xs)
    xs_valid = xs[valid_mask]
    valid_idx = np.where(valid_mask)[^0]
    n = len(xs_valid)

    # ── Step 1: 全局秩次 ─────────────────────────────
    # scipy.rankdata 处理并列值（取平均秩次）
    R = stats.rankdata(xs_valid)          # shape: (n,)

    # ── Step 2: 递推计算 U 统计量 ────────────────────
    # U[^0] = 0（初始化），U[t] = U[t-1] + 2*R[t] - (n+1)
    U = np.zeros(n)
    for t in range(1, n):
        U[t] = U[t-1] + 2 * R[t] - (n + 1)
    # 等价向量化写法（更快）：
    # increments = 2 * R - (n + 1)
    # U = np.cumsum(increments)
    # U = np.concatenate([[^0], U[:-1]])  # U[^0]=0，对应"0个点之前"

    # ── Step 3: 检验统计量 K ─────────────────────────
    abs_U = np.abs(U)
    K = abs_U.max()
    cp_in_valid = abs_U.argmax()          # 在去NaN序列中的位置
    cp = valid_idx[cp_in_valid]           # 映射回原始序列索引

    # ── Step 4: p 值（Pettitt 1979 渐近公式） ────────
    p_value = min(1.0, 2.0 * np.exp(-6.0 * K**2 / (n**3 + n**2)))

    # ── Step 5: 变点前后统计 ─────────────────────────
    before_mean = x[:cp].mean() if cp > 0    else np.nan
    after_mean  = x[cp:].mean() if cp < n_orig else np.nan
    if not np.isnan(before_mean) and before_mean != 0:
        drop_ratio = (before_mean - after_mean) / abs(before_mean)
    else:
        drop_ratio = np.nan

    # ── 将 U 映射回原始长度，便于绘图 ────────────────
    U_full = np.full(n_orig, np.nan)
    U_full[valid_idx] = U

    return {
        "cp"          : int(cp),
        "K"           : float(K),
        "p_value"     : float(p_value),
        "U"           : U_full,          # 原始长度，NaN处为端点
        "before_mean" : float(before_mean),
        "after_mean"  : float(after_mean),
        "drop_ratio"  : float(drop_ratio),
        "significant" : p_value < 0.05,
    }


def classify_lake(result: dict,
                  drop_thresh: float = 0.30,
                  persist_months: int = 24,
                  n_total: int = None) -> str:
    """
    根据 Pettitt 结果自动分类湖泊。

    返回
    ----
    "structural_degradation" : 永久性结构退化（排除出旱涝急转分析）
    "candidate_abrupt_shift" : 有显著突变但可能是急转，进入下一步
    "no_change"              : 无显著突变
    """
    if not result["significant"]:
        return "no_change"

    cp = result["cp"]
    after_length = (n_total - cp) if n_total else None

    is_permanent_drop = (
        result["drop_ratio"] > drop_thresh and          # 下降幅度 > 30%
        (after_length is None or after_length > persist_months)  # 持续 > 2年
    )

    return "structural_degradation" if is_permanent_drop else "candidate_abrupt_shift"


# ──────────────────────────────────────────────────────
# 使用示例
# ──────────────────────────────────────────────────────
if __name__ == "__main__":
    np.random.seed(42)
    n = 300
    t = np.arange(n)

    # 构造含断崖下跌的湖泊序列
    phase1 = 1.85e6 + 3e4*np.sin(2*np.pi*t[:180]/12) + np.random.normal(0, 2e4, 180)
    trans  = np.linspace(1.85e6, 0.05e6, 24) + np.random.normal(0, 1e4, 24)
    phase3 = np.clip(2e4 + np.random.normal(0, 1e4, 96), -1e4, 1.5e5)
    signal = np.concatenate([phase1, trans, phase3])

    result = pettitt_test(signal, preprocess=True, smooth_window=12)
    label  = classify_lake(result, n_total=n)

    print(f"变点位置: 月份 {result['cp']} → 年份 {2000 + result['cp']/12:.2f}")
    print(f"K = {result['K']:.0f},  p = {result['p_value']:.2e}")
    print(f"变点前均值: {result['before_mean']/1e6:.3f} M km²")
    print(f"变点后均值: {result['after_mean']/1e6:.3f} M km²")
    print(f"下降幅度: {result['drop_ratio']*100:.1f}%")
    print(f"湖泊分类: {label}")
```


***

## 关键实现细节

### 向量化加速（大批量湖泊时推荐）

```python
# 替代 for 循环的纯向量化版本，速度提升 10~50x
increments = 2 * R - (n + 1)          # shape (n,)
U_vec = np.cumsum(increments)          # 累计求和 = 递推展开
# 注意：标准定义 U[^0]=0 表示"前0个点"，需做一位偏移
U_vec = np.concatenate([[^0], U_vec[:-1]])
```


### Bootstrap p 值（小样本或精确推断）

当 $n < 50$ 或需要精确 p 值时，渐近公式可能不准确，用置换检验代替 ：[^2]

```python
def pettitt_p_bootstrap(x, n_boot=5000):
    _, K_obs, _, _ = pettitt_test_core(x)
    count = 0
    for _ in range(n_boot):
        xp = np.random.permutation(x)   # 零假设：无变点，随机排列
        _, K_b, _, _ = pettitt_test_core(xp)
        if K_b >= K_obs:
            count += 1
    return count / n_boot
```


### 并列值处理

`scipy.stats.rankdata` 默认对并列值取**平均秩次**，这在湖泊面积序列中有实际意义——同期冻结导致大量面积值相同时，平均秩次能避免随机排列的偏差 。[^3]

***

## 算法复杂度总结

| 步骤 | 操作 | 复杂度 |
| :-- | :-- | :-- |
| 秩次计算 | 排序 | $O(n \log n)$ |
| U 统计量 | 累积求和 | $O(n)$ |
| K 统计量 | 取最大值 | $O(n)$ |
| p 值（渐近） | 公式计算 | $O(1)$ |
| p 值（Bootstrap） | 置换 × n_boot | $O(n \cdot n_{\text{boot}})$ |
| **总体** |  | **$O(n \log n)$** |

这使得 Pettitt 即便对数千个湖泊批量运行也非常高效，是你批量筛选流程中最合适的第一道过滤器 。[^3][^2]
<span style="display:none">[^10][^11][^4][^5][^6][^7][^8][^9]</span>

<div align="center">⁂</div>

[^1]: https://blog.csdn.net/gitblog_09765/article/details/143148677

[^2]: https://arxiv.org/pdf/2411.05233.pdf

[^3]: https://blog.csdn.net/weixin_29091445/article/details/158820188

[^4]: https://blog.51cto.com/u_15494922/5103666

[^5]: https://www.tipdm.org/u/cms/www/202206/2714151095cv.pdf

[^6]: https://blog.csdn.net/zzqingyun/article/details/152791688

[^7]: http://set.baidu.com/view/321550120f4e767f5acfa1c7aa00b52acfc79c0c.html

[^8]: https://blog.51cto.com/u_16213585/11835754

[^9]: https://github.com/arxivsub/arXivSub_daily_arxiv

[^10]: https://majianthu.github.io/ce-survey.pdf

[^11]: http://qxyhjkx.cnjournals.com/uploadfile/qxyhjkxcn/20231023/《气象与环境科学》论文索引（2014年第1期-2023年第5期）.doc

