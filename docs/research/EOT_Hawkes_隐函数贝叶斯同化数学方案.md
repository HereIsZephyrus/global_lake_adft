# EOT 极值识别—Hawkes 急转识别—隐函数贝叶斯同化统一数学方案

## 1. 目标与总体框架

本文给出一条从月尺度湖泊面积序列出发、到旱涝急转识别与机理反演的统一数学链路：

1. 使用 EOT（Extremes Over Threshold）在非平稳背景下识别高尾/低尾极值事件；
2. 使用多元非对称 Hawkes 过程识别与解释旱涝急转（$D \rightarrow W, W \rightarrow D$）；
3. 构建以土壤水分、库容、径流为核心观测约束的隐函数状态空间模型；
4. 在贝叶斯同化框架中联合估计隐状态、过程参数与不确定度，实现“识别 + 归因 + 反演”的一体化。

记湖泊（或流域）索引为 $k$，离散月尺度时间索引为 $t=1,\dots,T$。

---

## 2. 观测、状态与符号

### 2.1 可观测量

- 湖泊面积（或水位代理）序列：$Y_t^{(k)}$；
- 气象强迫：$E_t^{(k)}$（降水、温度、VPD、再分析变量等）；
- 水文观测：$Z_t^{(k)} = [Z_{S,t}^{(k)}, Z_{R,t}^{(k)}, Z_{Q,t}^{(k)}]^T$，分别表示土壤水分、库容、径流（可扩展）。

### 2.2 隐状态

定义隐水文状态：
$$
H_t^{(k)} = [S_t^{(k)}, R_t^{(k)}, B_t^{(k)}, V_t^{(k)}]^T
$$

其中 $S_t$ 为土壤湿度快变量，$R_t$ 为库容状态，$B_t$ 为慢流/基流记忆项，$V_t$ 为植被或下垫面慢变量代理。

### 2.3 事件过程

- 干异常事件时刻集：$\mathcal{T}_D^{(k)}$；
- 湿异常事件时刻集：$\mathcal{T}_W^{(k)}$；
- 计数过程：$N_D^{(k)}(t),N_W^{(k)}(t)$。

---

## 3. 第一层：EOT 非平稳极值识别

## 3.1 连续时间与尾部统一

将月尺度时刻映射到连续时间 $t_c$（单位：年）：
$$
t_c = (year - year_0) + \frac{month-1}{12}
$$

高尾直接建模 $Y_t$；低尾通过 $\tilde{Y}_t=-Y_t$ 转为上尾问题。

### 3.2 非平稳阈值

采用分位数回归阈值 $u_q(t_c)$：
$$
u_q(t_c)=a_0+a_1t_c+\sum_{j=1}^{J}\left[b_j\sin(2\pi jt_c)+c_j\cos(2\pi jt_c)\right]
$$

通过 pinball 损失估计参数：
$$
\min_{a}\ \frac{1}{T}\sum_{t=1}^{T}\rho_q\!\left(Y_t-u_q(t_{c,t})\right),
\quad
\rho_q(r)=q\,r_+ + (1-q)(-r)_+
$$

### 3.3 去丛化与超阈值点

设去丛化后超阈值点为 $\{(t_j,x_j)\}_{j=1}^{n_u}$，其中 $x_j>u_q(t_j)$。  
对超阈值过程采用 NHPP 似然（对应现有 `eot` 模块）：
$$
\ell_{EOT}(\theta_E)=
-\int_0^{T_c}\Lambda_u(t;\theta_E)\,dt
+\sum_{j=1}^{n_u}\log \lambda_u(t_j,x_j;\theta_E)
$$

其中 $\theta_E$ 包含位置、尺度、形状与阈值相关参数。  
由此得到干/湿极值候选集合，并形成 $\mathcal{T}_D,\mathcal{T}_W$ 的先验候选时刻。

---

## 4. 第二层：Hawkes 急转识别与解释

### 4.1 多元非对称条件强度

在离散月尺度近似下，定义干/湿双变量 Hawkes 强度：
$$
\lambda_W(t)=\mu_W(t)+\sum_{\tau<t} \Phi_{WW}(t-\tau)\,dN_W(\tau)
+\sum_{\tau<t}\phi_{D\to W}(t-\tau;H_t)\,dN_D(\tau)
$$

$$
\lambda_D(t)=\mu_D(t)\Gamma(S_t)+\sum_{\tau<t}\Phi_{DD}(t-\tau)\,dN_D(\tau)
+\sum_{\tau<t}\phi_{W\to D}(t-\tau;H_t)\,dN_W(\tau)
$$

其中 $\Gamma(S_t)=\exp(-\eta S_t)$ 表示“湿态抑旱”缓冲。

### 4.2 状态门控互激核

定义门控特征向量：
$$
G_t=[\mathcal{V}_h(t),\mathcal{V}_e(t),\mathcal{M}_D(t),S_t,R_t]^T
$$

可取
$$
\mathcal{V}_h(t)=\frac{\partial Q_{NN}}{\partial P}(t),\quad
\mathcal{V}_e(t)=1-V_t,\quad
\mathcal{M}_D(t)=\sum_{i=1}^{L_m}\max(0,S^\star-S_{t-i})
$$

并定义互激强度：
$$
\alpha_{D\to W}(t)=\alpha_{0,DW}\left[1+\mathrm{Softplus}(w_{DW}^T G_t+b_{DW})\right]
$$
$$
\alpha_{W\to D}(t)=\alpha_{0,WD}\left[1+\mathrm{Softplus}(w_{WD}^T G_t+b_{WD})\right]
$$

若使用指数核：
$$
\phi_{D\to W}(\Delta;H_t)=\alpha_{D\to W}(t)\beta_{DW}e^{-\beta_{DW}\Delta},\ \Delta>0
$$
$$
\phi_{W\to D}(\Delta;H_t)=\alpha_{W\to D}(t)\beta_{WD}e^{-\beta_{WD}\Delta},\ \Delta>0
$$

### 4.3 Hawkes 似然

记 $m\in\{D,W\}$，则
$$
\ell_H(\theta_H,H_{1:T})=
\sum_{m}\left[
\sum_{t_i\in \mathcal{T}_m}\log \lambda_m(t_i)
-\int_0^{T_c}\lambda_m(u)\,du
\right]
$$

该层输出：

1. 急转风险强度序列（如 $D\to W$ 强度贡献）；
2. 外生项与内生记忆项分解；
3. 用于同化层反演的事件似然约束。

---

## 5. 第三层：隐函数状态空间与观测方程

核心思想：通过“隐状态动力学 + 多源观测”把不可直接观测的水文条件变化转化为可同化的后验推断问题。

### 5.1 状态转移（隐函数）

给定气象强迫 $E_t$，状态演化写作：
$$
H_t = f_\psi(H_{t-1},E_t) + \varepsilon_t,\quad
\varepsilon_t\sim \mathcal{N}(0,Q_t)
$$

其中 $f_\psi$ 可取：

- 机理-统计混合函数（质量守恒 + 可学习残差）；
- 神经网络隐函数（Neural ODE / RNN）；
- 高斯过程状态转移（小样本时更稳健）。

建议采用“半机理参数化”：
$$
\begin{aligned}
S_t &= S_{t-1} + g_P(P_t,H_{t-1}) - g_{ET}(E_t,H_{t-1}) - g_Q(H_{t-1}) + \epsilon_{S,t}\\
R_t &= R_{t-1} + q_{in}(P_t,H_{t-1}) - q_{out}(H_{t-1}) + \epsilon_{R,t}
\end{aligned}
$$
其余分量同理扩展。

### 5.2 观测方程

对土壤、库容、径流建立观测模型：
$$
Z_t = h_\varphi(H_t) + \nu_t,\quad \nu_t\sim \mathcal{N}(0,R_t^{obs})
$$

可分量化为：
$$
Z_{S,t}=h_S(H_t)+\nu_{S,t},\ 
Z_{R,t}=h_R(H_t)+\nu_{R,t},\ 
Z_{Q,t}=h_Q(H_t)+\nu_{Q,t}
$$

### 5.3 与 Hawkes 的耦合

状态通过门控进入 Hawkes 强度，形成闭环：
$$
H_t \Rightarrow G_t \Rightarrow \lambda_D(t),\lambda_W(t) \Rightarrow \text{事件概率}
$$

因此急转序列本身就是隐状态反演的附加观测。

---

## 6. 第四层：Copula 关联与风险融合（可选但推荐）

为刻画极端气象与水文异常共振，可在同化层并行引入条件 Copula：

设 $U_t$ 为 nLSWI 的 PIT 变量，$V_t$ 为极端降水或复合气象指标 PIT 变量，则
$$
F_{U,V|H}(u,v|H_t)=C_t(u,v;\theta_C(H_t))
$$

其中 $\theta_C(H_t)$ 可由线性映射或小网络给出。  
对应 Copula 对数似然：
$$
\ell_C(\theta_C,H_{1:T})=\sum_{t=1}^{T}\log c_t(u_t,v_t;\theta_C(H_t))
$$

风险融合可定义为
$$
S_t = w_1\tilde{\lambda}_{D\to W}(t)+w_2\tilde{J}_{D\to W}(t),
\quad
J_{D\to W}(t)=1-u_t-v_t+C_t(u_t,v_t)
$$

---

## 7. 统一贝叶斯同化模型

## 7.1 分层联合分布

给定参数集合
$$
\Theta=\{\theta_E,\theta_H,\psi,\varphi,\theta_C,Q,R^{obs}\},
$$
联合分布可写为
$$
\begin{aligned}
p(&H_{1:T},\Theta,\mathcal{T}_D,\mathcal{T}_W,Z_{1:T},Y_{1:T}\mid E_{1:T})\\
&=p(\Theta)\,p(H_1)\prod_{t=2}^{T}p(H_t\mid H_{t-1},E_t,\psi)\\
&\quad\times\prod_{t=1}^{T}p(Z_t\mid H_t,\varphi,R_t^{obs})
\times p(\mathcal{T}_D,\mathcal{T}_W\mid H_{1:T},\theta_H,\theta_E)\\
&\quad\times p(Y_{1:T}\mid \mathcal{T}_D,\mathcal{T}_W,\theta_E)
\times p(U_{1:T},V_{1:T}\mid H_{1:T},\theta_C).
\end{aligned}
$$

其中：

- $p(Y_{1:T}\mid\cdot)$ 对应 EOT/NHPP 层；
- $p(\mathcal{T}_D,\mathcal{T}_W\mid\cdot)$ 对应 Hawkes 层；
- $p(Z_t\mid H_t)$ 对应同化观测层；
- $p(U,V\mid H)$ 对应条件 Copula 层（可选）。

### 7.2 后验目标

核心目标是
$$
p(H_{1:T},\Theta\mid \text{all data})
\propto
\exp\left(\ell_{EOT}+\ell_H+\ell_C+\ell_Z\right)p(\Theta)p(H_1),
$$
其中
$$
\ell_Z=\sum_{t=1}^{T}\log p(Z_t\mid H_t).
$$

该后验直接给出“水文条件变化”的概率估计与不确定区间。

---

## 8. 推断算法建议

## 8.1 推荐路线 A：分阶段初始化 + 联合同化

1. **EOT 初始化**：拟合 $\theta_E$，提取 $\mathcal{T}_D,\mathcal{T}_W$ 初值；
2. **Hawkes 初始化**：在给定初始状态代理下拟合 $\theta_H$；
3. **状态同化**：固定或弱更新 $\theta_E,\theta_H$，用 PF/EnKF/UKF 推断 $H_{1:T}$；
4. **联合迭代**：EM 或 PMCMC 迭代更新 $\{H_{1:T},\Theta\}$ 直至收敛。

### 8.2 推荐路线 B：粒子 MCMC（全贝叶斯）

- 外层采样参数 $\Theta$；
- 内层粒子滤波估计状态边缘似然；
- 使用 PMMH 或 Particle Gibbs 采样后验。

优点是不确定度传播完整；代价是计算量较大。

---

## 9. 全链路不确定度分解与传播

设目标量为未来一个月急转风险 $R_{t+1}$，其后验方差可近似分解：
$$
\mathrm{Var}(R_{t+1}\mid \mathcal{D})
\approx
V_{obs}+V_{proc}+V_{param}+V_{struct}+V_{scenario}
$$

其中：

- $V_{obs}$：观测误差（$Z_t,Y_t,E_t$）；
- $V_{proc}$：过程噪声（状态转移误差）；
- $V_{param}$：参数后验不确定度；
- $V_{struct}$：模型结构选择不确定度（核函数、Copula 家族、状态方程型式）；
- $V_{scenario}$：外部情景输入不确定度。

### 9.1 计算策略

可采用双层蒙特卡罗：

1. 从参数后验采样 $\Theta^{(b)}$；
2. 在每个 $\Theta^{(b)}$ 下采样状态路径 $H_{1:T}^{(s)}$ 与未来扰动；
3. 计算 $R_{t+1}^{(b,s)}$；
4. 对样本方差做方差分解（ANOVA/Sobol 风格）得到各来源贡献。

### 9.2 报告指标

建议固定输出以下不确定度指标：

1. 后验中位数与 50/80/95% 可信区间；
2. 极端上尾覆盖率（如 $PIT$ 与超越概率回测）；
3. 事件识别的可靠性图与 Brier 分解；
4. 各不确定度源贡献率条形图；
5. 参数漂移（滚动窗口后验）与跨湖泊层级收缩效果。

---

## 10. 与工程模块的建议对接

在现有仓库结构下，建议按下列边界落地：

1. `eot/`：保留并扩展 EOT/NHPP 与极值时刻输出；
2. `hawkes/`（新增或并入 `eot/estimation.py`）：实现双变量非对称 Hawkes 与门控核；
3. `assimilation/`（建议新增）：实现状态转移、观测方程、滤波与平滑；
4. `quality/`：接入不确定度分解、校准诊断和风险评估指标；
5. `scripts/`：新增端到端实验脚本（训练/反演/评估一体）。

---

## 11. 最终数学产物与可解释输出

该统一方案最终输出三类结果：

1. **识别结果**：极值事件、急转事件时刻与概率；
2. **状态反演结果**：$H_t$（土壤、库容、径流相关隐状态）的后验路径；
3. **归因结果**：外生气象驱动、内生记忆驱动、结构不确定度的贡献拆分。

因此，模型不仅回答“是否发生急转”，还能回答“为什么发生、由谁主导、结论有多不确定”。

