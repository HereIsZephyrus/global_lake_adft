# Research Notes

本目录集中保存跨包共享的研究型文档、算法说明、方法笔记和论文摘录。

## 算法对比

| 文档 | 说明 |
|------|------|
| `极端事件识别算法对比.md` | Quantile、PWM Extreme、EOT 三种算法对比 |

## 算法文档

| 文档 | 说明 |
|------|------|
| `EOT模块说明.md` | EOT 极值阈值检验模块概述 |
| `EOT模块算法说明.md` | EOT 算法数学推导 |
| `eot-report.md` | EOT 实验报告 |
| `hawkes-report.md` | Hawkes 过程实验报告 |
| `monthly-anomaly-transition-spec.md` | 月距平分位数极端事件识别规范（Quantile） |
| `monthly-transition-report.md` | 月距平急转分析报告 |
| `pwm模块说明.md` | PWM 极值估计模块概述 |
| `EntropyCalculation.md` | 熵计算方法说明 |
| `点过程视角下的极值阈值检验.md` | 点过程理论视角（EOT 理论基础） |
| `基于最小交叉熵原理和次序统计量的极端分位数估计.md` | 最小交叉熵方法（PWM Extreme 理论基础） |

## 数据文档

| 文档 | 说明 |
|------|------|
| `water-balance-data-downloads.md` | 水量平衡数据下载说明 |
| `双频同化方案.md` | 双频同化方案 |
| `pfaf模块说明.md` | Pfafstetter 编码模块 |

## 论文摘录

| 文档 | 说明 |
|------|------|
| `研究方法.md` | 研究方法综述 |
| `Introductiontostatisticalmodelingof extremevalues.md` | 极值统计建模导论 |
| `Global_dominance_of_seasonality_in_shaping_lake-surface-extent_dynamics_content.md` | 湖面季节性动态研究 |

## What Belongs Here

- EOT、Hawkes、entropy、pfaf 等方法说明
- 分析方案、同化方案与研究笔记
- 论文摘录、阅读记录和算法推导

## What Does Not Belong Here

- 仓库结构与包边界：放到 `docs/architecture/`
- 包级使用说明：放到 `packages/<pkg>/README.md`