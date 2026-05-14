## Packages

当前仓库的核心 Python 包包括：

- `lakeanalysis`：算法实现、批处理框架、CLI
- `lakesource`：数据后端访问、provider、schema、配置
- `lakeviz`：可视化与栅格聚合输出

## lakeanalysis

`lakeanalysis` 负责：

- 湖泊相关统计分析与建模
- 批处理调度与算法入口
- 面向算法层的研究脚本

### 模块结构

| 模块 | 职责 |
|------|------|
| `batch` | MPI 分布式批处理框架 |
| `quantile` | 月距平分位数极端事件识别 |
| `pwm_extreme` | PWM 极值阈值估计 |
| `eot` | 极值阈值检验 |

### 依赖关系

```
lakeanalysis
    └── lakesource (LakeProvider, SourceConfig)
```

`lakeanalysis.batch` 通过 `LakeProvider` 抽象访问数据，支持 PostgreSQL 和 Parquet 两种后端。

其中 batch 语义边界是：

- `lakesource.provider` 提供 backend-oriented 共享读写能力
- `lakeanalysis.batch.io` 在其上包装 `BatchReader` / `BatchWriter`
- `lakeanalysis.batch.engine` 定义批处理候选域、分片和调度

## lakesource

`lakesource` 负责：

- `SourceConfig` 与 backend 选择
- `LakeProvider` 抽象
- Postgres / Parquet provider 实现
- schema / store / SQL helpers

它不直接承载 batch 调度语义，但提供 batch 所需的底层数据访问能力。

## lakeviz

`lakeviz` 负责：

- 可视化页面与地图
- grid aggregation 结果消费
- 导出和展示逻辑

## 数据边界

仓库内 `data/` 目录用于本地输入、缓存和分析输出，不视为可发布源码的一部分。

- 代码只应依赖明确的数据契约、数据库表结构或脚本参数
- 本地运行目录与中间产物不应成为包级 API 的一部分

## 文档边界

- 面向方法和研究说明的内容放在 `docs/research/`
- 面向仓库结构和工程边界的内容放在 `docs/architecture/`
- 包级使用方式放在各 package 的 `README.md`
