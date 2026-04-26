## lakeanalysis

`lakeanalysis` 是当前仓库唯一正式维护的代码包，负责：

- 湖泊相关统计分析与建模
- 数据库访问与批量查询
- 研究脚本与可视化产出

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

## 数据边界

仓库内 `data/` 目录用于本地输入、缓存和分析输出，不视为可发布源码的一部分。

- 代码只应依赖明确的数据契约、数据库表结构或脚本参数
- 本地运行目录与中间产物不应成为包级 API 的一部分

## 文档边界

- 面向方法和研究说明的内容放在 `docs/research/`
- 面向仓库结构和工程边界的内容放在 `docs/architecture/`
- 包级使用方式放在 `packages/lakeanalysis/README.md`