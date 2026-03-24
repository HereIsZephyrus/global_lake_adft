# Package Boundaries

## hydrofetch

`hydrofetch` 是数据生产侧包，负责：

- Earth Engine 导出任务创建与轮询
- Google Drive 下载与清理
- 本地 raster 采样
- 结果写出到文件或数据库

`hydrofetch` 应尽量保持为清晰的生产流水线入口，不承载分析建模职责。

## lakeanalysis

`lakeanalysis` 是数据消费和分析侧包，负责：

- 湖泊时间序列分析
- 统计建模和诊断
- 数据库访问与分析辅助模块

`lakeanalysis` 应避免直接依赖 `hydrofetch` 内部实现细节；优先通过数据契约协作。

## Collaboration Rules

- 默认通过数据文件、数据库表、schema 与约定的目录结构协作。
- 只有在确实需要共享 Python API 时，才通过 workspace source 建立显式依赖。
- 若未来出现稳定共享代码，再评估新增第三个共享 package。
