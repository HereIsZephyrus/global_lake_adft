# Integration Tests

本目录预留给跨包测试，不替代各 package 自己的单元测试目录。

## Recommended Coverage

- `hydrofetch` 输出结果是否满足 `lakeanalysis` 的消费契约
- Parquet schema、列名、日期字段与目录布局是否稳定
- 关键端到端流程的 smoke test

## Current Rule

- 包内测试继续保留在 `packages/<pkg>/tests/`
- 只有真正跨包的测试才放在这里
