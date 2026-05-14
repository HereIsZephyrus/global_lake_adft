## Hawkes Refactor Memo

### Goal

- 收口 `HawkesCalculator` 抽象
- 统一 Hawkes calculator 输出语义
- 明确 `return_level_rows` 属于 EVT 结果，不属于 Hawkes core
- 删除结果语义中的 `workflow_version`

### Result Model

#### HawkesCoreResult

只表达 Hawkes 核心输出：

- `summary`
- `lrt_rows`
- `transition_monthly_rows`

旧名 `RunHawkesPipelineResult` 直接改为 `HawkesCoreResult`。

#### HawkesResult

统一 batch calculator 的单湖输出：

- `core: HawkesCoreResult`
- `return_level_rows: list[dict]`
- `extra_rows_by_table: dict[str, list[dict]]`

语义划分：

- `core`：Hawkes 核心结果
- `return_level_rows`：EVT 共享结果
- `extra_rows_by_table`：路由特有副产品，例如 `pwm_hawkes_segments`

### Calculator Hierarchy

```text
Calculator
└── HawkesCalculator
    ├── EOTHawkesCalculator
    └── PWMHawkesCalculator
```

### Notes

- `route_summary_rows` 视为 PWM 路由诊断副产品，不进入统一主字段
- `workflow_version` 不再属于结果语义
- 底层存储 schema 即使仍保留 nullable `workflow_version` 列，上层也不再主动赋值
