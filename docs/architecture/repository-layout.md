# Repository Layout

当前仓库结构如下：

```text
global_lake_adft/
├── data/
├── docs/
│   ├── architecture/
│   └── research/
├── packages/
│   └── lakeanalysis/
├── .github/workflows/
├── pyproject.toml
└── uv.lock
```

约定：

- `packages/lakeanalysis/`：唯一正式维护的 Python 包
- `docs/architecture/`：仓库结构与工程边界
- `docs/research/`：研究方法、算法与报告
- `data/`：本地数据与输出，不纳入版本控制
