# Repository Layout

当前仓库结构如下：

```text
global_lake_adft/
├── data/
├── docs/
│   ├── architecture/
│   └── research/
├── packages/
│   ├── lakeanalysis/
│   ├── lakesource/
│   └── lakeviz/
├── .github/workflows/
├── pyproject.toml
└── uv.lock
```

约定：

- `packages/lakeanalysis/`：算法、批处理框架与 CLI
- `packages/lakesource/`：后端 provider、schema、存储与配置
- `packages/lakeviz/`：可视化与地图聚合导出
- `docs/architecture/`：仓库结构与工程边界
- `docs/research/`：研究方法、算法与报告
- `data/`：本地数据与输出，不纳入版本控制
