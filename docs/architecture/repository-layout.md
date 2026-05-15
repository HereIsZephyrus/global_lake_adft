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
- `data/`：本地与 HPC 仓库内共享输入根，不纳入版本控制

运行约定：

- 本地与 HPC 都从仓库内 `data/` 读取共享输入
- HPC 仓库外只保留 `lsf/` 等运维脚本目录，不再维护独立 `lake_data/`
- 业务输出统一写入仓库内 `output/<filter>/...`
