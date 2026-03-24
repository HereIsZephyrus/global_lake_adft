# Repository Layout

仓库采用以根级 workspace 为中心的目录布局：

```text
global_lake_adft/
├── docs/
│   ├── architecture/
│   ├── migration/
│   ├── research/
│   └── runbooks/
├── packages/
│   ├── hydrofetch/
│   └── lakeanalysis/
├── pyproject.toml
├── tests/
│   └── integration/
├── uv.lock
└── scripts/
```

## Layout Rules

- 根目录负责 workspace、锁文件、CI 和跨包文档，不承载业务包源码。
- `packages/hydrofetch/` 保存 GEE 导出、下载、采样、写出相关能力。
- `packages/lakeanalysis/` 保存分析建模、数据库读取与下游分析能力。
- `packages/<pkg>/docs/` 仅保存强绑定于单个包的说明文档。
- 与单个包强绑定的脚本保留在对应 package 下。
- 跨包或仓库级脚本放在根级 `scripts/`。
- 根级 `tests/integration/` 预留给跨包数据契约和端到端流程测试。

## Workspace Rules

- 所有开发命令优先从仓库根目录发起。
- 统一锁文件位于根目录 `uv.lock`。
- 根级 `.python-version` 作为主开发版本基线。
