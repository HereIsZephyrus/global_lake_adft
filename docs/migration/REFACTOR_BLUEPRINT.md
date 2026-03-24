# `global_lake_adft` Monorepo 重构蓝图

## 1. 背景

当前工作区下存在两个明显协作的 Python 项目：

- `hydrofetch`：负责 GEE 导出、Drive 下载、本地采样与结果写出。
- `lakeanalysis`：负责湖泊分析、统计建模与下游消费。

目前两者是独立目录、独立 GitHub 仓库、独立锁文件与独立开发环境。虽然职责边界清晰，但它们在数据生产与消费链路上存在长期协作关系，适合迁移为：

- 一个 Git 仓库
- 一个 workspace
- 两个继续独立安装和演进的 Python package

本蓝图目标是将当前目录整体重构为新的 monorepo 仓库：`global_lake_adft`。

## 2. 重构目标

### 2.1 总体目标

建立一个以 `global_lake_adft` 为根目录和根仓库的 monorepo，统一管理：

- `hydrofetch`
- `lakeanalysis`
- 根级开发工具链
- 根级锁文件
- CI / 文档 / 迁移说明

### 2.2 非目标

本次重构不应默认进行以下动作：

- 不强制合并为单一 Python 包
- 不强制重命名 `hydrofetch` 或 `lakeanalysis` 的 import 名
- 不强制让 `lakeanalysis` 立即以 Python 依赖方式引用 `hydrofetch`
- 不强制统一发布版本号

## 3. 目标目录结构

建议的 monorepo 目录结构如下：

```text
global_lake_adft/
├── pyproject.toml
├── uv.lock
├── README.md
├── .gitignore
├── .python-version
├── .github/
│   └── workflows/
├── docs/
│   ├── migration/
│   └── architecture/
├── packages/
│   ├── hydrofetch/
│   │   ├── pyproject.toml
│   │   ├── README.md
│   │   ├── .env.example
│   │   ├── src/hydrofetch/
│   │   ├── tests/
│   │   └── scripts/
│   └── lakeanalysis/
│       ├── pyproject.toml
│       ├── README.md
│       ├── .env.example
│       ├── src/lakeanalysis/
│       ├── tests/
│       └── scripts/
└── scripts/
```

说明：

- 根目录名使用 `global_lake_adft`，表示仓库和工作区。
- Python 包名保持为 `hydrofetch` 与 `lakeanalysis`，避免破坏现有接口。
- 原先分散在子仓库内的公共文档与迁移说明，逐步沉淀到根级 `docs/`。

## 4. 设计原则

1. 仓库统一，包边界保持独立。
2. 优先保留两个旧仓库的 Git 历史。
3. 优先使用根级 workspace 管理开发环境和锁文件。
4. 先统一仓库与工具链，再决定是否增加包间 Python 依赖。
5. 优先保持现有 CLI、import 路径和环境变量命名稳定。

## 5. 推荐技术方案

### 5.1 仓库与工作区

- 新建 GitHub 仓库：`global_lake_adft`
- 使用 `uv workspace` 作为 Python 工作区管理方案
- 根目录维护统一 `uv.lock`

### 5.2 包组织

- `packages/hydrofetch`
- `packages/lakeanalysis`

### 5.3 依赖策略

初始阶段建议：

- `hydrofetch` 与 `lakeanalysis` 继续保持各自依赖定义
- 不立即建立 Python 级强依赖
- 若后续 `lakeanalysis` 需要直接调用 `hydrofetch` 内部 API，再通过 workspace source 建立本地依赖

### 5.4 Python 版本策略

当前状态：

- `hydrofetch`：`>=3.12`
- `lakeanalysis`：`>=3.13`

建议在迁移中尽快统一为单一主开发版本。优先建议：

- 若 `lakeanalysis` 不依赖 3.13 特性，则统一到 `>=3.12`
- 若确认需要 3.13，则整体统一到 `>=3.13`

在 monorepo 中长期维持两套 Python 主版本会显著增加 CI、锁文件与本地开发复杂度，应尽量避免。

## 6. 根级配置建议

根级 `pyproject.toml` 建议以 workspace 为核心，而不是把根目录做成一个业务包。

示例草案：

```toml
[tool.uv.workspace]
members = [
    "packages/hydrofetch",
    "packages/lakeanalysis",
]

[dependency-groups]
dev = [
    "pytest>=8.0",
    "pytest-cov>=5.0",
    "pylint>=4.0",
]
```

后续可根据需要增加：

- 根级 lint/test 命令封装
- 公共 dev tools
- 文档构建依赖

## 7. Git 历史迁移方案

### 7.1 推荐方案：`git subtree`

推荐原因：

- 能保留两个原仓库的提交历史
- 合并路径清晰
- 不需要复杂的历史重写

建议流程：

1. 新建空仓库 `global_lake_adft`
2. 初始化本地仓库
3. 将两个旧仓库添加为 remote
4. 分别导入到 `packages/hydrofetch` 和 `packages/lakeanalysis`

示意命令：

```bash
git clone <global_lake_adft_repo_url>
cd global_lake_adft

git remote add hydrofetch-origin <hydrofetch_repo_url>
git fetch hydrofetch-origin
git subtree add --prefix=packages/hydrofetch hydrofetch-origin main

git remote add lakeanalysis-origin <lakeanalysis_repo_url>
git fetch lakeanalysis-origin
git subtree add --prefix=packages/lakeanalysis lakeanalysis-origin main
```

### 7.2 备选方案：直接复制文件

仅在以下场景考虑：

- 旧历史不重要
- 需要极快完成仓库整合

缺点：

- 丢失 Git 历史
- 后续 blame 和追踪上下文困难

除非有明确理由，不建议采用。

## 8. 分阶段实施计划

### 阶段 0：准备

目标：

- 确认迁移窗口
- 冻结高风险结构改动
- 明确仓库命名与默认分支策略

任务：

1. 创建新 GitHub 仓库 `global_lake_adft`
2. 确认默认分支名，如 `main`
3. 盘点两个旧仓库的以下内容：
   - GitHub Actions
   - README
   - issue / wiki / release
   - secrets 与 CI 环境变量
4. 决定旧仓库迁移后是归档还是只读保留

输出：

- 新仓库创建完成
- 迁移窗口和责任人明确

### 阶段 1：导入两个旧仓库

目标：

- 在新仓库中保留历史导入两个项目

任务：

1. 使用 `git subtree` 导入 `hydrofetch`
2. 使用 `git subtree` 导入 `lakeanalysis`
3. 确认目录落位到 `packages/`
4. 检查导入后的提交历史和文件结构

输出：

- `packages/hydrofetch`
- `packages/lakeanalysis`

### 阶段 2：建立根级 workspace

目标：

- 用单一 workspace 管理两个包

任务：

1. 新建根级 `pyproject.toml`
2. 配置 `tool.uv.workspace.members`
3. 生成根级 `uv.lock`
4. 评估并移除子目录中不再需要的独立锁文件
5. 统一根级 `.python-version`

输出：

- 根级 workspace 可正常解析
- 本地开发命令可在根目录运行

### 阶段 3：文档与路径修正

目标：

- 让所有文档都指向 monorepo 结构

任务：

1. 新建根级 `README.md`
2. 更新两个 package README 中的安装与运行说明
3. 修正文档中的相对路径引用
4. 修正仓库 URL、issue 链接与徽章
5. 增加迁移说明文档，说明旧仓库已迁至新仓库

输出：

- 用户可以根据根 README 完成开发环境初始化
- 所有关键文档路径有效

### 阶段 4：CI 与质量门禁整合

目标：

- 使用统一 CI 管理两个包

任务：

1. 在根目录建立 GitHub Actions
2. 将 lint/test 分为包级任务
3. 按路径变更触发对应 job
4. 视情况增加 monorepo 全局检查

建议最小 job 集：

- `hydrofetch-lint-test`
- `lakeanalysis-lint-test`
- `workspace-check`

输出：

- 提交到 monorepo 后能自动验证两个包

### 阶段 5：包关系与共享能力治理

目标：

- 明确两个包的长期协作边界

可选任务：

1. 判断 `lakeanalysis` 是否需要直接依赖 `hydrofetch`
2. 若需要，使用 workspace source 建立本地依赖
3. 提炼共享契约：
   - 数据目录结构
   - Parquet schema
   - 命名规则
   - 环境变量文档
4. 若存在共享工具代码，再评估是否新增第三个共享包

输出：

- 包间边界文档清晰
- 集成契约可测试

## 9. 文档重构建议

建议新增或调整以下文档：

- 根级 `README.md`
- 根级 `docs/migration/from-legacy-repos.md`
- 根级 `docs/architecture/repository-layout.md`
- 根级 `docs/architecture/package-boundaries.md`

建议 README 结构：

1. 项目简介
2. 仓库结构
3. package 职责说明
4. 快速开始
5. 常用开发命令
6. 迁移说明

## 10. 配置迁移建议

### 10.1 `.env.example`

保持各 package 自己的 `.env.example`：

- `packages/hydrofetch/.env.example`
- `packages/lakeanalysis/.env.example`

不建议立即合并成单一根级 `.env.example`，因为两者职责不同、环境变量命名空间也不同。

### 10.2 `.gitignore`

建议保留根级 `.gitignore` 统一忽略以下内容：

- `.venv/`
- `__pycache__/`
- `dist/`
- `build/`
- `.pytest_cache/`
- 日志、采样中间产物、临时下载文件

子 package 内若仍有特殊忽略项，可保留局部 `.gitignore`。

### 10.3 脚本目录

对于脚本文件，建议遵循以下原则：

- 与包强绑定的脚本保留在对应 package 内
- 跨包、仓库级、运维型脚本放到根级 `scripts/`

## 11. 风险与缓解措施

### 风险 1：历史迁移不完整

表现：

- blame 丢失
- 提交作者信息不完整

缓解：

- 优先使用 `git subtree`
- 导入后立刻检查 `git log -- packages/hydrofetch` 与 `git log -- packages/lakeanalysis`

### 风险 2：Python 版本不统一导致 workspace 复杂

表现：

- 锁文件冲突
- 本地环境难以复现
- CI 组合爆炸

缓解：

- 在迁移初期尽快决定统一版本基线

### 风险 3：文档路径失效

表现：

- README 中命令和相对路径错误

缓解：

- 将文档修正纳入独立阶段
- 对关键 README 进行人工 walkthrough

### 风险 4：旧仓库与新仓库并行造成混乱

表现：

- issue 开在旧仓库
- 提交发到错误仓库

缓解：

- 旧仓库 README 顶部写明迁移
- 仓库设置为 archived 或 restricted

### 风险 5：过早耦合两个包

表现：

- `lakeanalysis` 直接依赖过多 `hydrofetch` 内部模块

缓解：

- 先维持数据契约协作
- 只有在确实需要时，才引入 Python 级依赖

## 12. 验收标准

完成重构后，至少应满足以下条件：

1. 存在单一 Git 仓库 `global_lake_adft`
2. `hydrofetch` 与 `lakeanalysis` 都位于 `packages/` 下
3. 根目录存在统一 `pyproject.toml`
4. 根目录存在统一 `uv.lock`
5. 两个包都能在 workspace 内完成安装、lint、test
6. 根级 README 可指导开发者完成初始化
7. 旧仓库有明确迁移说明
8. 两个旧仓库的主要 Git 历史可在新仓库追溯

## 13. 推荐执行顺序

建议按以下顺序落地：

1. 创建 `global_lake_adft` 新仓库
2. 使用 `git subtree` 导入两个旧仓库历史
3. 将目录统一落位到 `packages/`
4. 新增根级 workspace 配置
5. 统一 Python 版本策略
6. 生成根级锁文件
7. 更新 README、文档、CI
8. 归档或只读化旧仓库

## 14. 后续可选优化

在 monorepo 稳定后，可以继续评估：

- 是否增加跨包集成测试
- 是否提炼共享 schema / utils / config 契约
- 是否建立统一 docs 站点
- 是否为两个 package 设计独立发布流水线

## 15. 结论

对于当前这两个“职责独立但协作紧密”的项目，最合适的重构方式不是把它们合成一个 Python 包，而是：

- 将仓库合并为 `global_lake_adft`
- 使用 workspace 统一开发环境
- 保留 `hydrofetch` 与 `lakeanalysis` 两个独立 package

这样既能降低跨仓协调成本，又不会破坏现有边界，是一条风险较低、收益较高的迁移路径。
