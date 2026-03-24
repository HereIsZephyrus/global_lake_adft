#+ `lakeanalysis` 重构方案

## 1. 背景与目标

当前仓库中的 `lakeanalysis` 同时承担了两类职责：

1. GEE 侧数据连接、导出与脚本化工作流。
2. 本地表处理、数据库读写、EOT/Hawkes 与后续分析。

随着 `hydrofetch` 项目的建立，这两类职责应当被明确拆开：

- `hydrofetch`：负责 `Hold -> Export -> Download -> Cleanup -> Sample -> Write` 的完整生产流水线。
- `lakeanalysis`：负责消费本地已落盘或已入库的数据，保留月尺度对齐、质量控制、EOT/Hawkes、同化前后的下游分析能力。

因此，本次重构的目标不是简单搬文件，而是将 `lakeanalysis` 收缩为一个**不拥有 GEE、只保留本地分析能力**的规范化 Python 项目。

## 2. 当前状态判断

### 2.1 `hydrofetch` 已开始承接的新职责

`hydrofetch` 当前已经具备新架构的基础骨架：

- `hydrofetch.config`：采用 `HYDROFETCH_*` 环境变量命名空间。
- `hydrofetch.gee.client`：负责 `ee.Initialize()` 与任务状态查询。
- `hydrofetch.drive.client`：负责 Drive OAuth、文件发现、下载、删除。
- `hydrofetch.catalog.parser`：负责 image export catalog 解析。
- `hydrofetch.export.namer`：负责稳定的导出命名。
- `hydrofetch.export.image_export`：已实现 ERA5-Land daily image export。

这说明 GEE 相关能力已经不应继续由 `lakeanalysis` 演进承载。

### 2.2 `lakeanalysis` 当前混杂的问题

当前 `lakeanalysis` 存在以下结构问题：

- 顶层直接散落 `geeconnect`、`meto`、`dbconnect`、`eot`、`hawkes`、`scripts` 等模块，未采用 `src/` 布局。
- `geeconnect` 同时负责 GEE 初始化、catalog、导出命名、表提取，形成独立的 GEE SDK。
- `meto` 中混入了 `meto.regions` 这类依赖 `ee.Geometry` 的 GEE 辅助函数，导致本地模块边界不干净。
- `scripts/run_gee_*` 仍然把 `lakeanalysis` 作为 GEE 工作流入口。
- `pyproject.toml` 中仍保留 `earthengine-api` 依赖。

## 3. 重构后的职责边界

### 3.1 `hydrofetch` 应负责的内容

以下职责应迁移或继续沉淀在 `hydrofetch`：

- GEE 项目初始化与认证。
- Drive OAuth、文件查询、下载、删除。
- 导出任务命名、任务状态跟踪、可恢复 job store。
- ERA5 image export catalog。
- 本地 raster 采样。
- 采样后写入文件或数据库。
- 未来统一的 `Hold -> Export -> Download -> Cleanup -> Sample -> Write -> Completed` 状态机。

### 3.2 `lakeanalysis` 应保留的内容

`lakeanalysis` 重构后只保留以下本地分析职责：

- 数据库访问层：`dbconnect`
- 气象表处理层：纯本地 `meto`
- 月尺度索引与对齐
- `lake_area` 及其派生数据消费
- EOT / Hawkes / entropy / pfaf 等分析模块
- 与双频同化方案直接相关的本地模型与统计分析

## 4. `lakeanalysis` 需要出清的部分

### 4.1 整个 `geeconnect/`

以下目录应整体迁出或退役：

- `lakeanalysis/geeconnect/__init__.py`
- `lakeanalysis/geeconnect/client.py`
- `lakeanalysis/geeconnect/config.py`
- `lakeanalysis/geeconnect/extraction.py`
- `lakeanalysis/geeconnect/export_namer.py`
- `lakeanalysis/geeconnect/catalog/`

原因：

- 它们已经与 `hydrofetch.gee`、`hydrofetch.drive`、`hydrofetch.catalog`、`hydrofetch.export` 的目标重叠。
- `LakeRegionSeriesExtractor` 属于旧方案“GEE 逐湖逐日表导出”的核心实现，不应继续作为主生产链路存在。

### 4.2 GEE 相关脚本

以下脚本应退休、迁移或改成迁移提示入口：

- `lakeanalysis/scripts/run_gee_era5land_lakes.py`
- `lakeanalysis/scripts/run_gee_smoke.py`
- `lakeanalysis/scripts/run_gee_era5land_area_quality_benchmark.sh`

它们都属于旧的 GEE workflow 入口，未来应由 `hydrofetch` CLI 取代。

### 4.3 `meto.regions`

以下文件不应继续作为 `lakeanalysis.meto` 的组成部分暴露：

- `lakeanalysis/meto/regions.py`

以及其在 `lakeanalysis/meto/__init__.py` 中的 re-export：

- `geojson_dict_to_ee_geometry`
- `wkt_to_ee_geometry`

原因：

- 这两个函数本质上是在生成 `ee.Geometry`，属于 GEE 辅助逻辑，不属于本地 meteo 工具层。

### 4.4 文档与环境变量

以下内容应出清或改写：

- `docs/migration/geeconnect-to-hydrofetch.md`
- `lakeanalysis/.env.example` 中的 GEE 专属配置说明
- 所有以 `GEE_PROJECT`、`EARTHENGINE_PROJECT`、`GEE_CLIENT_SECRETS`、`DRIVE_FOLDER_NAME` 为中心的文档和使用说明

这些内容应迁移到 `hydrofetch` 文档与 `HYDROFETCH_*` 配置体系下。

### 4.5 依赖

在 GEE 相关 import 全部移除后，`lakeanalysis/pyproject.toml` 中应删除：

- `earthengine-api`

`shapely` 是否保留，需要根据重构后是否仍有本地几何处理判断；如果仅剩 GEE 几何转换用途，则也应移除。

## 5. `lakeanalysis` 应保留的部分

以下内容应明确保留，不纳入本次出清：

- `lakeanalysis/dbconnect/`
- `lakeanalysis/eot/`
- `lakeanalysis/hawkes/`
- `lakeanalysis/pfaf/`
- `lakeanalysis/scripts/run_eot_batch.py`
- `lakeanalysis/scripts/run_hawkes_batch.py`
- `lakeanalysis/scripts/run_entropy.py`

`meto` 中应保留的纯本地模块：

- `lakeanalysis/meto/align.py`
- `lakeanalysis/meto/daily_aggregate.py`
- `lakeanalysis/meto/preprocess.py`
- `lakeanalysis/meto/time.py`

这些模块只依赖本地表结构与时间索引，不依赖 GEE，应继续作为下游分析工具层存在。

## 6. `lakeanalysis` 的规范化目录建议

建议将 `lakeanalysis` 重构为标准 `src` 布局：

```text
packages/lakeanalysis/
  pyproject.toml
  README.md
  docs/
    refactor-plan.md
  src/
    lakeanalysis/
      __init__.py
      dbconnect/
      meto/
      eot/
      hawkes/
      pfaf/
      entropy/
      models/
  scripts/
    run_eot_batch.py
    run_hawkes_batch.py
    run_entropy.py
  tests/
    ...
```

### 6.1 重构原则

- 包内模块全部改为 `lakeanalysis.*` 绝对导入。
- 业务包放到 `src/lakeanalysis/` 下，不再直接散落于项目根目录。
- `scripts/` 仅保留调用分析模块的轻入口，不再承载 GEE 逻辑。
- 包内 `docs/` 只保留强绑定于 `lakeanalysis` 的文档。
- 研究方法、算法说明、分析流程文档上移到根级 `docs/research/`。

### 6.2 `meto` 的重构目标

重构后的 `meto` 只保留纯本地职责，建议形态如下：

- `meto.align`
- `meto.daily_aggregate`
- `meto.preprocess`
- `meto.time`

必要时可继续扩展：

- `meto.features`
- `meto.qc`
- `meto.join`

但不应再承担：

- GEE 几何转换
- GEE 导出表构造
- GEE 认证与配置

## 7. 迁移顺序建议

### 第一阶段：职责冻结

- 停止在 `lakeanalysis/geeconnect` 上继续增加功能。
- 停止新增任何 `run_gee_*` 脚本或文档。
- 所有新 GEE / Drive / export / sample / write 能力只落在 `hydrofetch`。

### 第二阶段：功能迁移

- 将 `geeconnect/catalog` 的可复用思路迁入 `hydrofetch.catalog`。
- 将命名逻辑迁入 `hydrofetch.export.namer`。
- 将 GEE export 工作流迁入 `hydrofetch.export` + `hydrofetch.jobs` + 状态机实现。
- 将本地 sample / write 流程补齐到 `hydrofetch`。

### 第三阶段：`lakeanalysis` 出清

- 删除或废弃 `geeconnect/`
- 删除 `run_gee_*`
- 删除 `meto.regions` 及相关 re-export
- 更新文档，改为引用 `hydrofetch`
- 从 `lakeanalysis` 移除 `earthengine-api`

### 第四阶段：`src` 化重构

- 新建 `src/lakeanalysis/`
- 将现有保留模块迁入包目录
- 修正脚本导入路径
- 增补最基础的 import smoke tests

## 8. 风险与注意事项

### 8.1 不要误删通用 DB 能力

例如 `fetch_lake_geometry_wkt_by_ids()` 虽然目前主要服务旧 GEE 工作流，但它本质上是数据库几何读取函数，不一定必须直接删除。应根据 `hydrofetch` 是否复用决定其最终归属。

### 8.2 不要在迁移过程中同时维护两套主工作流

一旦 `hydrofetch` 具备可用的 ERA5 image export + sample + write 闭环，`lakeanalysis` 中旧的 GEE workflow 应尽快退役，否则后续会出现：

- 文档分叉
- 配置分叉
- 任务命名分叉
- 依赖分叉

### 8.3 `lakeanalysis` 的最终定位

重构后的 `lakeanalysis` 不应再被理解为“气象抓取与分析一体项目”，而应被明确为：

**湖泊时序分析与极值/事件统计项目。**

而 `hydrofetch` 应被明确为：

**湖泊气象驱动抓取、导出、下载、采样与写入项目。**

## 9. 最终结论

本次重构应遵循以下总原则：

- `hydrofetch` 接管所有 GEE / Drive / export / sample / write 职责。
- `lakeanalysis` 出清 `geeconnect`、`run_gee_*`、`meto.regions` 和 `earthengine-api` 依赖。
- `lakeanalysis` 采用 `src` 布局标准化项目结构。
- `lakeanalysis` 保留纯本地 `meto`、数据库访问层和 EOT/Hawkes 等下游分析模块。

完成后，两个项目的关系应变为：

- `hydrofetch` 生产数据
- `lakeanalysis` 消费数据

这是后续继续推进 ERA5 高频驱动、月尺度观测同化与双频分析方案的最清晰工程边界。
