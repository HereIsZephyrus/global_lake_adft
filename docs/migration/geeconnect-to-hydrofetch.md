# GEE 数据抓取迁移说明

> **本文档已归档。**  `geeconnect` 模块已退役，GEE 相关能力已迁移至独立项目
> [`hydrofetch`](../../packages/hydrofetch/README.md)。
>
> 以下为旧方案概述和新方案对照，供参考。

## 旧方案（已退役）

原 `lakeanalysis/geeconnect` 使用 `ee.batch.Export.table.toDrive` 对每个湖泊逐日
执行 `reduceRegions`，产出 CSV 表后由 `meto` 模块接管本地处理。

**问题：** 130 万湖泊 × 逐年导出约需 10 分钟/湖/年，输出近 1 TB CSV，
时空开销不可接受。

## 新方案（hydrofetch）

`hydrofetch` 改用 `Export.image.toDrive` 导出完整 ERA5-Land 栅格，下载到本地后
按湖泊质心坐标做点采样，跳过逐湖 `reduceRegions`。

完整状态机：`Hold → Export → Download → Cleanup → Sample → Write → Completed`

### 快速开始

```bash
cp packages/hydrofetch/.env.example packages/hydrofetch/.env

uv run --package hydrofetch hydrofetch era5 \
    --start 2020-01-01 \
    --end   2020-02-01 \
    --region        region.geojson \
    --geometry      lake_centroids.csv \
    --output-dir    ./results \
    --run
```

详细文档见 [`packages/hydrofetch/README.md`](../../packages/hydrofetch/README.md)。

## lakeanalysis 当前保留职责

重构后 `lakeanalysis` 不再拥有任何 GEE 逻辑，仅保留：

- `meto`：日→月聚合、湖泊月序列对齐、质量控制
- `dbconnect`：数据库读写
- `eot`、`hawkes`、`pfaf`、`entropy`：下游分析模块
