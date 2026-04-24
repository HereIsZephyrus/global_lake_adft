# Water-Balance Data Downloads

本 feature 为下一步水量平衡分析准备原始数据下载与目录骨架，统一使用 `/mnt/warehouse` 作为本地原始数据根目录。

## 目标目录

- `/mnt/warehouse/ESACCI/`：已有 ESA CCI soil moisture 原始数据
- `/mnt/warehouse/GPCC/`：公开可下载的 GPCC 月降水数据
- `/mnt/warehouse/GLEAM/`：需凭证的 GLEAM 数据
- `/mnt/warehouse/GRACE/`：需 Earthdata 鉴权的 GRACE/GRACE-FO 数据

## 当前脚本

- `packages/lakeanalysis/scripts/setup_water_balance_data.py`：创建目录、报告磁盘容量，并在空间足够时下载公开或鉴权数据
- `packages/lakeanalysis/scripts/download_esacci_soil_moisture_ceda.sh`：已有 ESA CCI 下载脚本

## 免鉴权下载范围

当前脚本默认只下载 `GPCC Full Data Monthly v2022` 的 `2001-2010` 与 `2011-2020` 两个 `0.25°` 压缩文件，用于尽快搭建与 `ESACCI` 对齐的公开数据底座。

对应目标文件：

- `full_data_monthly_v2022_2001_2010_025.nc.gz`
- `full_data_monthly_v2022_2011_2020_025.nc.gz`

## 需要鉴权的数据

- `GLEAM`：需 `gleam.eu` 的 SFTP 凭证，脚本通过 `curl sftp://...` 下载
- `GRACE/GRACE-FO`：需 NASA Earthdata 登录，脚本通过 `curl + .netrc` 下载

脚本会预创建目录，但不会尝试绕过鉴权；凭证缺失时只报告 `missing` 字段并跳过下载。

## 鉴权配置

按项目规范，密钥不写入 YAML 或代码，统一放在 `packages/lakeanalysis/.env`。可从模板复制：

```bash
cp packages/lakeanalysis/.env.example packages/lakeanalysis/.env
```

然后填写：

```bash
EARTHDATA_USERNAME=
EARTHDATA_PASSWORD=
GRACE_URL=

GLEAM_SFTP_HOST=sftp.gleam.eu
GLEAM_SFTP_PORT=2225
GLEAM_SFTP_USERNAME=
GLEAM_SFTP_PASSWORD=
GLEAM_REMOTE_DIR=
GLEAM_REMOTE_FILES=
```

说明：

- `GRACE_URL` 默认已指向当前 JPL Mascon 单文件，可按需要覆盖
- `GLEAM_REMOTE_FILES` 使用英文逗号分隔多个远端文件名
- `GLEAM_REMOTE_DIR` 示例：`/data/v3.8a/monthly`

## 使用方式

从仓库根目录执行：

```bash
uv run --package lakeanalysis python packages/lakeanalysis/scripts/setup_water_balance_data.py
```

如需在空间足够时直接下载公开数据：

```bash
uv run --package lakeanalysis python packages/lakeanalysis/scripts/setup_water_balance_data.py --download-public
```

如需读取 `.env` 中的凭证并下载鉴权数据：

```bash
uv run --package lakeanalysis python packages/lakeanalysis/scripts/setup_water_balance_data.py --download-auth
```

同时下载公开和鉴权数据：

```bash
uv run --package lakeanalysis python packages/lakeanalysis/scripts/setup_water_balance_data.py --download-public --download-auth
```

## 容量建议

- GPCC 公开部分：约 `0.54 GiB`
- GRACE 单文件：约 `43 MiB`
- GLEAM 月尺度档案：按保守上限预留约 `2.05 GiB+`
- 建议为整个流程至少预留 `10 GiB` 以上缓冲空间
