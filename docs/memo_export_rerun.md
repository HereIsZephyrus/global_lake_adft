# 数据导出与算法重跑 Memo

> 日期: 2026-05-08（最新更新）
> 背景: H×CV filter 已更新 (pv_threshold=0.001, 移除 dominant_ratio_max), area_quality/area_anomalies 已重算完成, 需要重新导出 Parquet 并在 HPC 上全量重跑 Quantile / PWM / EOT
>
> **状态: 已完成导出和作业提交，等待运行完成**

---

## 1. 当前数据状态

### 本地 Parquet (data/parquet/)

| 表 | 行数 | 备注 |
|---|---|---|
| lake_info | 1,401,802 | 全量湖泊元数据 |
| lake_area | ~387M | 月度水面面积 (m²) |
| anomaly | ~545M | 冻结月标记 |
| area_quality | 1,079,740 | 非异常湖泊 (2026-05-08 重新导出) |
| area_anomalies | 322,062 | 异常湖泊 (PV flag: 81,258, 其中纯 PV-only: ~5,762) |
| area_entropy_cv | 1,400,189 | H×CV 探索结果（仅 DB，未导出 parquet） |
| quantile_run_status | 0 | 空，需全量重跑 |
| pwm_extreme_run_status | 0 | 已清空 |
| eot_run_status | 0 | 空，需全量重跑 |

### area_anomalies flag 分布

| Flag | 值 | 数量 |
|---|---|---|
| median_zero | 1 | 57,462 |
| flat | 2 | 164,899 |
| area_ratio | 4 | 81,499 |
| outside_range | 8 | 187,246 |
| pv (H×CV) | 16 | 81,258 |

### 子集: atlas_area > 10 km²（从 parquet 筛选）

| 表 | 行数 | 来源 |
|---|---|---|
| hylak_id 集合 | 16,385 | area_quality(14,670) ∪ area_anomalies(1,715) |
| lake_area | 4,522,260 | 月度数据 |
| lake_info | 16,385 | 元数据 |
| anomaly | 6,564,625 | 冻结月标记 |
| area_quality | 14,670 | 正常湖泊 |
| area_anomalies | 1,715 | 异常湖泊 |
| af_nearest | 2,060 | 最近湖泊 |

---

## 2. HPC 连接信息

| 项目 | 值 |
|---|---|
| 主机 | 111.172.12.146 |
| 端口 | 4351 |
| 用户 | guxh01 |
| 密码 | 不落盘，按会话临时输入 |
| 项目路径 | /data/users/guxh01/2026_tcb/lake/global_lake_adft |
| 全量数据路径 | /data/users/guxh01/2026_tcb/lake/lake_data |
| 子集数据路径 | /data/users/guxh01/2026_tcb/lake/lake_data_gt10 |
| Conda 环境 | tcb_lake |

---

## 3. 执行步骤

### Step 1: 导出 Parquet（已完成 ✅）

#### 1a. 全量导出

`data/parquet/` 中 lake_area, lake_info, anomaly, af_nearest 由之前的运行导出，本次仅更新：
- `area_quality/` — 8 个分块（1,079,740 行）
- `area_anomalies/` — 8 个分块（322,062 行）

#### 1b. 子集导出（parquet→parquet，零 DB 开销）

```bash
python packages/lakesource/scripts/export_subset_parquet.py
```

- 从 `data/parquet/area_quality` + `area_anomalies` 中筛选 `atlas_area > 10` 得到 16,385 个 hylak_id
- 对每张表从 `data/parquet/<table>/` 读取 parquet，DuckDB 下推 hylak_id IN (...) 过滤
- 输出单文件 parquet 到 `data/parquet_gt10/`
- 耗时: ~4 秒（无需扫描全表，DuckDB 下推过滤）

### Step 2: rsync 到 HPC（已完成 ✅）

```bash
# 仅更新 area_quality 和 area_anomalies
rsync -avz --progress -e "ssh -p 4351" data/parquet/area_quality/ guxh01@111.172.12.146:/data/users/guxh01/2026_tcb/lake/lake_data/area_quality/
rsync -avz --progress -e "ssh -p 4351" data/parquet/area_anomalies/ guxh01@111.172.12.146:/data/users/guxh01/2026_tcb/lake/lake_data/area_anomalies/

# 子集
rsync -avz --progress -e "ssh -p 4351" data/parquet_gt10/ guxh01@111.172.12.146:/data/users/guxh01/2026_tcb/lake/lake_data_gt10/

# LSF 脚本
rsync -avz --progress -e "ssh -p 4351" lsf/ guxh01@111.172.12.146:/data/users/guxh01/2026_tcb/lake/global_lake_adft/lsf/
```

### Step 3: 清理 HPC 旧算法输出（已完成 ✅）

```bash
ssh -p 4351 guxh01@111.172.12.146
rm -rf /data/users/guxh01/2026_tcb/lake/lake_data/comparison
rm -rf /data/users/guxh01/2026_tcb/lake/lake_data/comparison-full
rm -f /data/users/guxh01/2026_tcb/lake/lake_data/quantile_*.parquet
rm -f /data/users/guxh01/2026_tcb/lake/lake_data/pwm_extreme_*.parquet
rm -f /data/users/guxh01/2026_tcb/lake/lake_data/eot_*.parquet
rm -f /data/users/guxh01/2026_tcb/lake/lake_data/comparison_*.parquet
```

### Step 4: 提交 LSF 作业（已完成 ✅）

```bash
cd /data/users/guxh01/2026_tcb/lake/global_lake_adft/lsf
bsub < quantile_tcb.sh       # Job 53856
bsub < pwm_tcb.sh            # Job 53857
bsub < eot_tcb.sh            # Job 53858
bsub < quantile_tcb_gt10.sh  # Job 53859
bsub < pwm_tcb_gt10.sh       # Job 53860
bsub < eot_tcb_gt10.sh       # Job 53861
```

### Step 5: Comparison（Quantile + PWM 全量完成后）

```bash
bsub < comparison_tcb_v2.sh
```

- 输出: `data/parquet/`
- 表: lake_area, lake_info, anomaly, area_quality, area_anomalies, af_nearest
- `--refresh` 覆盖旧文件 (area_quality/area_anomalies 已更新)
- 预计耗时: 30-60 分钟 (lake_area 387M 行最大)

#### 1b. 子集导出 (lake_area > 10 km²)

```bash
python packages/lakesource/scripts/export_subset_parquet.py
```

- 输出: `data/parquet_gt10/`
- 默认 `--min-area 10.0` (km²)
- 从 lake_info 筛选 lake_area > 10 的 hylak_id 集合, 所有表按此集合过滤
- 预计耗时: 5-10 分钟 (子集约 14,777 湖泊)

### Step 2: rsync 到 HPC

```bash
# 全量
rsync -avz --progress -e "ssh -p 4351" data/parquet/ guxh01@111.172.12.146:/data/users/guxh01/2026_tcb/lake/lake_data/

# 子集
rsync -avz --progress -e "ssh -p 4351" data/parquet_gt10/ guxh01@111.172.12.146:/data/users/guxh01/2026_tcb/lake/lake_data_gt10/
```

### Step 3: 清理 HPC 旧算法输出

```bash
ssh -p 4351 guxh01@111.172.12.146

# 全量
rm -f /data/users/guxh01/2026_tcb/lake/lake_data/quantile_*.parquet
rm -f /data/users/guxh01/2026_tcb/lake/lake_data/pwm_extreme_*.parquet
rm -f /data/users/guxh01/2026_tcb/lake/lake_data/eot_*.parquet
rm -f /data/users/guxh01/2026_tcb/lake/lake_data/comparison_*.parquet

# 子集 (如果有旧文件)
rm -f /data/users/guxh01/2026_tcb/lake/lake_data_gt10/quantile_*.parquet
rm -f /data/users/guxh01/2026_tcb/lake/lake_data_gt10/pwm_extreme_*.parquet
rm -f /data/users/guxh01/2026_tcb/lake/lake_data_gt10/eot_*.parquet
rm -f /data/users/guxh01/2026_tcb/lake/lake_data_gt10/comparison_*.parquet
```

### Step 4: 提交 LSF 作业

#### 全量作业 (3 个并行)

```bash
cd /data/users/guxh01/2026_tcb/lake/global_lake_adft/lsf

bsub < quantile_tcb.sh       # Quantile: 128 workers, 72h walltime
bsub < pwm_tcb.sh            # PWM Extreme: 128 workers, 72h walltime
bsub < eot_tcb.sh            # EOT: 128 workers, 72h walltime
```

#### 子集作业 (3 个并行)

```bash
bsub < quantile_tcb_gt10.sh  # Quantile gt10: 32 workers, 4h walltime
bsub < pwm_tcb_gt10.sh       # PWM gt10: 32 workers, 8h walltime
bsub < eot_tcb_gt10.sh       # EOT gt10: 32 workers, 12h walltime
```

#### Comparison (Quantile + PWM 全量完成后)

```bash
bsub < comparison_tcb_v2.sh  # Comparison: 128 workers, 72h walltime
```

---

## 4. LSF 作业详情

### 全量作业

| Job ID | 脚本 | 算法 | Workers | RAM | Walltime | 命令 |
|---|---|---|---|---|---|---|
| 53856 | quantile_tcb.sh | Quantile | 128 | 256GB | 72h | `run_quantile.py --chunk-size 10000 --io-budget 127` |
| 53857 | pwm_tcb.sh | PWM Extreme | 128 | 256GB | 72h | `run_pwm_extreme.py --chunk-size 10000 --io-budget 127` |
| 53858 | eot_tcb.sh | EOT | 128 | 256GB | 72h | `run_eot.py --tail both --threshold-quantiles 0.95 0.98 --chunk-size 10000 --io-budget 127` |
| - | comparison_tcb_v2.sh | Comparison | 128 | 256GB | 72h | `run_algorithm_comparison.py --chunk-size 10000 --io-budget 127` |

### 子集作业 (lake_area > 10 km²)

| Job ID | 脚本 | 算法 | Workers | RAM | Walltime | PARQUET_DATA_DIR |
|---|---|---|---|---|---|---|
| 53859 | quantile_tcb_gt10.sh | Quantile | 32 | 64GB | 4h | lake_data_gt10 |
| 53860 | pwm_tcb_gt10.sh | PWM Extreme | 32 | 64GB | 8h | lake_data_gt10 |
| 53861 | eot_tcb_gt10.sh | EOT | 32 | 64GB | 12h | lake_data_gt10 |

---

## 5. 算法输出表

### Quantile (4 张表)

| 表 | 内容 |
|---|---|
| quantile_labels | 全量标记序列 (anomaly, q_low, q_high, extreme_label) |
| quantile_extremes | 极端事件 |
| quantile_abrupt_transitions | 急剧转变 (low→high, high→low) |
| quantile_run_status | 运行状态追踪 |

### PWM Extreme (2 张表)

| 表 | 内容 |
|---|---|
| pwm_extreme_thresholds | 逐月阈值 (high, low, converged, lambda 系数) |
| pwm_extreme_run_status | 运行状态追踪 |

### EOT (3 张表)

| 表 | 内容 |
|---|---|
| eot_results | 拟合参数 (beta0, beta1, sin_1, cos_1, sigma, xi, converged, log_likelihood) |
| eot_extremes | 极端事件 (cluster_id, year, month, water_area, threshold_at_event) |
| eot_run_status | 运行状态追踪 |

### Comparison (5 张表)

| 表 | 内容 |
|---|---|
| comparison_labels | 对比标记 |
| comparison_thresholds | 对比阈值 |
| comparison_extremes | 对比极端事件 |
| comparison_agreement | 一致性指标 |
| comparison_run_status | 运行状态追踪 |

---

## 6. 依赖关系

```
[Parquet 导出] → [rsync 到 HPC] → [清理旧输出]
                                        │
                    ┌───────────────────┼───────────────────┐
                    │                   │                   │
                    ▼                   ▼                   ▼
             Quantile (全量)    PWM Extreme (全量)    EOT (全量)
                    │                   │
                    └───────┬───────────┘
                            ▼
                    Comparison (全量)

                    ┌───────────────────┼───────────────────┐
                    │                   │                   │
                    ▼                   ▼                   ▼
             Quantile (gt10)    PWM Extreme (gt10)    EOT (gt10)
```

- Quantile, PWM, EOT 互相独立, 可并行
- Comparison 依赖 Quantile + PWM 完成
- 全量和子集互相独立, 可并行

---

## 7. 新建/修改的文件

| 文件 | 类型 | 说明 |
|---|---|---|
| packages/lakesource/scripts/export_subset_parquet.py | 新建 | 子集 Parquet 导出脚本 |
| lsf/eot_tcb.sh | 新建 | EOT 全量 LSF 脚本 |
| lsf/pwm_tcb.sh | 新建 | PWM Extreme 全量 LSF 脚本 |
| lsf/quantile_tcb_gt10.sh | 新建 | Quantile 子集 LSF 脚本 |
| lsf/pwm_tcb_gt10.sh | 新建 | PWM Extreme 子集 LSF 脚本 |
| lsf/eot_tcb_gt10.sh | 新建 | EOT 子集 LSF 脚本 |
| packages/lakeanalysis/src/lakeanalysis/quality/metrics.py | 修改 | compute_penalized_volatility → H×CV |
| packages/lakeanalysis/src/lakeanalysis/quality/filters/penalized_volatility.py | 修改 | pv_threshold=0.001, 移除 dominant_ratio_max |
| packages/lakeanalysis/scripts/run_quality.py | 修改 | --pv-threshold 默认 0.001, 移除 --pv-dominant-ratio-max |

---

## 8. 注意事项

1. **ParquetLakeProvider.persist()** 是通用的, 接受任意 table_name, EOT 输出表可正常写入
2. **ParquetLakeProvider.fetch_lake_area_chunk()** JOIN area_quality, 所以子集只需 area_quality 中只含目标湖泊即可
3. **算法 resume**: 每个 Calculator 检查 `*_run_status` 表跳过已处理湖泊, 清空 run_status 即可全量重跑
4. **EOT 参数**: `--tail both --threshold-quantiles 0.95 0.98`, 每个 lake 产生 4 组结果 (high×0.95, high×0.98, low×0.95, low×0.98)
5. **子集 LSF 资源更少**: 32 workers / 64GB RAM / 更短 walltime, 因为只有 ~14,777 个湖泊
6. **rsync 增量**: rsync -avz 只传输差异部分, 如果之前已传过全量 parquet, 第二次只传更新的 area_quality/area_anomalies
7. **SSH 端口**: 4351（非默认 22）
8. **子集导出**: 从本地 parquet 直接读取过滤（DuckDB 下推），无需 PostgreSQL，4 秒完成

---

## 9. 作业状态检查

```bash
ssh -p 4351 guxh01@111.172.12.146
bjobs -w  # 列出所有作业
bpeek <JobID>  # 查看输出
```

### 完成后检查输出文件

```bash
ls -la /data/users/guxh01/2026_tcb/lake/lake_data/quantile_*.parquet
ls -la /data/users/guxh01/2026_tcb/lake/lake_data/pwm_extreme_*.parquet
ls -la /data/users/guxh01/2026_tcb/lake/lake_data/eot_*.parquet
ls -la /data/users/guxh01/2026_tcb/lake/lake_data_gt10/
```
