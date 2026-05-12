# Smoke Test 重构 Memo

> 日期: 2026-05-11
> 分支: fix/smoke-test-failures
> 背景: smoke test 暴露出两类问题——(1) `batch/io.py` 中 `_filter_done_status_ids` 使用了错误的 PyArrow API 且破坏了 provider 封装；(2) 测试结构将 PostgreSQL 和 Parquet backend 分开编写，实际运行时因 `.env` 中 `DATA_BACKEND=parquet` 导致"PostgreSQL 测试"也走了 Parquet 路径，测试意图与实际行为不一致。

---

## 1. 问题诊断

### 1.1 `_filter_done_status_ids` 的问题

位置: `packages/lakeanalysis/src/lakeanalysis/batch/io.py:107-143`

```python
def _filter_done_status_ids(provider, table_name, chunk_start, chunk_end, candidate_ids):
    if provider.backend_name == "parquet":
        # 直接访问 provider._data_dir（私有属性）
        # 使用 pq.ParquetFile.read(filters=...) — API 不存在
        ...
    else:
        # 调用 provider.fetch_rows() + Python 过滤
        ...
```

问题清单:

| # | 问题 | 严重性 |
|---|------|--------|
| 1 | `ParquetFile.read()` 不支持 `filters` 参数，运行时 TypeError | 阻断 |
| 2 | 通过 `provider.backend_name` 做 backend sniffing，违反 LSP | 设计缺陷 |
| 3 | 直接访问 `provider._data_dir` 私有属性 | 封装破坏 |
| 4 | Postgres 分支用 `fetch_rows()` 全量读取再 Python 过滤，性能差 | 次要 |

### 1.2 测试结构问题

- `.env` 设置 `DATA_BACKEND=parquet`，`SourceConfig()` 默认构造走 parquet
- `conftest.py` 中 `provider` fixture 调用 `load_env()` 后创建的是 ParquetLakeProvider
- 前 5 个"PostgreSQL backend"测试实际走 Parquet，与后面的 `test_d_parquet_*` 重复
- `pwm_extreme` 使用通用 `id_range`（来自 `sample_hylak_ids`），但这些 lake 数据量不足（每月 < 10 条），导致 PWM 算法全部失败
- MPI 测试验证逻辑直接查 PostgreSQL，但 engine 写入的是 parquet 文件

### 1.3 根因

`LakeProvider.fetch_done_ids` 只返回"表中存在的 ID"，不支持按 `status` 列过滤。当 `done_requires_status=True` 时（quantile/pwm_extreme/eot/comparison/pwm_hawkes 全部如此），`batch/io.py` 被迫自行实现 status 过滤，导致 backend-specific 代码泄漏到 IO 层。

---

## 2. 重构方案

### 2.1 Provider 接口扩展

**文件:** `packages/lakesource/src/lakesource/provider/base.py`

```python
def fetch_done_ids(
    self,
    table_name: str,
    chunk_start: int,
    chunk_end: int,
    *,
    status: str | None = None,
) -> set[int]:
    raise NotImplementedError(...)
```

不向后兼容——所有子类必须同步更新签名。当前只有两个实现（PostgresLakeProvider、ParquetLakeProvider），均在本次修改范围内。

**PostgresLakeProvider 实现:**

```python
def fetch_done_ids(
    self, table_name: str, chunk_start: int, chunk_end: int,
    *, status: str | None = None,
) -> set[int]:
    with self._conn() as conn:
        with conn.cursor() as cur:
            if status is None:
                cur.execute(
                    f"SELECT DISTINCT hylak_id FROM {table_name} "
                    f"WHERE hylak_id >= %s AND hylak_id < %s",
                    (chunk_start, chunk_end),
                )
            else:
                cur.execute(
                    f"SELECT DISTINCT hylak_id FROM {table_name} "
                    f"WHERE hylak_id >= %s AND hylak_id < %s AND status = %s",
                    (chunk_start, chunk_end, status),
                )
            return {int(row[0]) for row in cur.fetchall()}
```

**ParquetLakeProvider 实现:**

```python
def fetch_done_ids(
    self, table_name: str, chunk_start: int, chunk_end: int,
    *, status: str | None = None,
) -> set[int]:
    df = self._read_table_df(table_name)
    if df.empty or "hylak_id" not in df.columns:
        return set()
    mask = (df["hylak_id"] >= chunk_start) & (df["hylak_id"] < chunk_end)
    if status is not None and "status" in df.columns:
        mask &= df["status"] == status
    return set(df.loc[mask, "hylak_id"].astype(int).tolist())
```

### 2.2 删除 `_filter_done_status_ids`

**文件:** `packages/lakeanalysis/src/lakeanalysis/batch/io.py`

删除 `_filter_done_status_ids` 函数（第 107-143 行），修改 `ProviderBatchReader.fetch_done_ids`:

```python
def fetch_done_ids(self, algorithm: str, chunk_start: int, chunk_end: int) -> set[int]:
    spec = get_batch_task_spec(algorithm)
    table_name = self._done_table if self._done_table is not None else spec.done_table
    if table_name is None:
        return set()
    done_requires_status = self._done_requires_status or spec.done_requires_status
    status = "done" if done_requires_status else None
    return self._provider.fetch_done_ids(table_name, chunk_start, chunk_end, status=status)
```

影响分析: `_filter_done_status_ids` 是私有函数，仅在 `io.py:79` 一处调用。其他直接调用 `provider.fetch_done_ids()` 的位置（`entropy/service.py`、`quality/batch.py`、`artificial/pfaf/runner.py`）均不传 `status` 参数，行为不变。

### 2.3 测试重构: backend 参数化

**文件:** `packages/lakeanalysis/tests/smoke/conftest.py`

用 `@pytest.fixture(params=["parquet", "postgres"])` 统一双 backend 测试。

核心 fixtures:

```python
@pytest.fixture(params=["parquet", "postgres"], scope="session")
def backend(request):
    """Parametrize across backends. Skip postgres if env not available."""
    if request.param == "postgres":
        from lakesource.env import load_env
        load_env()
        _require_db_env()
    return request.param


@pytest.fixture(scope="session")
def source_config(backend, parquet_data_dir):
    """Return SourceConfig for the current backend."""
    if backend == "parquet":
        return SourceConfig(backend=Backend.PARQUET, data_dir=parquet_data_dir)
    from lakesource.env import load_env
    load_env()
    return SourceConfig(backend=Backend.POSTGRES)


@pytest.fixture(scope="session")
def id_range(backend, source_config):
    """Discover id range suitable for quantile/eot."""
    ...


@pytest.fixture(scope="session")
def pwm_id_range(backend, source_config):
    """Discover id range suitable for pwm_extreme.

    Uses brute-force trial: iterate candidate start IDs, run PWM on each lake,
    return first range with at least 1 success.
    Candidate list: [2, 6, 14, 35, 63, 68, 483363] (shared across backends, data is同源).
    """
    ...


@pytest.fixture()
def cleanup(backend, source_config, ...):
    """Post-test cleanup: unlink parquet files or DELETE FROM postgres tables."""
    ...
```

删除旧的重复 fixtures: `provider`, `sample_hylak_ids`, `parquet_provider`, `parquet_id_range`, `parquet_pwm_id_range`.

### 2.4 测试重构: test_batch_smoke.py

**文件:** `packages/lakeanalysis/tests/smoke/test_batch_smoke.py`

```python
@pytest.mark.parametrize("algorithm,calc_kwargs", [
    ("quantile", {}),
    ("eot", {"tails": ["high"], "quantiles": [0.95]}),
])
def test_algorithm_smoke(algorithm, calc_kwargs, source_config, id_range, cleanup):
    """quantile/eot × parquet/postgres."""
    cleanup.register(algorithm)
    _run_algorithm_smoke(algorithm, id_range[0], id_range[1], source_config, **calc_kwargs)


def test_pwm_extreme_smoke(source_config, pwm_id_range, cleanup):
    """pwm_extreme × parquet/postgres (dedicated id range)."""
    cleanup.register("pwm_extreme")
    _run_algorithm_smoke("pwm_extreme", pwm_id_range[0], pwm_id_range[1], source_config)


def test_incremental_skip(source_config, id_range, cleanup):
    """quantile run twice; second run should skip all lakes."""
    cleanup.register("quantile")
    engine = _build_engine("quantile", id_range, source_config)
    report1 = engine.run()
    assert report1.success_lakes >= 1
    report2 = engine.run()
    assert report2.skipped_lakes >= 1
    assert report2.success_lakes == 0


def test_error_handling(source_config, id_range, cleanup):
    """Deliberately trigger errors with impossible min_valid_observations."""
    cleanup.register("quantile")
    engine = _build_engine("quantile", id_range, source_config, min_valid_observations=999_999)
    report = engine.run()
    assert report is not None
    assert report.error_lakes >= 0
```

测试 ID 示例:
- `test_algorithm_smoke[parquet-quantile-{}]`
- `test_algorithm_smoke[postgres-quantile-{}]`
- `test_pwm_extreme_smoke[parquet]`
- `test_pwm_extreme_smoke[postgres]`
- `test_incremental_skip[parquet]`
- `test_incremental_skip[postgres]`

### 2.5 测试重构: test_batch_mpi_smoke.py

**文件:** `packages/lakeanalysis/tests/smoke/test_batch_mpi_smoke.py`

- `_run_mpi_batch` 增加 `backend` 参数，通过 `env["DATA_BACKEND"]` 传递给子进程
- 验证逻辑根据 backend 分支:
  - parquet: 读 `*_run_status.parquet` 验证 done 记录
  - postgres: 查 `series_conn` 验证 done 记录
- 测试函数通过 `backend` fixture 参数化
- cleanup 根据 backend 清理对应存储

---

## 3. 影响范围

| 包 | 文件 | 改动类型 |
|----|------|----------|
| lakesource | `provider/base.py` | 接口扩展（不向后兼容） |
| lakesource | `provider/postgres_provider.py` | 实现扩展 |
| lakesource | `provider/parquet_provider.py` | 实现扩展 |
| lakeanalysis | `batch/io.py` | 删除 `_filter_done_status_ids` + 简化调用 |
| lakeanalysis | `tests/smoke/conftest.py` | 重构 fixtures |
| lakeanalysis | `tests/smoke/test_batch_smoke.py` | 重构为参数化 |
| lakeanalysis | `tests/smoke/test_batch_mpi_smoke.py` | 参数化 + 验证适配 |

总计 7 个文件，无新增源码文件。

---

## 4. 验收标准

```bash
uv run pytest packages/lakeanalysis/tests/smoke/ -v
```

- 所有测试 × 两个 backend 通过
- PostgreSQL backend 在 env 不可用时自动 skip（不报 FAIL）
- PWM 测试使用动态发现的 id range，不因数据量不足而假失败
- `batch/io.py` 中无 backend sniffing，无 `provider._data_dir` 访问
- MPI 测试验证逻辑与实际写入的 backend 一致

---

## 5. 注意事项

- `SourceConfig()` 默认 backend 由 `DATA_BACKEND` 环境变量决定（当前为 `parquet`）；测试中通过 fixture 显式指定 backend，不依赖环境变量
- PWM id range 候选列表 `[2, 6, 14, 35, 63, 68, 483363]` 两个 backend 共用（数据同源）
- `fetch_done_ids` 签名变更不向后兼容，所有 `LakeProvider` 子类必须同步更新
- 其他直接调用 `provider.fetch_done_ids()` 的位置（entropy、quality、pfaf）不传 `status`，行为不变，无需修改
