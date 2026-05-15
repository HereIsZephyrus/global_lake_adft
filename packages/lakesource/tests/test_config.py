"""Tests for lakesource config — env resolution, error paths, connection params."""

from __future__ import annotations

from pathlib import Path

import pytest

from lakesource.config import Backend, OutputFilter, SourceConfig, _env, _normalize_data_dir
from lakesource.env import config_dir, ensure_env_loaded, load_env, _find_dotenv


# ── _env() helper ──────────────────────────────────────────────────────────

class TestEnvHelper:
    def test_reads_existing_env_var(self, monkeypatch):
        monkeypatch.setenv("TEST_KEY", "hello")
        assert _env("TEST_KEY") == "hello"

    def test_returns_default_when_not_set(self, monkeypatch):
        monkeypatch.delenv("TEST_KEY", raising=False)
        assert _env("TEST_KEY", "fallback") == "fallback"

    def test_returns_none_when_not_set_no_default(self, monkeypatch):
        monkeypatch.delenv("TEST_KEY", raising=False)
        assert _env("TEST_KEY") is None

    def test_strips_whitespace(self, monkeypatch):
        monkeypatch.setenv("TEST_KEY", "  spaced  ")
        assert _env("TEST_KEY") == "spaced"

    def test_strips_double_quotes(self, monkeypatch):
        monkeypatch.setenv("TEST_KEY", '"quoted"')
        assert _env("TEST_KEY") == "quoted"

    def test_strips_quotes_and_whitespace(self, monkeypatch):
        monkeypatch.setenv("TEST_KEY", '  "value"  ')
        assert _env("TEST_KEY") == "value"

    def test_empty_string_after_clean_returns_default(self, monkeypatch):
        monkeypatch.setenv("TEST_KEY", '  ""  ')
        assert _env("TEST_KEY", "fallback") == "fallback"

    def test_empty_string_no_default_returns_none(self, monkeypatch):
        monkeypatch.setenv("TEST_KEY", "  ")
        assert _env("TEST_KEY") is None


# ── _normalize_data_dir() ─────────────────────────────────────────────────

class TestNormalizeDataDir:
    def test_no_change_for_non_legacy_path(self):
        assert _normalize_data_dir(Path("/real/data/path")) == Path("/real/data/path")

    def test_collapses_legacy_parquet_variant(self):
        assert _normalize_data_dir(Path("/base/data/parquet")) == Path("/base/data")

    def test_collapses_legacy_full_variant(self):
        assert _normalize_data_dir(Path("/base/data/full")) == Path("/base/data")

    def test_collapses_legacy_gt10_variant(self):
        assert _normalize_data_dir(Path("/base/data/gt10")) == Path("/base/data")

    def test_collapses_legacy_no_pwm_err_variant(self):
        assert _normalize_data_dir(Path("/base/data/no_pwm_err")) == Path("/base/data")

    def test_no_collapse_when_grandparent_not_named_data(self):
        assert _normalize_data_dir(Path("/base/other/parquet")) == Path("/base/other/parquet")

    def test_no_collapse_when_not_a_known_variant_name(self):
        assert _normalize_data_dir(Path("/base/data/custom")) == Path("/base/data/custom")


# ── SourceConfig: backend resolution ──────────────────────────────────────

class TestSourceConfigBackend:
    def test_explicit_backend_is_kept(self):
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.backend == Backend.POSTGRES

    def test_resolves_from_env_data_backend(self, monkeypatch):
        monkeypatch.setenv("DATA_BACKEND", "parquet")
        monkeypatch.setenv("PARQUET_DATA_DIR", "/tmp/data")
        cfg = SourceConfig()
        assert cfg.backend == Backend.PARQUET

    def test_invalid_data_backend_env_raises(self, monkeypatch):
        monkeypatch.setenv("DATA_BACKEND", "mysql")
        with pytest.raises(ValueError, match="Invalid DATA_BACKEND: 'mysql'"):
            SourceConfig()

    def test_falls_back_to_adapter_yaml_when_no_env(self, monkeypatch):
        monkeypatch.delenv("DATA_BACKEND", raising=False)
        cfg = SourceConfig()
        assert cfg.backend in (Backend.PARQUET, Backend.POSTGRES)


# ── SourceConfig: data_dir resolution ─────────────────────────────────────

class TestSourceConfigDataDir:
    def test_parquet_defaults_to_cwd_data_when_no_env(self, monkeypatch):
        monkeypatch.delenv("PARQUET_DATA_DIR", raising=False)
        cfg = SourceConfig(backend=Backend.PARQUET)
        assert cfg.data_dir == Path.cwd() / "data"

    def test_parquet_resolves_from_env_parquet_data_dir(self, monkeypatch):
        monkeypatch.setenv("PARQUET_DATA_DIR", "/env/data")
        cfg = SourceConfig(backend=Backend.PARQUET)
        assert cfg.data_dir == Path("/env/data")

    def test_parquet_env_path_is_normalized(self, monkeypatch):
        monkeypatch.setenv("PARQUET_DATA_DIR", "/base/data/parquet")
        cfg = SourceConfig(backend=Backend.PARQUET)
        assert cfg.data_dir == Path("/base/data")

    def test_explicit_data_dir_is_kept_for_parquet(self):
        cfg = SourceConfig(backend=Backend.PARQUET, data_dir=Path("/explicit"))
        assert cfg.data_dir == Path("/explicit")

    def test_explicit_data_dir_is_normalized(self):
        cfg = SourceConfig(backend=Backend.PARQUET, data_dir=Path("/base/data/gt10"))
        assert cfg.data_dir == Path("/base/data")

    def test_postgres_does_not_set_data_dir(self, monkeypatch):
        monkeypatch.delenv("PARQUET_DATA_DIR", raising=False)
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.data_dir is None


# ── SourceConfig: output_filter resolution ────────────────────────────────

class TestSourceConfigOutputFilter:
    def test_default_is_full(self, monkeypatch):
        monkeypatch.delenv("LAKE_FILTER", raising=False)
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.output_filter == OutputFilter.FULL

    def test_resolves_from_env_lake_filter(self, monkeypatch):
        monkeypatch.setenv("LAKE_FILTER", "gt10")
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.output_filter == OutputFilter.GT10

    def test_resolves_no_pwm_err(self, monkeypatch):
        monkeypatch.setenv("LAKE_FILTER", "no_pwm_err")
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.output_filter == OutputFilter.NO_PWM_ERR

    def test_invalid_lake_filter_raises(self, monkeypatch):
        monkeypatch.setenv("LAKE_FILTER", "invalid")
        with pytest.raises(ValueError, match="Invalid LAKE_FILTER: 'invalid'"):
            SourceConfig(backend=Backend.POSTGRES)

    def test_explicit_filter_is_kept(self):
        cfg = SourceConfig(backend=Backend.POSTGRES, output_filter=OutputFilter.GT10)
        assert cfg.output_filter == OutputFilter.GT10


# ── SourceConfig: output_dir / figures_dir resolution ─────────────────────

class TestSourceConfigOutputDirs:
    def test_output_dir_default_full(self, monkeypatch):
        monkeypatch.delenv("OUTPUT_DIR", raising=False)
        monkeypatch.delenv("LAKE_FILTER", raising=False)
        cfg = SourceConfig(backend=Backend.PARQUET, data_dir=Path("/data"))
        assert cfg.output_dir == Path.cwd() / "output" / "full"

    def test_output_dir_follows_output_filter(self, monkeypatch):
        monkeypatch.delenv("OUTPUT_DIR", raising=False)
        monkeypatch.setenv("LAKE_FILTER", "gt10")
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.output_dir == Path.cwd() / "output" / "gt10"

    def test_output_dir_from_env(self, monkeypatch):
        monkeypatch.setenv("OUTPUT_DIR", "/custom/output")
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.output_dir == Path("/custom/output")

    def test_figures_dir_defaults_to_output_visualize(self, monkeypatch):
        monkeypatch.delenv("OUTPUT_DIR", raising=False)
        monkeypatch.delenv("FIGURES_DIR", raising=False)
        monkeypatch.delenv("LAKE_FILTER", raising=False)
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.figures_dir == Path.cwd() / "output" / "full" / "visualize"

    def test_figures_dir_from_env(self, monkeypatch):
        monkeypatch.setenv("FIGURES_DIR", "/env/figures")
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.figures_dir == Path("/env/figures")

    def test_explicit_figures_dir_overrides_env(self, monkeypatch):
        monkeypatch.setenv("FIGURES_DIR", "/env/figures")
        cfg = SourceConfig(backend=Backend.POSTGRES, figures_dir=Path("/explicit"))
        assert cfg.figures_dir == Path("/explicit")


# ── SourceConfig: data_path resolution ────────────────────────────────────

class TestSourceConfigDataPath:
    def test_default_is_none(self, monkeypatch):
        monkeypatch.delenv("DATA_PATH", raising=False)
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.data_path is None

    def test_resolves_from_env(self, monkeypatch):
        monkeypatch.setenv("DATA_PATH", "/env/data/path")
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.data_path == Path("/env/data/path")

    def test_explicit_overrides_env(self, monkeypatch):
        monkeypatch.setenv("DATA_PATH", "/env")
        cfg = SourceConfig(backend=Backend.POSTGRES, data_path=Path("/explicit"))
        assert cfg.data_path == Path("/explicit")


# ── SourceConfig: DB connection env resolution ────────────────────────────

class TestSourceConfigDBConnectivity:
    def test_db_host_from_env(self, monkeypatch):
        monkeypatch.setenv("DB_HOST", "pg.example.com")
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.db_host == "pg.example.com"

    def test_db_host_explicit_overrides_env(self, monkeypatch):
        monkeypatch.setenv("DB_HOST", "env-host")
        cfg = SourceConfig(backend=Backend.POSTGRES, db_host="explicit-host")
        assert cfg.db_host == "explicit-host"

    def test_db_port_from_env(self, monkeypatch):
        monkeypatch.setenv("DB_PORT", "5000")
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.db_port == 5000

    def test_db_port_explicit_overrides_env(self, monkeypatch):
        monkeypatch.setenv("DB_PORT", "5000")
        cfg = SourceConfig(backend=Backend.POSTGRES, db_port=1234)
        assert cfg.db_port == 1234

    def test_db_port_invalid_env_raises(self, monkeypatch):
        monkeypatch.setenv("DB_PORT", "not_a_number")
        with pytest.raises(ValueError, match="DB_PORT must be a valid integer, got 'not_a_number'"):
            SourceConfig(backend=Backend.POSTGRES)

    def test_db_user_from_env(self, monkeypatch):
        monkeypatch.setenv("DB_USER", "admin")
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.db_user == "admin"

    def test_db_password_from_env(self, monkeypatch):
        monkeypatch.setenv("DB_PASSWORD", "secret")
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.db_password == "secret"

    def test_atlas_db_name_from_env(self, monkeypatch):
        monkeypatch.setenv("ALTAS_DB", "atlas_dev")
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.atlas_db_name == "atlas_dev"

    def test_series_db_name_from_env(self, monkeypatch):
        monkeypatch.setenv("SERIES_DB", "series_dev")
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.series_db_name == "series_dev"

    def test_db_defaults_are_none(self, monkeypatch):
        for var in ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "ALTAS_DB", "SERIES_DB"):
            monkeypatch.delenv(var, raising=False)
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.db_host is None
        assert cfg.db_port is None
        assert cfg.db_user is None
        assert cfg.db_password is None
        assert cfg.atlas_db_name is None
        assert cfg.series_db_name is None


# ── SourceConfig: properties and connection_params() ─────────────────────

class TestSourceConfigConnectionParams:
    def test_resolved_db_host_defaults_to_localhost(self, monkeypatch):
        monkeypatch.delenv("DB_HOST", raising=False)
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.resolved_db_host == "localhost"

    def test_resolved_db_host_returns_explicit(self):
        cfg = SourceConfig(backend=Backend.POSTGRES, db_host="explicit.host")
        assert cfg.resolved_db_host == "explicit.host"

    def test_resolved_db_port_defaults_to_5432(self, monkeypatch):
        monkeypatch.delenv("DB_PORT", raising=False)
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.resolved_db_port == 5432

    def test_resolved_db_port_returns_explicit(self):
        cfg = SourceConfig(backend=Backend.POSTGRES, db_port=9999)
        assert cfg.resolved_db_port == 9999

    def test_connection_params_returns_correct_dict(self):
        cfg = SourceConfig(
            backend=Backend.POSTGRES,
            db_host="myhost",
            db_port=1234,
            db_user="user",
            db_password="pass",
        )
        params = cfg.connection_params("mydb")
        assert params == {
            "host": "myhost",
            "port": 1234,
            "dbname": "mydb",
            "user": "user",
            "password": "pass",
        }

    def test_connection_params_raises_when_no_user(self, monkeypatch):
        monkeypatch.delenv("DB_USER", raising=False)
        monkeypatch.delenv("DB_PASSWORD", raising=False)
        cfg = SourceConfig(backend=Backend.POSTGRES)
        with pytest.raises(ValueError, match="db_user and db_password must be set"):
            cfg.connection_params("mydb")

    def test_connection_params_raises_when_no_password(self, monkeypatch):
        monkeypatch.delenv("DB_USER", raising=False)
        monkeypatch.delenv("DB_PASSWORD", raising=False)
        cfg = SourceConfig(backend=Backend.POSTGRES, db_user="user")
        with pytest.raises(ValueError, match="db_user and db_password must be set"):
            cfg.connection_params("mydb")

    def test_connection_params_allows_empty_password_if_set(self):
        cfg = SourceConfig(backend=Backend.POSTGRES, db_user="user", db_password="")
        params = cfg.connection_params("mydb")
        assert params["password"] == ""  # empty string is explicitly set


# ── SourceConfig: t property ──────────────────────────────────────────────

class TestSourceConfigTProperty:
    def test_t_returns_table_config(self):
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.t is not None
        assert cfg.t.series_table is not None  # has a callable method


# ── OutputFilter enum ─────────────────────────────────────────────────────

class TestOutputFilterEnum:
    def test_values(self):
        assert OutputFilter.FULL.value == "full"
        assert OutputFilter.GT10.value == "gt10"
        assert OutputFilter.NO_PWM_ERR.value == "no_pwm_err"

    def test_from_string(self):
        assert OutputFilter("full") == OutputFilter.FULL
        assert OutputFilter("gt10") == OutputFilter.GT10
        assert OutputFilter("no_pwm_err") == OutputFilter.NO_PWM_ERR


# ── Backend enum ──────────────────────────────────────────────────────────

class TestBackendEnum:
    def test_values(self):
        assert Backend.POSTGRES.value == "postgres"
        assert Backend.PARQUET.value == "parquet"

    def test_from_string(self):
        assert Backend("postgres") == Backend.POSTGRES
        assert Backend("parquet") == Backend.PARQUET

    def test_case_insensitive_from_string(self):
        assert Backend("PoStGrEs".lower()) == Backend.POSTGRES


# ── SourceConfig: year filters ────────────────────────────────────────────

class TestSourceConfigYearFilters:
    def test_default_none(self, monkeypatch):
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.year_start is None
        assert cfg.year_end is None

    def test_explicit_values_passthrough(self):
        cfg = SourceConfig(backend=Backend.POSTGRES, year_start=2000, year_end=2020)
        assert cfg.year_start == 2000
        assert cfg.year_end == 2020

    def test_partial_filter(self):
        cfg = SourceConfig(backend=Backend.POSTGRES, year_start=2000)
        assert cfg.year_start == 2000
        assert cfg.year_end is None


# ── SourceConfig: chunk_size / limit_id ───────────────────────────────────

class TestSourceConfigChunkAndLimit:
    def test_default_chunk_size(self):
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.chunk_size == 10_000

    def test_custom_chunk_size(self):
        cfg = SourceConfig(backend=Backend.POSTGRES, chunk_size=500)
        assert cfg.chunk_size == 500

    def test_limit_id_default_none(self):
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.limit_id is None

    def test_limit_id_explicit(self):
        cfg = SourceConfig(backend=Backend.POSTGRES, limit_id=1000)
        assert cfg.limit_id == 1000


# ── _find_dotenv() ─────────────────────────────────────────────────────────

class TestFindDotenv:
    def test_returns_lake_env_file_when_set(self, monkeypatch, tmp_path):
        env_file = tmp_path / "custom.env"
        env_file.write_text("KEY=val")
        monkeypatch.setenv("LAKE_ENV_FILE", str(env_file))
        assert _find_dotenv() == env_file

    def test_falls_back_to_cwd_when_dotenv_installed(self, monkeypatch, tmp_path):
        """When LAKE_ENV_FILE is not set, find_dotenv returns the cwd .env if it exists."""
        monkeypatch.delenv("LAKE_ENV_FILE", raising=False)
        dotenv_file = tmp_path / ".env"
        dotenv_file.write_text("KEY=val")
        from unittest import mock
        with mock.patch("lakesource.env.Path.cwd", return_value=tmp_path):
            assert _find_dotenv() == dotenv_file

    def test_falls_back_to_legacy_path(self, monkeypatch):
        """When no env, no .env in cwd, returns the legacy path."""
        monkeypatch.delenv("LAKE_ENV_FILE", raising=False)
        result = _find_dotenv()
        expected_suffix = Path("lakesource") / ".env"
        assert result.name == ".env"
        assert result.parent.name == "lakesource"


# ── load_env() / ensure_env_loaded() ──────────────────────────────────────

class TestEnvLoading:
    def test_load_env_sets_variables(self, tmp_path, monkeypatch):
        env_file = tmp_path / ".env"
        env_file.write_text("TEST_VAR_FROM_ENV=hooray\n")
        monkeypatch.delenv("TEST_VAR_FROM_ENV", raising=False)
        load_env(dotenv_path=env_file)
        import os
        assert os.environ["TEST_VAR_FROM_ENV"] == "hooray"

    def test_load_env_skips_if_not_exists(self, tmp_path, monkeypatch):
        nonexistent = tmp_path / "nonexistent.env"
        load_env(dotenv_path=nonexistent)  # should not raise

    def test_ensure_env_loaded_is_idempotent(self, tmp_path, monkeypatch):
        env_file = tmp_path / ".env"
        env_file.write_text("DB_HOST=example.test\n")
        monkeypatch.delenv("DB_HOST", raising=False)
        ensure_env_loaded(dotenv_path=env_file)
        ensure_env_loaded(dotenv_path=env_file)  # second call is no-op

    def test_load_env_override_true(self, tmp_path, monkeypatch):
        env_file = tmp_path / ".env"
        env_file.write_text("OVERRIDE_VAR=new_value\n")
        monkeypatch.setenv("OVERRIDE_VAR", "old_value")
        load_env(dotenv_path=env_file, override=True)
        import os
        assert os.environ["OVERRIDE_VAR"] == "new_value"

    def test_load_env_override_false(self, tmp_path, monkeypatch):
        env_file = tmp_path / ".env"
        env_file.write_text("OVERRIDE_VAR=new_value\n")
        monkeypatch.setenv("OVERRIDE_VAR", "old_value")
        load_env(dotenv_path=env_file, override=False)
        import os
        assert os.environ["OVERRIDE_VAR"] == "old_value"


# ── config_dir() ──────────────────────────────────────────────────────────

class TestConfigDir:
    def test_defaults_to_cwd_config(self, monkeypatch):
        monkeypatch.delenv("LAKE_CONFIG_DIR", raising=False)
        assert config_dir() == Path.cwd() / "config"

    def test_resolves_from_env(self, monkeypatch):
        monkeypatch.setenv("LAKE_CONFIG_DIR", "/custom/config")
        assert config_dir() == Path("/custom/config")


# ── SourceConfig: frozen dataclass ────────────────────────────────────────

class TestSourceConfigFrozen:
    def test_cannot_mutate_after_construction(self):
        cfg = SourceConfig(backend=Backend.POSTGRES)
        with pytest.raises(Exception):
            cfg.backend = Backend.PARQUET  # type: ignore[misc]

    def test_tables_is_never_none_after_post_init(self):
        cfg = SourceConfig(backend=Backend.POSTGRES)
        assert cfg.tables is not None


# ── Legacy tests (kept for backward validation) ───────────────────────────

def test_year_filters_default_none():
    config = SourceConfig(backend=Backend.POSTGRES)
    assert config.year_start is None
    assert config.year_end is None


def test_year_filters_set():
    config = SourceConfig(backend=Backend.POSTGRES, year_start=2010, year_end=2020)
    assert config.year_start == 2010
    assert config.year_end == 2020
