"""Integration tests for hawkes_qc read-only queries (P0).

Requires pwm_hawkes_results, pwm_hawkes_lrt, pwm_hawkes_transition_monthly,
eot_hawkes_results tables to be created and seeded.
"""

from __future__ import annotations

import psycopg
import pytest

from lakesource.config import Backend, SourceConfig

TEST_DSN = "host=localhost port=5432 dbname=lake_test user=postgres password=postgres"


def _make_provider() -> "PostgresLakeProvider":
    from lakesource.provider.postgres_provider import PostgresLakeProvider
    import os
    os.environ["SERIES_DB"] = "lake_test"
    os.environ["DB_USER"] = "postgres"
    os.environ["DB_PASSWORD"] = "postgres"
    os.environ["DB_HOST"] = "localhost"
    os.environ["DB_PORT"] = "5432"
    return PostgresLakeProvider(
        SourceConfig(
            backend=Backend.POSTGRES,
            db_host="localhost", db_port=5432,
            db_user="postgres", db_password="postgres",
            series_db_name="lake_test",
        )
    )


@pytest.fixture(scope="module")
def _seed_hawkes():
    """Module-scoped: create tables, seed data, cleanup on teardown."""
    provider = _make_provider()
    provider.ensure_table("pwm_hawkes")
    provider.ensure_table("eot_hawkes")
    provider.ensure_table("eot")

    conn = psycopg.connect(TEST_DSN)
    conn.autocommit = True
    with conn.cursor() as cur:
        # pwm_hawkes_results — 4 rows across 2 quantiles
        for hid, q, conv, mu_d, mu_w, la, qc_pass in [
            (1, 0.95, True, 0.1, 0.2, -100.0, True),
            (2, 0.95, False, 0.3, 0.4, -200.0, False),
            (1, 0.98, True, 0.15, 0.25, -150.0, True),
            (2, 0.98, True, 0.35, 0.45, -120.0, True),
        ]:
            cur.execute(
                """INSERT INTO pwm_hawkes_results (
                    hylak_id, threshold_quantile, converged, log_likelihood,
                    objective_value, n_events, n_dry_events, n_wet_events,
                    mu_d, mu_w, alpha_dd, alpha_dw, alpha_wd, alpha_ww,
                    beta_dd, beta_dw, beta_wd, beta_ww, spectral_radius,
                    lrt_p_d_to_w, lrt_p_w_to_d, qc_pass, qc_exceedance_rate,
                    qc_relative_amplitude, qc_median_excess, error_message
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, 10, 5, 5,
                    %s, %s, 0.1, 0.1, 0.1, 0.1,
                    1.0, 1.0, 1.0, 1.0, 0.5,
                    0.01, 0.02, %s, 0.5,
                    0.3, 0.2, %s
                )""",
                (hid, q, conv, la, la, mu_d, mu_w, qc_pass,
                 "err timeout" if not qc_pass else None),
            )

        # pwm_hawkes_lrt — 2 rows
        for hid, q, test_name, p_val, reject in [
            (1, 0.95, "d->w", 0.001, True),
            (2, 0.95, "d->w", 0.500, False),
        ]:
            cur.execute(
                """INSERT INTO pwm_hawkes_lrt (
                    hylak_id, threshold_quantile, test_name,
                    lr_statistic, df, p_value, significance_level,
                    reject_null, restricted_log_likelihood, full_log_likelihood
                ) VALUES (%s, %s, %s, 10.0, 2, %s, 0.05, %s, -110.0, -100.0)""",
                (hid, q, test_name, p_val, reject),
            )

        # pwm_hawkes_transition_monthly — 2 rows
        for hid, q, year, month, direction, sig in [
            (1, 0.95, 2020, 1, "dry->wet", True),
            (2, 0.95, 2020, 6, "wet->dry", False),
        ]:
            cur.execute(
                """INSERT INTO pwm_hawkes_transition_monthly (
                    hylak_id, threshold_quantile, year, month, direction,
                    score_raw, score_norm, significance_quantile,
                    significance_threshold, significant
                ) VALUES (%s, %s, %s, %s, %s, 5.0, 2.5, 0.95, 3.0, %s)""",
                (hid, q, year, month, direction, sig),
            )

        # eot_hawkes_results — 1 row for coverage test
        cur.execute(
            """INSERT INTO eot_hawkes_results (
                hylak_id, threshold_quantile, converged, log_likelihood,
                objective_value, n_events, n_dry_events, n_wet_events,
                mu_d, mu_w, alpha_dd, alpha_dw, alpha_wd, alpha_ww,
                beta_dd, beta_dw, beta_wd, beta_ww, spectral_radius,
                lrt_p_d_to_w, lrt_p_w_to_d, qc_pass, qc_exceedance_rate,
                qc_relative_amplitude, qc_median_excess, error_message
            ) VALUES (1, 0.95, TRUE, -100.0, 0.0, 10, 5, 5,
                      0.1, 0.2, 0.1, 0.1, 0.1, 0.1,
                      1.0, 1.0, 1.0, 1.0, 0.5,
                      0.01, 0.02, TRUE, 0.5, 0.3, 0.2, NULL)"""
        )

        # eot_results — 4 rows for coverage test
        for hid, q, tail in [
            (1, 0.95, "high"), (1, 0.95, "low"),
            (2, 0.95, "high"), (3, 0.95, "high"),
        ]:
            cur.execute(
                """INSERT INTO eot_results (
                    hylak_id, tail, threshold_quantile, converged,
                    log_likelihood, threshold, n_extremes, n_observations,
                    n_frozen_months, beta0, beta1, sin_1, cos_1, sigma, xi
                ) VALUES (%s, %s, %s, TRUE, -10.0, 100.0, 5, 240, 0,
                         10.0, 0.1, 0.2, 0.3, 2.0, 0.1)""",
                (hid, tail, q),
            )
    conn.close()

    yield

    # teardown: truncate seeded data
    conn = psycopg.connect(TEST_DSN)
    conn.autocommit = True
    with conn.cursor() as cur:
        for tbl in [
            "pwm_hawkes_results", "pwm_hawkes_lrt",
            "pwm_hawkes_transition_monthly",
            "eot_hawkes_results",
            "eot_results",
        ]:
            cur.execute(f"TRUNCATE {tbl} CASCADE")
    conn.close()


class TestHawkesQCSummaryByQuantile:
    @pytest.mark.usefixtures("_seed_hawkes")
    def test_groupby_aggregation(self, provider) -> None:
        from lakesource.postgres.hawkes_qc import fetch_hawkes_qc_summary_by_quantile
        with provider._conn() as conn:
            df = fetch_hawkes_qc_summary_by_quantile(conn)
        assert len(df) >= 2
        assert "qc_pass_rate" in df.columns
        assert "converged_rate" in df.columns

    @pytest.mark.usefixtures("_seed_hawkes")
    def test_qc_pass_rate(self, provider) -> None:
        from lakesource.postgres.hawkes_qc import fetch_hawkes_qc_summary_by_quantile
        with provider._conn() as conn:
            df = fetch_hawkes_qc_summary_by_quantile(conn)
        row = df[df["threshold_quantile"] == 0.95]
        assert float(row["qc_pass_rate"].iloc[0]) == 0.5


class TestHawkesErrorCounts:
    @pytest.mark.usefixtures("_seed_hawkes")
    def test_groupby(self, provider) -> None:
        from lakesource.postgres.hawkes_qc import fetch_hawkes_error_message_counts
        with provider._conn() as conn:
            df = fetch_hawkes_error_message_counts(conn, limit=10)
        assert len(df) >= 1
        assert "error_prefix" in df.columns


class TestHawkesResultsSelect:
    @pytest.mark.usefixtures("_seed_hawkes")
    def test_no_filter(self, provider) -> None:
        from lakesource.postgres.hawkes_qc import fetch_hawkes_results
        with provider._conn() as conn:
            df = fetch_hawkes_results(conn)
        assert len(df) == 4

    @pytest.mark.usefixtures("_seed_hawkes")
    def test_quantile_filter(self, provider) -> None:
        from lakesource.postgres.hawkes_qc import fetch_hawkes_results
        with provider._conn() as conn:
            df = fetch_hawkes_results(conn, threshold_quantile=0.95)
        assert len(df) == 2

    @pytest.mark.usefixtures("_seed_hawkes")
    def test_qc_pass_only(self, provider) -> None:
        from lakesource.postgres.hawkes_qc import fetch_hawkes_results
        with provider._conn() as conn:
            df = fetch_hawkes_results(conn, qc_pass_only=True)
        assert (df["qc_pass"] == True).all()  # noqa: E712

    @pytest.mark.usefixtures("_seed_hawkes")
    def test_limit(self, provider) -> None:
        from lakesource.postgres.hawkes_qc import fetch_hawkes_results
        with provider._conn() as conn:
            df = fetch_hawkes_results(conn, limit=1)
        assert len(df) == 1


class TestHawkesLRTSelect:
    @pytest.mark.usefixtures("_seed_hawkes")
    def test_no_filter(self, provider) -> None:
        from lakesource.postgres.hawkes_qc import fetch_hawkes_lrt
        with provider._conn() as conn:
            df = fetch_hawkes_lrt(conn)
        assert len(df) >= 2


class TestHawkesLRTSummary:
    @pytest.mark.usefixtures("_seed_hawkes")
    def test_aggregation(self, provider) -> None:
        from lakesource.postgres.hawkes_qc import fetch_hawkes_lrt_summary_by_test
        with provider._conn() as conn:
            df = fetch_hawkes_lrt_summary_by_test(conn)
        assert "reject_null_rate" in df.columns
        assert "mean_p_value" in df.columns


class TestEOTHawkesCoverage:
    @pytest.mark.usefixtures("_seed_hawkes")
    def test_coverage(self, provider) -> None:
        from lakesource.postgres.hawkes_qc import fetch_eot_hawkes_coverage
        with provider._conn() as conn:
            df = fetch_eot_hawkes_coverage(conn)
        assert len(df) >= 1
        assert "frac_both_eot_tails" in df.columns


class TestHawkesTransitionMonthly:
    @pytest.mark.usefixtures("_seed_hawkes")
    def test_no_filter(self, provider) -> None:
        from lakesource.postgres.hawkes_qc import fetch_hawkes_transition_monthly
        with provider._conn() as conn:
            df = fetch_hawkes_transition_monthly(conn)
        assert len(df) >= 2

    @pytest.mark.usefixtures("_seed_hawkes")
    def test_by_hylak_id(self, provider) -> None:
        from lakesource.postgres.hawkes_qc import fetch_hawkes_transition_monthly
        with provider._conn() as conn:
            df = fetch_hawkes_transition_monthly(conn, hylak_id=1)
        assert len(df) == 1
        assert df.iloc[0]["direction"] == "dry->wet"
