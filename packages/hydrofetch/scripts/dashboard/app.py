"""Streamlit Web UI for monitoring Hydrofetch jobs on localhost."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components

from data_loader import load_jobs
from db_stats import load_db_progress
from log_parser import build_log_alerts, parse_logs
from metrics import (
    build_alerts,
    build_date_progress,
    build_failure_tables,
    build_kpis,
    build_recent_activity,
    build_recent_updates,
    build_state_counts,
    build_tile_progress,
)

REPO_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_JOB_DIR = REPO_ROOT / "data" / "hydrofetch_full_file_db_jobs"
DEFAULT_LOG_DIR = REPO_ROOT / "logs"
DEFAULT_DB_TABLE = "era5_forcing"
DEFAULT_PORT = 8501


@st.cache_data(show_spinner=False)
def cached_load_jobs(job_dir: str):
    return load_jobs(job_dir)


@st.cache_data(show_spinner=False)
def cached_parse_logs(log_dir: str):
    return parse_logs(log_dir)


@st.cache_data(show_spinner=False)
def cached_load_db_progress(table_name: str):
    return load_db_progress(table_name=table_name)


def _enable_auto_refresh(seconds: int) -> None:
    if seconds <= 0:
        return
    components.html(
        f"""
        <script>
        window.setTimeout(function() {{
            window.parent.location.reload();
        }}, {seconds * 1000});
        </script>
        """,
        height=0,
    )


def _render_dataframe(df: pd.DataFrame, height: int = 420) -> None:
    if df.empty:
        st.info("暂无数据。")
        return
    st.dataframe(df, height=height)


def render_overview(
    jobs_df: pd.DataFrame,
    logs_df: pd.DataFrame,
    db_table: str,
    job_dir: str,
    log_dir: str,
) -> None:
    kpis = build_kpis(jobs_df)
    state_counts = build_state_counts(jobs_df)
    recent_activity = build_recent_activity(jobs_df, hours=6)
    tile_progress = build_tile_progress(jobs_df).head(20)

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("总任务数", f"{kpis['total_jobs']:,}")
    col2.metric("活跃任务", f"{kpis['active_jobs']:,}")
    col3.metric("完成数", f"{kpis['completed_jobs']:,}")
    col4.metric("失败数", f"{kpis['failed_jobs']:,}")
    col5.metric("完成率", f"{kpis['completion_rate']:.2f}%")

    left, right = st.columns([1, 1])
    with left:
        fig = px.bar(
            state_counts,
            x="state",
            y="count",
            title="状态分布",
            color="state",
            color_discrete_sequence=px.colors.qualitative.Safe,
        )
        st.plotly_chart(fig, use_container_width=True)
    with right:
        if recent_activity.empty:
            st.info("最近 6 小时没有可用更新时间趋势。")
        else:
            fig = px.line(
                recent_activity,
                x="hour",
                y="count",
                color="state",
                markers=True,
                title="最近 6 小时状态更新时间趋势",
            )
            st.plotly_chart(fig, use_container_width=True)

    st.subheader("按 Tile 进度")
    _render_dataframe(tile_progress)

    st.subheader("运行上下文")
    latest_log = logs_df["timestamp"].max() if not logs_df.empty else None
    context_df = pd.DataFrame(
        [
            {"项": "Job 目录", "值": job_dir},
            {"项": "日志目录", "值": log_dir},
            {"项": "数据库表", "值": db_table},
            {"项": "最新日志时间", "值": str(latest_log) if latest_log is not None else "N/A"},
            {"项": "访问地址", "值": f"http://localhost:{DEFAULT_PORT}"},
        ]
    )
    _render_dataframe(context_df, height=220)


def render_failures(jobs_df: pd.DataFrame) -> None:
    failed_jobs, error_counts = build_failure_tables(jobs_df)
    if failed_jobs.empty:
        st.success("当前没有 failed 任务。")
        return

    top_left, top_right = st.columns([1, 1])
    with top_left:
        fig = px.bar(
            error_counts.head(15),
            x="count",
            y="error_group",
            orientation="h",
            title="错误聚合 Top 15",
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, use_container_width=True)
    with top_right:
        tile_counts = (
            failed_jobs.groupby("tile_id")
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        fig = px.bar(tile_counts, x="tile_id", y="count", title="失败按 Tile 分布")
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("失败按日期分布")
    date_counts = (
        failed_jobs.groupby("date_iso").size().reset_index(name="count").sort_values("date_iso")
    )
    fig = px.line(date_counts, x="date_iso", y="count", markers=True)
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("失败任务表")
    columns = ["job_id", "tile_id", "date_iso", "attempt", "last_error", "updated_at"]
    _render_dataframe(failed_jobs.loc[:, columns], height=520)


def render_tasks(jobs_df: pd.DataFrame) -> None:
    st.subheader("任务筛选")
    state_options = ["全部"] + [str(item) for item in jobs_df["state"].dropna().unique().tolist()]
    tile_options = ["全部"] + sorted(str(item) for item in jobs_df["tile_id"].dropna().unique().tolist())

    col1, col2, col3, col4 = st.columns(4)
    selected_state = col1.selectbox("状态", state_options, index=0)
    selected_tile = col2.selectbox("Tile", tile_options, index=0)
    min_attempt = col3.number_input("最小 attempt", min_value=0, value=0, step=1)
    only_failed = col4.checkbox("仅失败任务", value=False)

    filtered = jobs_df.copy()
    if selected_state != "全部":
        filtered = filtered.loc[filtered["state"].astype(str) == selected_state]
    if selected_tile != "全部":
        filtered = filtered.loc[filtered["tile_id"].astype(str) == selected_tile]
    filtered = filtered.loc[filtered["attempt"] >= min_attempt]
    if only_failed:
        filtered = filtered.loc[filtered["state"].astype(str) == "failed"]

    st.caption(f"筛选后任务数: {len(filtered):,}")
    columns = [
        "job_id",
        "state",
        "tile_id",
        "date_iso",
        "attempt",
        "task_id",
        "drive_file_id",
        "local_raw_path",
        "local_sample_path",
        "updated_at",
        "last_error",
    ]
    _render_dataframe(filtered.loc[:, columns], height=560)


def render_ingest(jobs_df: pd.DataFrame, db_table: str) -> None:
    st.subheader("入库进度")
    db_progress = cached_load_db_progress(db_table)

    fallback_completed = int((jobs_df["state"] == "completed").sum()) if not jobs_df.empty else 0
    fallback_write = int((jobs_df["state"] == "write").sum()) if not jobs_df.empty else 0

    if db_progress.available:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("数据库总行数", f"{db_progress.total_rows:,}")
        col2.metric("最早日期", db_progress.min_date or "N/A")
        col3.metric("最晚日期", db_progress.max_date or "N/A")
        col4.metric("最近入库时间", db_progress.latest_ingested_at or "N/A")

        st.subheader("最近每日写入行数")
        if db_progress.daily_counts is not None and not db_progress.daily_counts.empty:
            fig = px.bar(
                db_progress.daily_counts.sort_values("date"),
                x="date",
                y="row_count",
                title="近 30 天每日入库行数",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("数据库表存在，但最近没有读取到每日入库统计。")

        st.subheader("最近成功写入记录")
        if db_progress.recent_rows is not None:
            _render_dataframe(db_progress.recent_rows, height=320)
    else:
        st.warning(db_progress.message)

    st.subheader("Job 与 DB 对照")
    compare_df = pd.DataFrame(
        [
            {"指标": "completed jobs", "值": fallback_completed},
            {"指标": "write jobs", "值": fallback_write},
            {"指标": "db rows", "值": db_progress.total_rows if db_progress.available else "N/A"},
        ]
    )
    _render_dataframe(compare_df, height=180)


def render_alerts(jobs_df: pd.DataFrame, logs_df: pd.DataFrame) -> None:
    alerts = build_alerts(jobs_df)
    log_alerts = build_log_alerts(logs_df)

    st.subheader("长时间停滞任务")
    stalled_columns = ["job_id", "state", "tile_id", "date_iso", "updated_at", "updated_age_hours"]
    _render_dataframe(alerts["stalled"].loc[:, stalled_columns] if not alerts["stalled"].empty else alerts["stalled"])

    st.subheader("高重试但未失败")
    retry_columns = ["job_id", "state", "tile_id", "date_iso", "attempt", "updated_at"]
    _render_dataframe(alerts["high_retry"].loc[:, retry_columns] if not alerts["high_retry"].empty else alerts["high_retry"], height=280)

    st.subheader("最近日志错误")
    error_columns = ["timestamp", "logger", "message", "log_file"]
    _render_dataframe(log_alerts["errors"].loc[:, error_columns] if not log_alerts["errors"].empty else log_alerts["errors"], height=320)

    st.subheader("最近写库日志")
    write_columns = ["timestamp", "logger", "message", "log_file"]
    _render_dataframe(log_alerts["writes"].loc[:, write_columns] if not log_alerts["writes"].empty else log_alerts["writes"], height=260)


def main() -> None:
    st.set_page_config(page_title="Hydrofetch Dashboard", layout="wide")
    st.title("Hydrofetch 本机监控面板")
    st.caption("面向 job 状态、失败诊断与入库进度的本机 Web UI。")

    with st.sidebar:
        st.header("运行参数")
        job_dir = st.text_input("Job 目录", value=str(DEFAULT_JOB_DIR))
        log_dir = st.text_input("日志目录", value=str(DEFAULT_LOG_DIR))
        refresh_seconds = int(
            st.selectbox("自动刷新", options=[0, 15, 30, 60, 120], index=2)
        )
        page = st.radio("页面", ["总览", "失败", "任务", "入库", "告警"], index=0)
        st.markdown("### 启动命令")
        st.code(
            "uv run --package hydrofetch streamlit run "
            "packages/hydrofetch/scripts/dashboard/app.py --server.port 8501"
        )
        st.markdown("### 访问地址")
        st.code(f"http://localhost:{DEFAULT_PORT}")

    _enable_auto_refresh(refresh_seconds)

    load_result = cached_load_jobs(job_dir)
    jobs_df = load_result.jobs_df
    logs_df = cached_parse_logs(log_dir)
    db_table = (
        jobs_df["db_table"].dropna().iloc[0]
        if not jobs_df.empty and not jobs_df["db_table"].dropna().empty
        else DEFAULT_DB_TABLE
    )

    if load_result.invalid_files:
        st.warning(f"跳过损坏 job 文件 {len(load_result.invalid_files)} 个。")

    status_left, status_right = st.columns([2, 1])
    with status_left:
        st.info(
            f"已读取 `{load_result.total_files}` 个 job 文件，当前目录为 `{load_result.job_dir}`。"
        )
    with status_right:
        latest_update = jobs_df["updated_at"].iloc[0] if not jobs_df.empty else "N/A"
        st.metric("最新 job 更新时间", latest_update)

    if jobs_df.empty:
        st.warning("未读取到任何 job JSON。请检查 `job_dir` 路径。")
        return

    if page == "总览":
        render_overview(jobs_df, logs_df, db_table, job_dir, log_dir)
    elif page == "失败":
        render_failures(jobs_df)
    elif page == "任务":
        render_tasks(jobs_df)
    elif page == "入库":
        render_ingest(jobs_df, db_table)
    else:
        render_alerts(jobs_df, logs_df)

    st.subheader("最近更新任务")
    _render_dataframe(build_recent_updates(jobs_df, limit=30), height=320)

    st.subheader("按日期进度")
    _render_dataframe(build_date_progress(jobs_df).head(60), height=320)


if __name__ == "__main__":
    main()
