"""Row shapers for EOT outputs."""

from __future__ import annotations


def return_levels_to_rows(
    hylak_id: int,
    threshold_quantile: float,
    rows_by_tail: dict[str, list[dict]],
) -> list[dict]:
    rows: list[dict] = []
    for tail, tail_rows in rows_by_tail.items():
        for row in tail_rows:
            rows.append(
                {
                    "hylak_id": int(hylak_id),
                    "tail": str(tail),
                    "threshold_quantile": float(threshold_quantile),
                    "return_period_years": float(row["return_period_years"]),
                    "return_level": float(row["return_level"]),
                    "standard_error": _maybe_float(row.get("standard_error")),
                    "ci_lower": _maybe_float(row.get("ci_lower")),
                    "ci_upper": _maybe_float(row.get("ci_upper")),
                }
            )
    return rows


def _maybe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    if value != value:
        return None
    return value
