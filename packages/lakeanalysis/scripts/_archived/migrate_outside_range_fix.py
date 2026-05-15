"""Fix outside_range anomaly flag (unit mismatch: m² vs km²).

Steps:
  1. Create temp table, fill in chunks (lake_area GROUP BY per chunk)
  2. Update area_anomalies.anomaly_flags: clear bit 3, set from temp table
  3. Move rows with anomaly_flags=0 from area_anomalies to area_quality
  4. Move rows from area_quality where is_outside_range=TRUE to area_anomalies
  5. Drop temp table

Usage:
    uv run python scripts/migrate_outside_range_fix.py --dry-run
    uv run python scripts/migrate_outside_range_fix.py
    uv run python scripts/migrate_outside_range_fix.py --chunk-size 5000
"""

from __future__ import annotations

import argparse
import logging
import time

from lakesource.env import load_env
from lakesource.postgres import series_db
from lakeanalysis.logger import Logger

log = logging.getLogger(__name__)

FLAG_OUTSIDE_RANGE = 8
CHUNK_SIZE = 10_000


def _get_max_hylak_id(cur) -> int:
    cur.execute("SELECT COALESCE(MAX(hylak_id), 0) FROM lake_info")
    return int(cur.fetchone()[0])


def _build_temp_table(cur, chunk_size: int) -> None:
    max_id = _get_max_hylak_id(cur)
    n_chunks = (max_id + chunk_size) // chunk_size
    total_inserted = 0
    start = time.time()

    for i in range(n_chunks):
        cs = i * chunk_size
        ce = cs + chunk_size
        cur.execute(
            """
            INSERT INTO tmp_outside_range_fix (hylak_id, is_outside_range)
            SELECT li.hylak_id,
                   CASE WHEN li.lake_area <= 0 THEN FALSE
                        ELSE li.lake_area < la.min_km2 OR li.lake_area > la.max_km2
                   END
            FROM lake_info li
            JOIN (
                SELECT hylak_id, MIN(water_area) / 1e6 AS min_km2, MAX(water_area) / 1e6 AS max_km2
                FROM lake_area
                WHERE hylak_id >= %s AND hylak_id < %s
                GROUP BY hylak_id
            ) la ON li.hylak_id = la.hylak_id
            WHERE li.hylak_id >= %s AND li.hylak_id < %s
            """,
            (cs, ce, cs, ce),
        )
        total_inserted += cur.rowcount or 0
        if (i + 1) % 20 == 0 or i == n_chunks - 1:
            elapsed = time.time() - start
            log.info(
                "  [%d/%d] inserted %d rows (%.1fs)",
                i + 1, n_chunks, total_inserted, elapsed,
            )

    cur.execute("SELECT COUNT(*), SUM(CASE WHEN is_outside_range THEN 1 ELSE 0 END) FROM tmp_outside_range_fix")
    total, n_outside = cur.fetchone()
    log.info(
        "Temp table complete: %d lakes, %d outside_range (%.1f%%)",
        total, n_outside, n_outside / total * 100 if total else 0,
    )


def _update_anomalies_flags(cur) -> None:
    log.info("Step 2: Update area_anomalies.anomaly_flags")
    cur.execute(
        """
        UPDATE area_anomalies aa
        SET anomaly_flags = (aa.anomaly_flags & ~{bit})
                          | (CASE WHEN t.is_outside_range THEN {bit} ELSE 0 END)
        FROM tmp_outside_range_fix t
        WHERE aa.hylak_id = t.hylak_id
        """.format(bit=FLAG_OUTSIDE_RANGE),
    )
    log.info("  Updated %d rows", cur.rowcount or 0)

    cur.execute("SELECT COUNT(*) FROM area_anomalies WHERE anomaly_flags = 0")
    n_zero = cur.fetchone()[0]
    log.info("  Rows with anomaly_flags=0: %d", n_zero)


def _move_flags_zero_to_quality(cur) -> None:
    log.info("Step 3: Move anomaly_flags=0 rows to area_quality")
    cur.execute(
        """
        INSERT INTO area_quality (hylak_id, rs_area_mean, rs_area_median, atlas_area)
        SELECT hylak_id, rs_area_mean, rs_area_median, atlas_area
        FROM area_anomalies
        WHERE anomaly_flags = 0
        ON CONFLICT (hylak_id) DO UPDATE SET
            rs_area_mean = EXCLUDED.rs_area_mean,
            rs_area_median = EXCLUDED.rs_area_median,
            atlas_area = EXCLUDED.atlas_area
        """,
    )
    log.info("  Inserted into area_quality: %d", cur.rowcount or 0)

    cur.execute("DELETE FROM area_anomalies WHERE anomaly_flags = 0")
    log.info("  Deleted from area_anomalies: %d", cur.rowcount or 0)


def _move_quality_outside_to_anomalies(cur) -> None:
    log.info("Step 4: Move area_quality rows with is_outside_range=TRUE to area_anomalies")
    cur.execute(
        """
        SELECT COUNT(*)
        FROM area_quality aq
        JOIN tmp_outside_range_fix t ON aq.hylak_id = t.hylak_id
        WHERE t.is_outside_range = TRUE
        """,
    )
    n_move = cur.fetchone()[0]
    log.info("  Rows to move: %d", n_move)

    cur.execute(
        """
        INSERT INTO area_anomalies (hylak_id, rs_area_mean, rs_area_median, atlas_area, anomaly_flags)
        SELECT aq.hylak_id, aq.rs_area_mean, aq.rs_area_median, aq.atlas_area, {bit}
        FROM area_quality aq
        JOIN tmp_outside_range_fix t ON aq.hylak_id = t.hylak_id
        WHERE t.is_outside_range = TRUE
        ON CONFLICT (hylak_id) DO NOTHING
        """.format(bit=FLAG_OUTSIDE_RANGE),
    )
    log.info("  Inserted into area_anomalies: %d", cur.rowcount or 0)

    cur.execute(
        """
        DELETE FROM area_quality
        WHERE hylak_id IN (
            SELECT aq.hylak_id FROM area_quality aq
            JOIN tmp_outside_range_fix t ON aq.hylak_id = t.hylak_id
            WHERE t.is_outside_range = TRUE
        )
        """,
    )
    log.info("  Deleted from area_quality: %d", cur.rowcount or 0)


def _print_final_stats(cur) -> None:
    cur.execute("SELECT COUNT(*) FROM area_anomalies")
    n_anomalies = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM area_quality")
    n_quality = cur.fetchone()[0]
    cur.execute(
        """
        SELECT (anomaly_flags & 8) > 0, COUNT(*)
        FROM area_anomalies
        GROUP BY 1 ORDER BY 1
        """,
    )
    or_counts = cur.fetchall()
    log.info("Final: area_anomalies=%d, area_quality=%d", n_anomalies, n_quality)
    for is_or, cnt in or_counts:
        label = "outside_range=T" if is_or else "outside_range=F"
        log.info("  %s: %d", label, cnt)


def run(chunk_size: int = CHUNK_SIZE, dry_run: bool = False) -> None:
    load_env()

    with series_db.connection_context() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            log.info("Step 1: Create temp table tmp_outside_range_fix (chunk_size=%d)", chunk_size)
            cur.execute("DROP TABLE IF EXISTS tmp_outside_range_fix")
            cur.execute(
                "CREATE TEMP TABLE tmp_outside_range_fix "
                "(hylak_id INT PRIMARY KEY, is_outside_range BOOLEAN)"
            )

            _build_temp_table(cur, chunk_size)

            conn.autocommit = False
            _update_anomalies_flags(cur)
            _move_flags_zero_to_quality(cur)
            _move_quality_outside_to_anomalies(cur)

            log.info("Step 5: Cleanup")
            cur.execute("DROP TABLE IF EXISTS tmp_outside_range_fix")

            _print_final_stats(cur)

            if dry_run:
                log.info("DRY RUN - rolling back all changes")
                conn.rollback()
            else:
                conn.commit()
                log.info("All changes committed")


def main() -> None:
    parser = argparse.ArgumentParser(description="Fix outside_range anomaly flag (unit mismatch).")
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE, metavar="N",
                        help="Chunk size for temp table build (default: 10000).")
    parser.add_argument("--dry-run", action="store_true", help="Roll back all changes after preview.")
    args = parser.parse_args()

    Logger("migrate_outside_range_fix")
    run(chunk_size=args.chunk_size, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
