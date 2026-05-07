from __future__ import annotations

import lakesource.postgres as postgres


def test_area_quality_exports_resolve():
    assert postgres.ensure_area_quality_table is not None
    assert postgres.ensure_area_anomalies_table is not None
    assert postgres.fetch_area_quality_hylak_ids is not None
    assert postgres.fetch_area_quality_hylak_ids_in_range is not None
    assert postgres.count_area_quality_hylak_ids_in_range is not None
    assert postgres.fetch_atlas_area_chunk is not None
