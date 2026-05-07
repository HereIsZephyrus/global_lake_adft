from __future__ import annotations

from lakesource.config import Backend, SourceConfig

from lakeanalysis.batch import build_batch_reader, build_batch_writer


def test_build_batch_reader_selects_backend_implementation(tmp_path) -> None:
    reader = build_batch_reader(SourceConfig(backend=Backend.PARQUET, data_dir=tmp_path))

    assert reader.__class__.__name__ == "ProviderBatchReader"


def test_build_batch_writer_selects_backend_implementation(tmp_path) -> None:
    writer = build_batch_writer(SourceConfig(backend=Backend.PARQUET, data_dir=tmp_path))

    assert writer.__class__.__name__ == "ProviderBatchWriter"
