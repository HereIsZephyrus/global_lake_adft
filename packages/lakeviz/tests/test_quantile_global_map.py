"""Tests for quantile global map orchestration."""

from __future__ import annotations

from pathlib import Path

from lakeviz.quantile import global_map


class DummyConfig:
    provider = object()
    resolution = 0.5
    output_dir = Path("/tmp/quantile")


def test_plot_quantile_global_maps_calls_standard_renderers(monkeypatch) -> None:
    calls: list[tuple[str, str | None]] = []

    def _record(name: str):
        def _fn(config, *, refresh: bool = False, min_lakes: int = 1):
            assert config is DummyConfig
            assert refresh is True
            assert min_lakes == 3
            calls.append((name, None))
            return Path(f"/tmp/{name}.png")

        return _fn

    def _record_extreme(config, event_type: str, *, refresh: bool = False, min_lakes: int = 1):
        assert config is DummyConfig
        assert refresh is True
        assert min_lakes == 3
        calls.append(("extreme", event_type))
        return Path(f"/tmp/extreme_{event_type}.png")

    def _record_transition(
        config,
        transition_type: str,
        *,
        refresh: bool = False,
        min_lakes: int = 1,
    ):
        assert config is DummyConfig
        assert refresh is True
        assert min_lakes == 3
        calls.append(("transition", transition_type))
        return Path(f"/tmp/transition_{transition_type}.png")

    monkeypatch.setattr(global_map, "plot_extremes_density_map", _record("extremes_density"))
    monkeypatch.setattr(global_map, "plot_extremes_event_density_map", _record("extremes_event_density"))
    monkeypatch.setattr(global_map, "plot_transition_density_map", _record("transition_density"))
    monkeypatch.setattr(global_map, "plot_transition_event_density_map", _record("transition_event_density"))
    monkeypatch.setattr(global_map, "plot_extremes_by_type_map", _record_extreme)
    monkeypatch.setattr(global_map, "plot_transition_by_type_map", _record_transition)

    outputs = global_map.plot_quantile_global_maps(
        DummyConfig,
        refresh=True,
        min_lakes=3,
    )

    assert calls == [
        ("extremes_density", None),
        ("extremes_event_density", None),
        ("transition_density", None),
        ("transition_event_density", None),
        ("extreme", "high"),
        ("extreme", "low"),
        ("transition", "low_to_high"),
        ("transition", "high_to_low"),
    ]
    assert len(outputs) == 8


def test_plot_quantile_global_maps_filters_empty_outputs(monkeypatch) -> None:
    monkeypatch.setattr(global_map, "plot_extremes_density_map", lambda *args, **kwargs: Path())
    monkeypatch.setattr(global_map, "plot_extremes_event_density_map", lambda *args, **kwargs: Path("/tmp/a.png"))
    monkeypatch.setattr(global_map, "plot_transition_density_map", lambda *args, **kwargs: Path())
    monkeypatch.setattr(global_map, "plot_transition_event_density_map", lambda *args, **kwargs: Path("/tmp/b.png"))
    monkeypatch.setattr(global_map, "plot_extremes_by_type_map", lambda *args, **kwargs: Path())
    monkeypatch.setattr(global_map, "plot_transition_by_type_map", lambda *args, **kwargs: Path("/tmp/c.png"))

    outputs = global_map.plot_quantile_global_maps(DummyConfig)

    assert outputs == [Path("/tmp/a.png"), Path("/tmp/b.png"), Path("/tmp/c.png"), Path("/tmp/c.png")]
