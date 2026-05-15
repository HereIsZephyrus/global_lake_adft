"""Tests for PWM global map orchestration."""

from __future__ import annotations

from pathlib import Path

from lakeviz.pwm import global_map


class DummyConfig:
    provider = object()
    resolution = 0.5
    output_dir = Path("/tmp/pwm")


def test_plot_pwm_global_maps_calls_standard_renderers(monkeypatch) -> None:
    calls: list[str] = []

    def _record(name: str):
        def _fn(config, *, refresh: bool = False, min_lakes: int = 1):
            assert config is DummyConfig
            assert refresh is True
            assert min_lakes == 3
            calls.append(name)
            return Path(f"/tmp/{name}.png")

        return _fn

    monkeypatch.setattr(global_map, "plot_pwm_wet_grid_map", _record("wet_grid"))
    monkeypatch.setattr(global_map, "plot_pwm_dry_grid_map", _record("dry_grid"))
    monkeypatch.setattr(global_map, "plot_pwm_high_exceedance_density_map", _record("wet_kde"))
    monkeypatch.setattr(global_map, "plot_pwm_low_exceedance_density_map", _record("dry_kde"))

    outputs = global_map.plot_pwm_global_maps(DummyConfig, refresh=True, min_lakes=3)

    assert calls == ["wet_grid", "dry_grid", "wet_kde", "dry_kde"]
    assert len(outputs) == 4


def test_plot_pwm_global_maps_filters_empty_outputs(monkeypatch) -> None:
    monkeypatch.setattr(global_map, "plot_pwm_wet_grid_map", lambda *args, **kwargs: Path())
    monkeypatch.setattr(global_map, "plot_pwm_dry_grid_map", lambda *args, **kwargs: Path("/tmp/dry.png"))
    monkeypatch.setattr(global_map, "plot_pwm_high_exceedance_density_map", lambda *args, **kwargs: Path())
    monkeypatch.setattr(global_map, "plot_pwm_low_exceedance_density_map", lambda *args, **kwargs: Path("/tmp/kde.png"))

    outputs = global_map.plot_pwm_global_maps(DummyConfig)

    assert outputs == [Path("/tmp/dry.png"), Path("/tmp/kde.png")]
