import numpy as np
import pytest

from lakeanalysis.hawkes.types import (
    HawkesEventSeries,
    HawkesFitResult,
    HawkesModelSpec,
    TYPE_DRY,
    TYPE_WET,
    TYPE_LABELS,
    LRTResult,
)


class TestConstants:
    def test_type_labels(self):
        assert TYPE_LABELS == ("D", "W")


class TestHawkesEventSeries:
    def test_valid_construction(self):
        es = HawkesEventSeries(
            times=np.array([0.5, 1.0, 1.5]),
            event_types=np.array([TYPE_DRY, TYPE_WET, TYPE_DRY]),
            start_time=0.0,
            end_time=2.0,
        )
        assert es.duration == pytest.approx(2.0)

    def test_start_equals_end_raises(self):
        with pytest.raises(ValueError, match="end_time must be greater"):
            HawkesEventSeries(
                times=np.array([]),
                event_types=np.array([], dtype=int),
                start_time=1.0,
                end_time=1.0,
            )

    def test_ndim_validation_raises(self):
        with pytest.raises(ValueError, match="1-D arrays"):
            HawkesEventSeries(
                times=np.array([[0.5]]),
                event_types=np.array([0]),
                start_time=0.0,
                end_time=1.0,
            )

    def test_length_mismatch_raises(self):
        with pytest.raises(ValueError, match="same length"):
            HawkesEventSeries(
                times=np.array([0.5, 1.0]),
                event_types=np.array([0]),
                start_time=0.0,
                end_time=2.0,
            )

    def test_unsorted_times_raises(self):
        with pytest.raises(ValueError, match="sorted in ascending order"):
            HawkesEventSeries(
                times=np.array([1.0, 0.5]),
                event_types=np.array([0, 1]),
                start_time=0.0,
                end_time=2.0,
            )

    def test_invalid_event_types_raises(self):
        with pytest.raises(ValueError, match="event_types must be in"):
            HawkesEventSeries(
                times=np.array([0.5]),
                event_types=np.array([2]),
                start_time=0.0,
                end_time=2.0,
            )

    def test_end_before_start_raises(self):
        with pytest.raises(ValueError, match="end_time must be greater"):
            HawkesEventSeries(
                times=np.array([0.5]),
                event_types=np.array([0]),
                start_time=1.0,
                end_time=0.5,
            )

    def test_event_times_out_of_range_raises(self):
        with pytest.raises(ValueError, match="event times must be within"):
            HawkesEventSeries(
                times=np.array([2.5]),
                event_types=np.array([0]),
                start_time=0.0,
                end_time=2.0,
            )

    def test_empty_events_accepted(self):
        es = HawkesEventSeries(
            times=np.array([], dtype=float),
            event_types=np.array([], dtype=int),
            start_time=0.0,
            end_time=3.0,
        )
        assert len(es.times) == 0
        assert es.duration == pytest.approx(3.0)

    def test_timeline_and_events_table_optional(self):
        es = HawkesEventSeries(
            times=np.array([0.5]),
            event_types=np.array([0]),
            start_time=0.0,
            end_time=1.0,
            timeline=None,
            events_table=None,
        )
        assert es.timeline is None
        assert es.events_table is None


class TestHawkesModelSpec:
    def test_default_construction(self):
        spec = HawkesModelSpec()
        assert spec.free_alpha_mask.shape == (2, 2)
        assert spec.free_alpha_mask.all()
        assert spec.enforce_stability is True
        assert spec.stability_penalty == 1e6

    def test_invalid_mask_shape_raises(self):
        with pytest.raises(ValueError, match="free_alpha_mask must have shape"):
            HawkesModelSpec(free_alpha_mask=np.ones(3, dtype=bool))

    def test_negative_kernel_window_raises(self):
        with pytest.raises(ValueError, match="kernel_window_years must be positive"):
            HawkesModelSpec(kernel_window_years=-1.0)

    def test_zero_kernel_window_raises(self):
        with pytest.raises(ValueError, match="kernel_window_years must be positive"):
            HawkesModelSpec(kernel_window_years=0.0)


class TestLRTResult:
    def test_construction(self):
        result = LRTResult(
            test_name="test",
            lr_statistic=5.0,
            df=2,
            p_value=0.08,
            significance_level=0.05,
            reject_null=False,
            restricted_log_likelihood=-100.0,
            full_log_likelihood=-97.5,
        )
        assert result.test_name == "test"
        assert result.reject_null is False
        assert result.p_value == 0.08
