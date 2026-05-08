"""Monthly time-series primitives for EOT modelling."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd

TailDirection = Literal["high", "low"]
MIN_OBSERVATIONS = 20


@dataclass(frozen=True)
class MonthlyTimeSeries:
    """Monthly water-area series represented on a continuous time axis."""

    data: pd.DataFrame
    value_column: str = "water_area"
    direction: TailDirection = "high"

    @classmethod
    def from_frame(
        cls,
        df: pd.DataFrame,
        value_column: str = "water_area",
    ) -> "MonthlyTimeSeries":
        """Create a validated monthly time series from a lake-area frame.

        Args:
            df: Input frame with columns year, month and the value column.
            value_column: Column containing the monthly measurement.

        Returns:
            A validated ``MonthlyTimeSeries`` on the original high-tail scale.
        """
        required = {"year", "month", value_column}
        missing = required.difference(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {sorted(missing)}")

        frame = (
            df.loc[:, ["year", "month", value_column]]
            .dropna(subset=["year", "month", value_column])
            .copy()
        )
        if frame.empty:
            raise ValueError("Input frame is empty after dropping missing values")

        frame["year"] = frame["year"].astype(int)
        frame["month"] = frame["month"].astype(int)
        frame["original_value"] = frame[value_column].astype(float)
        frame = frame.sort_values(["year", "month"]).reset_index(drop=True)

        invalid_months = frame.loc[~frame["month"].between(1, 12), "month"]
        if not invalid_months.empty:
            raise ValueError("Month values must be in the range 1..12")

        start_year = int(frame["year"].min())
        frame["time"] = (
            (frame["year"] - start_year).astype(float)
            + (frame["month"].astype(float) - 1.0) / 12.0
        )
        frame["value"] = frame["original_value"].astype(float)

        return cls(
            data=frame.loc[
                :, ["year", "month", "time", "value", "original_value"]
            ].reset_index(drop=True),
            value_column=value_column,
            direction="high",
        )

    def for_tail(self, direction: TailDirection) -> "MonthlyTimeSeries":
        """Return a view transformed for high- or low-tail modelling."""
        if direction == self.direction:
            return self

        frame = self.data.copy()
        if direction == "high":
            frame["value"] = frame["original_value"]
        else:
            # Negate working value, not original_value, to preserve any preprocessing
            frame["value"] = -frame["value"]

        return MonthlyTimeSeries(
            data=frame,
            value_column=self.value_column,
            direction=direction,
        )

    def defrozen(self, frozen_year_months: set[int] | None = None) -> "MonthlyTimeSeries":
        """Return a series with frozen-month observations removed.

        Args:
            frozen_year_months: Set of YYYYMM integers flagged as frozen in the
                anomaly table. ``None`` or an empty set leaves the series unchanged.

        Returns:
            A new ``MonthlyTimeSeries`` with frozen months removed while preserving
            the original continuous time coordinates of the remaining observations.
        """
        if not frozen_year_months:
            return self

        from lakeanalysis.quality.frozen import defrozen_frame

        result = defrozen_frame(self.data, frozen_year_months)
        return MonthlyTimeSeries(
            data=result,
            value_column=self.value_column,
            direction=self.direction,
        )

    def validate_min_observations(
        self,
        min_observations: int = MIN_OBSERVATIONS,
    ) -> "MonthlyTimeSeries":
        """Validate that the series contains enough observations for EOT fitting."""
        if self.n_obs < min_observations:
            raise ValueError(
                f"At least {min_observations} observations are required after preprocessing; "
                f"got {self.n_obs}"
            )
        return self

    @property
    def n_obs(self) -> int:
        """Return the number of monthly observations."""
        return len(self.data)

    @property
    def duration_years(self) -> float:
        """Return the observed time span in years."""
        if self.data.empty:
            return 0.0
        times = self.data["time"].to_numpy(dtype=float)
        return float(times.max() - times.min() + 1.0 / 12.0)

    @property
    def values(self) -> np.ndarray:
        """Return the working values used for tail modelling."""
        return self.data["value"].to_numpy(dtype=float)

    @property
    def original_values(self) -> np.ndarray:
        """Return the untransformed values."""
        return self.data["original_value"].to_numpy(dtype=float)


__all__ = ["TailDirection", "MIN_OBSERVATIONS", "MonthlyTimeSeries"]
