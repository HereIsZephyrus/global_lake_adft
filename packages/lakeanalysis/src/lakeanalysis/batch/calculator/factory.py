"""CalculatorFactory: create Calculator by algorithm name."""

from __future__ import annotations

from ..engine import Calculator


class CalculatorFactory:
    _registry: dict[str, type] = {}

    @staticmethod
    def create(algorithm: str, **kwargs) -> Calculator:
        cls = CalculatorFactory._registry.get(algorithm)
        if cls is None:
            _lazy_register(algorithm)
            cls = CalculatorFactory._registry.get(algorithm)
        if cls is None:
            raise ValueError(f"Unknown algorithm: {algorithm!r}")
        return cls(**kwargs)


def _lazy_register(algorithm: str) -> None:
    if algorithm in CalculatorFactory._registry:
        return
    if algorithm == "quantile":
        from .quantile import QuantileCalculator
        CalculatorFactory._registry["quantile"] = QuantileCalculator
    elif algorithm == "pwm_extreme":
        from .pwm_extreme import PWMExtremeCalculator
        CalculatorFactory._registry["pwm_extreme"] = PWMExtremeCalculator
    elif algorithm == "eot":
        from .eot import EOTCalculator
        CalculatorFactory._registry["eot"] = EOTCalculator
    elif algorithm == "comparison":
        from lakeanalysis.comparison.calculator import ComparisonCalculator
        CalculatorFactory._registry["comparison"] = ComparisonCalculator