from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class PlannerConfig:
    max_relations: int = 8
    fast_candidate_budget: int = 4
    comparison_relative_tolerance: Decimal = Decimal("1e-9")
    cost_quantum: Decimal = Decimal("0.000001")
    scan_unit_cost: Decimal = Decimal("0.002000")
    join_unit_cost: Decimal = Decimal("0.010000")
    write_unit_cost: Decimal = Decimal("0.004000")
    max_cardinality: Decimal = Decimal("1000000000000000000")
    min_selectivity: Decimal = Decimal("0.000000000001")
    build_delay_seconds: float = 0.0

    def __post_init__(self) -> None:
        if self.max_relations < 1:
            raise ValueError("max_relations must be positive")
        if self.fast_candidate_budget < 1:
            raise ValueError("fast_candidate_budget must be positive")
        if self.comparison_relative_tolerance < 0:
            raise ValueError("comparison tolerance cannot be negative")
        if self.cost_quantum <= 0:
            raise ValueError("cost_quantum must be positive")
        if self.max_cardinality < 1:
            raise ValueError("max_cardinality must be at least one")
        if not (Decimal("0") < self.min_selectivity <= Decimal("1")):
            raise ValueError("min_selectivity must be in (0, 1]")

