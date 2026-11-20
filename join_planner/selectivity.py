from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal

from .config import PlannerConfig
from .errors import invalid_query
from .query import ColumnRef, Predicate, QuerySpec
from .snapshot import ColumnStats, RelationStats, Snapshot


@dataclass(frozen=True)
class PredicateEstimate:
    predicate_id: str
    predicate_type: str
    selectivity: Decimal
    formula: str
    assumption_source: str
    correlation_declared: bool
    involved_columns: tuple[dict[str, str], ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "predicate_id": self.predicate_id,
            "type": self.predicate_type,
            "selectivity": format(self.selectivity, ".12f"),
            "formula": self.formula,
            "assumption_source": self.assumption_source,
            "correlation_declared": self.correlation_declared,
            "involved_columns": list(self.involved_columns),
        }


def _clamp_selectivity(value: Decimal, floor: Decimal) -> Decimal:
    if value <= 0 or not value.is_finite():
        return floor
    if value > Decimal(1):
        return Decimal(1)
    return value


class SelectivityEstimator:
    def __init__(self, snapshot: Snapshot, query: QuerySpec, config: PlannerConfig) -> None:
        self.snapshot = snapshot
        self.query = query
        self.config = config
        self.relations = query.relation_map()

    def relation_stats(self, alias: str) -> RelationStats:
        relation = self.relations.get(alias)
        if relation is None:
            raise invalid_query("unknown relation alias", alias=alias)
        stats = self.snapshot.relations.get(relation.table)
        if stats is None:
            raise invalid_query(
                "relation table is absent from the bound snapshot",
                alias=alias,
                table=relation.table,
                snapshot_version=self.snapshot.version,
            )
        return stats

    def column_stats(self, ref: ColumnRef) -> ColumnStats:
        stats = self.relation_stats(ref.relation)
        column = stats.columns.get(ref.column)
        if column is None:
            raise invalid_query(
                "predicate column is absent from relation statistics",
                relation=ref.relation,
                table=stats.name,
                column=ref.column,
            )
        return column

    def _involved_columns(self, predicate: Predicate) -> tuple[dict[str, str], ...]:
        columns = [predicate.left.as_dict()]
        if predicate.right is not None:
            columns.append(predicate.right.as_dict())
        return tuple(columns)

    def _correlation_declared(self, predicate: Predicate) -> bool:
        for column_ref in (predicate.left, predicate.right):
            if column_ref is None:
                continue
            stats = self.relation_stats(column_ref.relation)
            for correlation in stats.correlations:
                if column_ref.column in correlation.columns:
                    return True
        return False

    def _range_local_selectivity(self, predicate: Predicate) -> tuple[Decimal, str]:
        column = self.column_stats(predicate.left)
        if column.min_value is None or column.max_value is None:
            raise invalid_query(
                "range predicate requires column min and max statistics",
                predicate_id=predicate.predicate_id,
                column=predicate.left.as_dict(),
            )
        domain_min = Decimal(str(column.min_value))
        domain_max = Decimal(str(column.max_value))
        domain_width = domain_max - domain_min
        lower = Decimal(str(predicate.lower)) if predicate.lower is not None else domain_min
        upper = Decimal(str(predicate.upper)) if predicate.upper is not None else domain_max
        if domain_width <= 0:
            selectivity = Decimal(1) if lower <= domain_min <= upper else self.config.min_selectivity
            return _clamp_selectivity(selectivity, self.config.min_selectivity), (
                "degenerate_domain_match" if selectivity == 1 else "degenerate_domain_miss"
            )
        overlap = max(Decimal(0), min(domain_max, upper) - max(domain_min, lower))
        raw = overlap / domain_width
        return _clamp_selectivity(raw, self.config.min_selectivity), "range_overlap / column_domain_width"

    def _range_join_selectivity(self, predicate: Predicate) -> tuple[Decimal, str]:
        assert predicate.right is not None
        left = self.column_stats(predicate.left)
        right = self.column_stats(predicate.right)
        if None in (left.min_value, left.max_value, right.min_value, right.max_value):
            raise invalid_query(
                "range join requires min and max statistics on both columns",
                predicate_id=predicate.predicate_id,
            )
        assert left.min_value is not None and left.max_value is not None
        assert right.min_value is not None and right.max_value is not None
        left_min, left_max = Decimal(str(left.min_value)), Decimal(str(left.max_value))
        right_min, right_max = Decimal(str(right.min_value)), Decimal(str(right.max_value))
        overlap = max(Decimal(0), min(left_max, right_max) - max(left_min, right_min))
        denominator = max(left_max - left_min, right_max - right_min)
        if denominator <= 0:
            raw = Decimal(1) if left_min == right_min else self.config.min_selectivity
        else:
            raw = overlap / denominator
        return _clamp_selectivity(raw, self.config.min_selectivity), "domain_overlap / max_domain_width"

    def estimate(self, predicate: Predicate) -> PredicateEstimate:
        if predicate.predicate_type == "equality":
            left = self.column_stats(predicate.left)
            if predicate.right is not None:
                right = self.column_stats(predicate.right)
                selectivity = Decimal(1) / Decimal(max(left.ndv, right.ndv))
                formula = "1 / max(left_ndv, right_ndv)"
            else:
                selectivity = Decimal(1) / Decimal(left.ndv)
                formula = "1 / left_ndv"
        elif predicate.predicate_type == "self_join":
            assert predicate.right is not None
            left = self.column_stats(predicate.left)
            right = self.column_stats(predicate.right)
            selectivity = Decimal(1) / Decimal(max(left.ndv, right.ndv))
            formula = "1 / max(outer_ndv, inner_ndv)"
        elif predicate.predicate_type == "range":
            if predicate.right is not None:
                selectivity, formula = self._range_join_selectivity(predicate)
            else:
                selectivity, formula = self._range_local_selectivity(predicate)
        else:
            raise invalid_query(
                "unsupported predicate type",
                predicate_id=predicate.predicate_id,
                predicate_type=predicate.predicate_type,
            )

        selectivity = _clamp_selectivity(selectivity, self.config.min_selectivity)
        correlation_declared = self._correlation_declared(predicate)
        return PredicateEstimate(
            predicate_id=predicate.predicate_id,
            predicate_type=predicate.predicate_type,
            selectivity=selectivity,
            formula=formula,
            assumption_source=(
                "declared_correlation_pending_compound_correction"
                if correlation_declared
                else "independence_assumption"
            ),
            correlation_declared=correlation_declared,
            involved_columns=self._involved_columns(predicate),
        )

    @staticmethod
    def selectivity_float(estimate: PredicateEstimate) -> float:
        value = float(estimate.selectivity)
        if not math.isfinite(value) or value <= 0:
            raise ValueError("selectivity must be finite and positive")
        return min(1.0, max(float(Decimal("1e-300")), value))

