from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_EVEN, localcontext
from functools import lru_cache
from typing import Any, Iterable

from .config import PlannerConfig
from .errors import PlannerError, invalid_query
from .query import QuerySpec
from .selectivity import PredicateEstimate, SelectivityEstimator
from .snapshot import Snapshot


def _fixed(value: Decimal, quantum: Decimal, precision: int = 60) -> str:
    with localcontext() as context:
        context.prec = precision
        rounded = value.quantize(quantum, rounding=ROUND_HALF_EVEN)
    return format(rounded, "f")


def _fixed_cardinality(value: Decimal) -> str:
    return _fixed(value, Decimal("0.000001"), precision=70)


def _log(value: float) -> float:
    return math.log(max(value, 1e-300))


@dataclass(frozen=True)
class Candidate:
    order: tuple[str, ...]
    raw_total: Decimal
    rendered: dict[str, Any]


class PlanOptimizer:
    def __init__(self, config: PlannerConfig) -> None:
        self.config = config

    def validate(self, snapshot: Snapshot, query: QuerySpec) -> SelectivityEstimator:
        relation_count = len(query.relations)
        if relation_count > self.config.max_relations:
            if relation_count <= 20:
                upper_bound: Any = str(math.factorial(relation_count))
            else:
                digits = math.lgamma(relation_count + 1) / math.log(10)
                upper_bound = f"10^{math.floor(digits)}"
            raise PlannerError(
                "E_SEARCH_SPACE_TOO_LARGE",
                "The query exceeds the configured relation limit; the search space was not truncated.",
                {
                    "relation_count": relation_count,
                    "max_relations": self.config.max_relations,
                    "candidate_scale": {
                        "formula": "n!",
                        "upper_bound": upper_bound,
                    },
                },
            )

        relation_map = query.relation_map()
        for relation in query.relations:
            if relation.table not in snapshot.relations:
                raise invalid_query(
                    "relation table is absent from the bound snapshot",
                    alias=relation.alias,
                    table=relation.table,
                    snapshot_version=snapshot.version,
                )

        estimator = SelectivityEstimator(snapshot, query, self.config)
        for predicate in query.predicates:
            estimator.estimate(predicate)
            if predicate.predicate_type == "self_join":
                assert predicate.right is not None
                left_table = relation_map[predicate.left.relation].table
                right_table = relation_map[predicate.right.relation].table
                if left_table != right_table:
                    raise invalid_query(
                        "self_join must reference the same table through two aliases",
                        predicate_id=predicate.predicate_id,
                        left_table=left_table,
                        right_table=right_table,
                    )

        adjacency = {alias: set() for alias in query.aliases}
        for predicate in query.predicates:
            if len(predicate.relation_aliases) == 2:
                left_alias, right_alias = tuple(predicate.relation_aliases)
                adjacency[left_alias].add(right_alias)
                adjacency[right_alias].add(left_alias)
        visited: set[str] = set()
        pending = [query.aliases[0]]
        while pending:
            alias = pending.pop()
            if alias in visited:
                continue
            visited.add(alias)
            pending.extend(sorted(adjacency[alias] - visited))
        if visited != set(query.aliases):
            missing = sorted(set(query.aliases) - visited)
            raise invalid_query(
                "query relation graph is disconnected; Cartesian products are forbidden",
                missing_relations=missing,
                rule="Every relation alias must be reachable through at least one two-relation predicate edge.",
            )
        return estimator

    def _count_candidate_space(self, query: QuerySpec) -> int:
        aliases = tuple(sorted(query.aliases))
        index = {alias: position for position, alias in enumerate(aliases)}
        adjacency = {alias: set() for alias in aliases}
        for predicate in query.predicates:
            if len(predicate.relation_aliases) == 2:
                left_alias, right_alias = tuple(predicate.relation_aliases)
                adjacency[left_alias].add(right_alias)
                adjacency[right_alias].add(left_alias)
        full_mask = (1 << len(aliases)) - 1

        @lru_cache(maxsize=None)
        def count(mask: int) -> int:
            if mask == full_mask:
                return 1
            total = 0
            for alias in aliases:
                bit = 1 << index[alias]
                if mask & bit:
                    continue
                if mask == 0 or any(mask & (1 << index[neighbor]) for neighbor in adjacency[alias]):
                    total += count(mask | bit)
            return total

        return count(0)

    def _enumerate(self, query: QuerySpec, limit: int | None) -> Iterable[tuple[str, ...]]:
        aliases = tuple(sorted(query.aliases))
        adjacency = {alias: set() for alias in aliases}
        for predicate in query.predicates:
            if len(predicate.relation_aliases) == 2:
                left_alias, right_alias = tuple(predicate.relation_aliases)
                adjacency[left_alias].add(right_alias)
                adjacency[right_alias].add(left_alias)
        emitted = 0
        joined: list[str] = []
        joined_set: set[str] = set()

        def walk() -> Iterable[tuple[str, ...]]:
            nonlocal emitted
            if len(joined) == len(aliases):
                if limit is not None and emitted >= limit:
                    return
                emitted += 1
                yield tuple(joined)
                return
            for alias in aliases:
                if alias in joined_set:
                    continue
                if joined and not (adjacency[alias] & joined_set):
                    continue
                joined.append(alias)
                joined_set.add(alias)
                yield from walk()
                joined_set.remove(alias)
                joined.pop()
                if limit is not None and emitted >= limit:
                    return

        yield from walk()

    def _correlation_adjustments(
        self,
        snapshot: Snapshot,
        query: QuerySpec,
        joined_aliases: tuple[str, ...],
        observed_columns: dict[str, set[str]],
        applied_correlations: set[tuple[str, int]],
    ) -> list[dict[str, Any]]:
        relation_map = query.relation_map()
        adjustments: list[dict[str, Any]] = []
        for alias in joined_aliases:
            table = relation_map[alias].table
            stats = snapshot.relations[table]
            for index, correlation in enumerate(stats.correlations):
                key = (alias, index)
                if key in applied_correlations:
                    continue
                if set(correlation.columns) <= observed_columns[alias]:
                    applied_correlations.add(key)
                    adjustments.append(
                        {
                            "relation": alias,
                            "table": table,
                            "columns": list(correlation.columns),
                            "coefficient": format(Decimal(str(correlation.coefficient)), ".12f"),
                            "source": "declared_correlation",
                        }
                    )
        return adjustments

    def _estimate_order(
        self,
        snapshot: Snapshot,
        query: QuerySpec,
        estimator: SelectivityEstimator,
        order: tuple[str, ...],
    ) -> Candidate:
        relation_map = query.relation_map()
        predicate_by_id = {predicate.predicate_id: predicate for predicate in query.predicates}
        estimates = {identifier: estimator.estimate(predicate) for identifier, predicate in predicate_by_id.items()}
        applied_predicates: set[str] = set()
        applied_correlations: set[tuple[str, int]] = set()
        observed_columns: dict[str, set[str]] = {alias: set() for alias in query.aliases}
        joined_aliases: list[str] = []
        log_cardinality: float | None = None
        upper_bound = Decimal(1)
        previous_cardinality = Decimal(1)
        scan_raw = Decimal(0)
        join_raw = Decimal(0)
        write_raw = Decimal(0)
        steps: list[dict[str, Any]] = []
        correlation_corrections: list[dict[str, Any]] = []
        max_log = math.log(float(self.config.max_cardinality))
        min_log = math.log(float(self.config.min_selectivity))

        for position, alias in enumerate(order):
            relation = relation_map[alias]
            relation_stats = snapshot.relations[relation.table]
            joined_aliases.append(alias)
            joined_set = set(joined_aliases)
            if log_cardinality is None:
                log_cardinality = _log(float(relation_stats.rows))
                upper_bound = Decimal(relation_stats.rows)
            else:
                log_cardinality += _log(float(relation_stats.rows))
                upper_bound = min(
                    self.config.max_cardinality,
                    upper_bound * Decimal(relation_stats.rows),
                )

            newly_available = sorted(
                (
                    predicate
                    for predicate in query.predicates
                    if predicate.predicate_id not in applied_predicates
                    and predicate.relation_aliases <= joined_set
                ),
                key=lambda predicate: predicate.predicate_id,
            )
            step_estimates: list[PredicateEstimate] = []
            log_selectivity = 0.0
            for predicate in newly_available:
                estimate = estimates[predicate.predicate_id]
                step_estimates.append(estimate)
                log_selectivity += _log(float(estimate.selectivity))
                applied_predicates.add(predicate.predicate_id)
                for reference in (predicate.left, predicate.right):
                    if reference is not None:
                        observed_columns[reference.relation].add(reference.column)

            adjustments = self._correlation_adjustments(
                snapshot,
                query,
                tuple(joined_aliases),
                observed_columns,
                applied_correlations,
            )
            for adjustment in adjustments:
                log_selectivity += _log(float(adjustment["coefficient"]))
                adjustment["step"] = position
                correlation_corrections.append(adjustment)

            clamped_selectivity_log = min(0.0, max(min_log, log_selectivity))
            selectivity_bounded = not (min_log <= log_selectivity <= 0.0)
            log_cardinality += clamped_selectivity_log
            cardinality_upper_capped = log_cardinality > max_log
            cardinality_lower_capped = log_cardinality < 0.0
            effective_log_cardinality = min(max_log, max(0.0, log_cardinality))
            cardinality = Decimal(str(math.exp(effective_log_cardinality)))

            scan_component = Decimal(relation_stats.rows) * self.config.scan_unit_cost
            scan_raw += scan_component
            if position == 0:
                join_component = Decimal(0)
                write_component = Decimal(0)
            else:
                join_component = (
                    previous_cardinality + Decimal(relation_stats.rows)
                ) * self.config.join_unit_cost
                write_component = cardinality * self.config.write_unit_cost
                join_raw += join_component
                write_raw += write_component

            steps.append(
                {
                    "step": position + 1,
                    "relation": alias,
                    "table": relation.table,
                    "input_rows": str(relation_stats.rows),
                    "predicates_applied": [estimate.predicate_id for estimate in step_estimates],
                    "predicate_estimates": [estimate.as_dict() for estimate in step_estimates],
                    "correlation_adjustments": adjustments,
                    "combined_step_selectivity": format(
                        Decimal(str(math.exp(clamped_selectivity_log))), ".12f"
                    ),
                    "selectivity_clamped": selectivity_bounded,
                    "log2_estimated_rows": format(Decimal(str(effective_log_cardinality / math.log(2))), ".12f"),
                    "estimated_rows": _fixed_cardinality(cardinality),
                    "finite_upper_bound": _fixed_cardinality(upper_bound),
                    "upper_bound_capped": cardinality_upper_capped,
                    "lower_bound_applied": cardinality_lower_capped,
                    "cost": {
                        "scan": _fixed(scan_component, self.config.cost_quantum),
                        "join": _fixed(join_component, self.config.cost_quantum),
                        "intermediate_write": _fixed(write_component, self.config.cost_quantum),
                    },
                }
            )
            previous_cardinality = cardinality

        missing_predicates = sorted(set(predicate_by_id) - applied_predicates)
        if missing_predicates:
            raise RuntimeError(f"internal predicate application failure: {missing_predicates}")

        total_raw = scan_raw + join_raw + write_raw
        scan_fixed = _fixed(scan_raw, self.config.cost_quantum)
        join_fixed = _fixed(join_raw, self.config.cost_quantum)
        write_fixed = _fixed(write_raw, self.config.cost_quantum)
        comparison_quantum = Decimal("0.000000000001")
        cost = {
            "scan": {
                "fixed": scan_fixed,
                "formula": "sum(relation_rows * scan_unit_cost)",
                "unit_cost": format(self.config.scan_unit_cost, "f"),
            },
            "join": {
                "fixed": join_fixed,
                "formula": "sum((previous_estimated_rows + new_relation_rows) * join_unit_cost) over joins",
                "unit_cost": format(self.config.join_unit_cost, "f"),
            },
            "intermediate_write": {
                "fixed": write_fixed,
                "formula": "sum(estimated_rows_after_each_join * write_unit_cost)",
                "unit_cost": format(self.config.write_unit_cost, "f"),
            },
            "total": {
                "fixed": _fixed(
                    Decimal(scan_fixed) + Decimal(join_fixed) + Decimal(write_fixed),
                    self.config.cost_quantum,
                ),
                "formula": "scan + join + intermediate_write using rounded components",
            },
            "comparison_total": {
                "fixed": _fixed(total_raw, comparison_quantum, precision=80),
                "usage": "Used for relative-tolerance comparison before fixed-point rendering.",
            },
            "rounding": {
                "quantum": format(self.config.cost_quantum, "f"),
                "mode": "ROUND_HALF_EVEN",
                "max_absolute_error_per_component": format(
                    self.config.cost_quantum / Decimal(2), "f"
                ),
                "note": "Each component is quantized independently; total is the sum of the rendered component decimals.",
            },
        }
        predicate_estimates = [estimates[identifier].as_dict() for identifier in sorted(estimates)]
        rendered = {
            "selected_order": list(order),
            "selected_tables": [relation_map[alias].table for alias in order],
            "cost": cost,
            "steps": steps,
            "predicate_estimates": predicate_estimates,
            "correlation_corrections": correlation_corrections,
        }
        return Candidate(order=order, raw_total=total_raw, rendered=rendered)

    def _better_candidate(self, best: Candidate | None, candidate: Candidate) -> Candidate:
        if best is None:
            return candidate
        scale = max(abs(best.raw_total), abs(candidate.raw_total), Decimal("0.000000000001"))
        tolerance = self.config.comparison_relative_tolerance * scale
        if abs(candidate.raw_total - best.raw_total) <= tolerance:
            return candidate if candidate.order < best.order else best
        return candidate if candidate.raw_total < best.raw_total else best

    def plan(self, snapshot: Snapshot, query: QuerySpec, mode: str) -> dict[str, Any]:
        if mode not in {"fast", "exact"}:
            raise invalid_query("mode must be 'fast' or 'exact'", mode=mode)
        estimator = self.validate(snapshot, query)
        candidate_space_size = self._count_candidate_space(query)
        if candidate_space_size == 0:
            raise invalid_query(
                "no legal left-deep order exists for the query",
                reason="Every complete ordering must grow through predicate edges.",
            )
        budget = self.config.fast_candidate_budget if mode == "fast" else None
        evaluated = 0
        best: Candidate | None = None
        for order in self._enumerate(query, budget):
            candidate = self._estimate_order(snapshot, query, estimator, order)
            best = self._better_candidate(best, candidate)
            evaluated += 1
        if best is None:
            raise RuntimeError("candidate enumeration produced no plan")

        pruned_branches = candidate_space_size - evaluated
        plan_payload: dict[str, Any] = {
            "query_id": query.query_id,
            "mode": mode,
            "snapshot": {
                "version": snapshot.version,
                "checksum": snapshot.checksum,
                "generated_at": snapshot.generated_at,
            },
            "search": {
                "strategy": "left_deep",
                "candidate_space_size": candidate_space_size,
                "candidates_evaluated": evaluated,
                "candidate_budget": budget,
                "pruned_branches": pruned_branches,
                "pruned_branches_definition": "complete legal left-deep orders not evaluated because the fast-mode budget was reached",
                "optimality_guarantee": pruned_branches == 0,
                "cartesian_products": False,
            },
            "comparison": {
                "relative_tolerance": format(self.config.comparison_relative_tolerance, "f"),
                "tie_break": "ascending lexicographic selected_order",
                "rule": "Costs within relative tolerance are treated as ties; ties use stable lexicographic order.",
            },
            "assumptions": [
                "Equality selectivity uses 1 / max(column NDVs).",
                "Range selectivity assumes a uniform distribution over the declared column domain.",
                "Predicates are independent unless a snapshot correlation coefficient is applied exactly once in a step.",
                "Correlation-adjusted step selectivity is clamped to (0, 1].",
                "Intermediate cardinalities accumulate in log space and are capped at max_cardinality.",
            ],
            **best.rendered,
        }
        digest_source = {
            key: plan_payload[key]
            for key in (
                "query_id",
                "mode",
                "snapshot",
                "selected_order",
                "search",
                "cost",
                "steps",
                "predicate_estimates",
                "correlation_corrections",
            )
        }
        plan_payload["plan_digest"] = hashlib.sha256(
            json.dumps(
                digest_source,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()
        return plan_payload

