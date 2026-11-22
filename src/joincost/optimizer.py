from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Iterable

from .errors import (
    BusinessError,
    E_INPUT_INVALID,
    E_NO_CONNECTED_PLAN,
    E_SEARCH_SPACE_TOO_LARGE,
)
from .fixed import (
    CARD_SCALE,
    COST_SCALE,
    MIN_SELECTIVITY,
    RATIO_SCALE,
    format_decimal,
    format_fixed,
    log2_to_card_fixed,
    tolerance_is_close,
)
from .model import (
    MAX_SEARCH_RELATIONS,
    PlanOptions,
    Predicate,
    RelationStats,
    StatsSnapshot,
    parse_predicate,
)


@dataclass(frozen=True)
class SelectivityEstimate:
    predicate: Predicate
    base_fixed: int
    correlation_fixed: int
    selectivity_fixed: int
    method: str
    ndv_a: int | None = None
    ndv_b: int | None = None
    correction_source: str = "statistics"

    def public_dict(self) -> dict[str, Any]:
        result = self.predicate.public_dict()
        result.update(
            {
                "method": self.method,
                "base_selectivity": format_fixed(self.base_fixed, RATIO_SCALE, 9),
                "correlation_correction": format_fixed(
                    self.correlation_fixed, RATIO_SCALE, 9
                ),
                "selectivity": format_fixed(self.selectivity_fixed, RATIO_SCALE, 9),
                "selectivity_fixed": str(self.selectivity_fixed),
                "range_check": {
                    "lower_exclusive": "0.000000000",
                    "upper_inclusive": "1.000000000",
                    "valid": 0 < self.selectivity_fixed <= RATIO_SCALE,
                },
                "correction_source": self.correction_source,
            }
        )
        if self.ndv_a is not None:
            result["ndv_a"] = self.ndv_a
        if self.ndv_b is not None:
            result["ndv_b"] = self.ndv_b
        return result


@dataclass(frozen=True)
class SubsetCardinality:
    mask: int
    log2_rows: float | None
    card_fixed: int
    upper_bound_rows: int
    capped: bool
    zero: bool
    applied_predicate_ids: tuple[str, ...]

    def public_rows(self) -> str:
        return format_fixed(self.card_fixed, CARD_SCALE, 6)


def _ndv_count(relation: RelationStats) -> int:
    if relation.row_count <= 0:
        return 1
    product = relation.row_count * relation.ndv_ratio_fixed
    rounded = (product + RATIO_SCALE // 2) // RATIO_SCALE
    return max(1, int(rounded))


def _clamp_selectivity(value: int) -> int:
    return max(MIN_SELECTIVITY, min(RATIO_SCALE, value))


def _apply_correlation(selectivity: int, correlation: int) -> int:
    corrected = (selectivity * correlation + RATIO_SCALE // 2) // RATIO_SCALE
    return _clamp_selectivity(corrected)


def _predicate_relation_ids(
    predicate: Predicate, index_by_name: dict[str, int]
) -> tuple[int, ...]:
    ids = []
    for name in predicate.relations:
        if name not in index_by_name:
            raise BusinessError(
                E_INPUT_INVALID,
                "predicate references an unknown relation",
                {"predicate": predicate.id, "relation": name},
            )
        ids.append(index_by_name[name])
    return tuple(sorted(set(ids)))


def _correlation_for_predicate(
    predicate: Predicate,
    relation_a: RelationStats,
    relation_b: RelationStats | None,
) -> tuple[int, str]:
    if predicate.correlation_fixed is not None:
        return predicate.correlation_fixed, "predicate"
    if relation_b is not None:
        return min(relation_a.correlation_fixed, relation_b.correlation_fixed), "statistics_min"
    return relation_a.correlation_fixed, "statistics"


def estimate_selectivities(
    snapshot: StatsSnapshot, predicates: list[Predicate]
) -> list[SelectivityEstimate]:
    relations = snapshot.by_name()
    estimates: list[SelectivityEstimate] = []
    seen_ids: set[str] = set()
    for predicate in predicates:
        if predicate.id in seen_ids:
            raise BusinessError(
                E_INPUT_INVALID,
                "duplicate predicate id",
                {"predicate": predicate.id},
            )
        seen_ids.add(predicate.id)
        relation_a = relations.get(predicate.relation_a)
        if relation_a is None:
            raise BusinessError(
                E_INPUT_INVALID,
                "predicate references an unknown relation",
                {"predicate": predicate.id, "relation": predicate.relation_a},
            )
        relation_b = relations.get(predicate.relation_b) if predicate.relation_b else None
        if predicate.relation_b is not None and relation_b is None:
            raise BusinessError(
                E_INPUT_INVALID,
                "predicate references an unknown relation",
                {"predicate": predicate.id, "relation": predicate.relation_b},
            )
        correlation, correction_source = _correlation_for_predicate(
            predicate, relation_a, relation_b
        )
        ndv_a: int | None = None
        ndv_b: int | None = None
        if predicate.kind == "equality":
            if predicate.relation_b is None or predicate.column_b is None:
                raise BusinessError(
                    E_INPUT_INVALID,
                    "equality predicate must reference two relations",
                    {"predicate": predicate.id},
                )
            ndv_a = _ndv_count(relation_a)
            ndv_b = _ndv_count(relation_b) if relation_b is not None else 1
            base = RATIO_SCALE // max(ndv_a, ndv_b, 1)
            method = "equality: 1 / max(ndv_left, ndv_right)"
        elif predicate.kind == "self_join":
            if predicate.column_b is None:
                raise BusinessError(
                    E_INPUT_INVALID,
                    "self_join predicate requires two columns",
                    {"predicate": predicate.id},
                )
            ndv_a = _ndv_count(relation_a)
            base = RATIO_SCALE // max(ndv_a, 1)
            method = "self_join: 1 / ndv"
        elif predicate.kind == "range":
            if predicate.range_fraction_permille is None:
                raise BusinessError(
                    E_INPUT_INVALID,
                    "range predicate is missing range_fraction_permille",
                    {"predicate": predicate.id},
                )
            base = predicate.range_fraction_permille * (RATIO_SCALE // 1000)
            if predicate.relation_b is not None:
                ndv_a = _ndv_count(relation_a)
                ndv_b = _ndv_count(relation_b) if relation_b is not None else 1
                base = (base * (RATIO_SCALE // max(ndv_a, ndv_b, 1))) // RATIO_SCALE
                method = "range_join: fraction / max(ndv_left, ndv_right)"
            else:
                method = "range_filter: declared fraction"
        else:
            raise BusinessError(
                E_INPUT_INVALID,
                "unsupported predicate kind",
                {"predicate": predicate.id, "kind": predicate.kind},
            )
        base = _clamp_selectivity(base)
        selectivity = _apply_correlation(base, correlation)
        estimates.append(
            SelectivityEstimate(
                predicate=predicate,
                base_fixed=base,
                correlation_fixed=correlation,
                selectivity_fixed=selectivity,
                method=method,
                ndv_a=ndv_a,
                ndv_b=ndv_b,
                correction_source=correction_source,
            )
        )
    return estimates


def _validate_predicates(
    snapshot: StatsSnapshot, predicates: list[Predicate]
) -> tuple[dict[str, int], list[tuple[int, ...]], set[tuple[int, int]]]:
    names = [relation.name for relation in snapshot.relations]
    index_by_name = {name: index for index, name in enumerate(names)}
    relation_sets: list[tuple[int, ...]] = []
    edges: set[tuple[int, int]] = set()
    for predicate in predicates:
        ids = _predicate_relation_ids(predicate, index_by_name)
        relation_sets.append(ids)
        if predicate.kind in {"equality", "range"} and len(ids) == 2:
            edges.add((ids[0], ids[1]))
            edges.add((ids[1], ids[0]))
        if predicate.kind == "equality" and len(ids) != 2:
            raise BusinessError(
                E_INPUT_INVALID,
                "equality predicate must connect two distinct relations",
                {"predicate": predicate.id},
            )
        if predicate.kind == "range" and len(ids) > 2:
            raise BusinessError(
                E_INPUT_INVALID,
                "range predicate can involve at most two relations",
                {"predicate": predicate.id},
            )
    return index_by_name, relation_sets, edges


def _selectivities_by_predicate(
    estimates: list[SelectivityEstimate],
) -> dict[str, SelectivityEstimate]:
    return {estimate.predicate.id: estimate for estimate in estimates}


def _compute_subset(
    mask: int,
    snapshot: StatsSnapshot,
    estimates_by_predicate: dict[str, SelectivityEstimate],
    predicate_relation_ids: dict[str, tuple[int, ...]],
) -> SubsetCardinality:
    log2_rows: float | None = 0.0
    applied: list[str] = []
    for index, relation in enumerate(snapshot.relations):
        if mask & (1 << index):
            if relation.row_count == 0:
                log2_rows = None
            elif log2_rows is not None:
                log2_rows += math.log2(relation.row_count)
    for predicate_id in sorted(estimates_by_predicate):
        ids = predicate_relation_ids[predicate_id]
        if not ids:
            continue
        required_mask = 0
        for relation_id in ids:
            required_mask |= 1 << relation_id
        if required_mask & ~mask == 0:
            applied.append(predicate_id)
            if log2_rows is not None:
                selectivity = estimates_by_predicate[predicate_id].selectivity_fixed
                log2_rows += math.log2(selectivity / RATIO_SCALE)
    card_fixed, upper_bound_rows, capped, zero = log2_to_card_fixed(log2_rows)
    return SubsetCardinality(
        mask=mask,
        log2_rows=log2_rows,
        card_fixed=card_fixed,
        upper_bound_rows=upper_bound_rows,
        capped=capped,
        zero=zero,
        applied_predicate_ids=tuple(applied),
    )


def _scan_cost(snapshot: StatsSnapshot) -> int:
    return sum(relation.row_count * COST_SCALE for relation in snapshot.relations)


def _candidate_cost(
    sequence: tuple[int, ...],
    subset_cache: dict[int, SubsetCardinality],
    scan_cost: int,
) -> tuple[int, int, int]:
    join_cost = 0
    write_cost = 0
    mask = 0
    previous = subset_cache[1 << sequence[0]]
    mask |= 1 << sequence[0]
    for relation_id in sequence[1:]:
        bit = 1 << relation_id
        right = subset_cache[bit]
        mask |= bit
        output = subset_cache[mask]
        join_cost += previous.card_fixed + right.card_fixed
        write_cost += output.card_fixed
        previous = output
    return scan_cost + join_cost + write_cost, join_cost, write_cost


def _plan_details(
    *,
    request_id: str,
    snapshot: StatsSnapshot,
    predicates: list[Predicate],
    estimates: list[SelectivityEstimate],
    subset_cache: dict[int, SubsetCardinality],
    sequence: tuple[int, ...],
    scan_cost: int,
    join_cost: int,
    write_cost: int,
    total_cost: int,
    search: dict[str, Any],
    tolerance_selection: dict[str, Any],
) -> dict[str, Any]:
    by_id = _selectivities_by_predicate(estimates)
    steps: list[dict[str, Any]] = []
    mask = 0
    previous: SubsetCardinality | None = None
    previous_applied: set[str] = set()
    cumulative_join = 0
    cumulative_write = 0
    for ordinal, relation_id in enumerate(sequence, start=1):
        relation = snapshot.relations[relation_id]
        bit = 1 << relation_id
        mask |= bit
        current = subset_cache[mask]
        applied = set(current.applied_predicate_ids)
        new_predicates = sorted(applied - previous_applied)
        step: dict[str, Any] = {
            "ordinal": ordinal,
            "relation": relation.name,
            "prefix": [snapshot.relations[index].name for index in sequence[:ordinal]],
            "applied_predicates": [
                {
                    "id": predicate_id,
                    "selectivity": format_fixed(
                        by_id[predicate_id].selectivity_fixed, RATIO_SCALE, 9
                    ),
                }
                for predicate_id in new_predicates
            ],
            "log2_cardinality": (
                format_decimal(current.log2_rows, 12)
                if current.log2_rows is not None
                else None
            ),
            "estimated_rows": current.public_rows(),
            "upper_bound_rows": current.upper_bound_rows,
            "finite_upper_bound": True,
            "overflow_capped": current.capped,
        }
        if previous is None:
            step["join_cost"] = "0.000000"
            step["intermediate_write_cost"] = "0.000000"
        else:
            step_join = previous.card_fixed + subset_cache[bit].card_fixed
            step_write = current.card_fixed
            cumulative_join += step_join
            cumulative_write += step_write
            step["join_input_left_rows"] = previous.public_rows()
            step["join_input_right_rows"] = subset_cache[bit].public_rows()
            step["join_cost"] = format_fixed(step_join, COST_SCALE, 6)
            step["intermediate_write_cost"] = format_fixed(step_write, COST_SCALE, 6)
        step["cumulative_join_cost"] = format_fixed(cumulative_join, COST_SCALE, 6)
        step["cumulative_intermediate_write_cost"] = format_fixed(
            cumulative_write, COST_SCALE, 6
        )
        steps.append(step)
        previous = current
        previous_applied = applied
    stale_relations = [
        {"relation": relation.name, "stale_keys": list(relation.stale_keys)}
        for relation in snapshot.relations
        if relation.stale_keys
    ]
    scan_component = format_fixed(scan_cost, COST_SCALE, 6)
    join_component = format_fixed(join_cost, COST_SCALE, 6)
    write_component = format_fixed(write_cost, COST_SCALE, 6)
    total_component = format_fixed(total_cost, COST_SCALE, 6)
    return {
        "ok": True,
        "request_id": request_id,
        "snapshot_version": snapshot.version,
        "snapshot_digest": snapshot.digest,
        "ordered_relations": [snapshot.relations[index].name for index in sequence],
        "plan": {
            "ordered_relations": [snapshot.relations[index].name for index in sequence],
            "steps": steps,
        },
        "predicate_selectivity": [
            by_id[predicate.id].public_dict() for predicate in predicates
        ],
        "cost": {
            "formula": {
                "scan": "sum(base_row_count)",
                "join": "sum(left_input_estimated_rows + right_input_estimated_rows)",
                "intermediate_write": "sum(joined_estimated_rows)",
            },
            "scan": scan_component,
            "join": join_component,
            "intermediate_write": write_component,
            "total": total_component,
            "scan_fixed": str(scan_cost),
            "join_fixed": str(join_cost),
            "intermediate_write_fixed": str(write_cost),
            "total_fixed": str(total_cost),
            "fixed_scale": str(COST_SCALE),
            "rounding_note": (
                "Cardinalities are accumulated in log2 space and rounded half-even to "
                "six row decimals. Cost components are integer micro-units at scale "
                "1e6; no floating-point cost arithmetic is used."
            ),
        },
        "search": search,
        "tolerance_contract": tolerance_selection,
        "stale_statistics": stale_relations,
        "version_binding": {
            "bound_snapshot_version": snapshot.version,
            "bound_snapshot_digest": snapshot.digest,
            "mixed_versions_allowed": False,
        },
        "determinism": {
            "fixed_scale": str(COST_SCALE),
            "tie_break": "relative tolerance, then lexicographic relation-name sequence",
            "same_snapshot_and_request_is_bit_identical": True,
        },
    }


def build_plan(
    snapshot: StatsSnapshot,
    predicates: Iterable[Predicate | dict[str, Any]],
    options: PlanOptions | dict[str, Any] | None = None,
    *,
    request_id: str = "inline",
) -> dict[str, Any]:
    if options is None:
        plan_options = PlanOptions()
    elif isinstance(options, PlanOptions):
        plan_options = options
    elif isinstance(options, dict):
        plan_options = PlanOptions.from_mapping(options)
    else:
        raise BusinessError(E_INPUT_INVALID, "options must be an object or PlanOptions")
    parsed_predicates = [
        predicate if isinstance(predicate, Predicate) else parse_predicate(predicate, index)
        for index, predicate in enumerate(predicates)
    ]
    relation_count = len(snapshot.relations)
    effective_max_relations = min(plan_options.max_relations, MAX_SEARCH_RELATIONS)
    if relation_count == 0:
        raise BusinessError(E_INPUT_INVALID, "snapshot contains no relations")
    if relation_count > effective_max_relations:
        pruned = min(math.factorial(relation_count), (2**63) - 1)
        raise BusinessError(
            E_SEARCH_SPACE_TOO_LARGE,
            "relation count exceeds the configured search-space limit",
            {
                "relation_count": relation_count,
                "max_relations": effective_max_relations,
                "pruned_branches": pruned,
                "pruned_branch_bound": "complete permutations withheld before search",
                "snapshot_version": snapshot.version,
                "snapshot_digest": snapshot.digest,
            },
        )
    _, predicate_relation_ids_list, edges = _validate_predicates(
        snapshot, parsed_predicates
    )
    predicate_relation_ids = {
        predicate.id: ids
        for predicate, ids in zip(parsed_predicates, predicate_relation_ids_list)
    }
    estimates = estimate_selectivities(snapshot, parsed_predicates)
    estimates_by_predicate = _selectivities_by_predicate(estimates)
    subset_cache: dict[int, SubsetCardinality] = {}

    def get_subset(mask: int) -> SubsetCardinality:
        if mask not in subset_cache:
            subset_cache[mask] = _compute_subset(
                mask,
                snapshot,
                estimates_by_predicate,
                predicate_relation_ids,
            )
        return subset_cache[mask]

    for mask in range(1, 1 << relation_count):
        get_subset(mask)
    scan_cost = _scan_cost(snapshot)
    candidates: list[tuple[int, tuple[int, ...]]] = []
    pruned_branches = 0

    def visit(mask: int, sequence: tuple[int, ...]) -> None:
        nonlocal pruned_branches
        if len(sequence) == relation_count:
            total, _, _ = _candidate_cost(sequence, subset_cache, scan_cost)
            candidates.append((total, sequence))
            return
        for relation_id in range(relation_count):
            bit = 1 << relation_id
            if mask & bit:
                continue
            if mask == 0 or any((mask & (1 << other)) and (other, relation_id) in edges for other in range(relation_count)):
                visit(mask | bit, sequence + (relation_id,))
            else:
                pruned_branches += 1

    visit(0, ())
    if not candidates:
        raise BusinessError(
            E_NO_CONNECTED_PLAN,
            "no complete left-deep plan can be formed without a Cartesian product",
            {
                "relation_count": relation_count,
                "pruned_branches": pruned_branches,
                "edge_count": len(edges) // 2,
                "snapshot_version": snapshot.version,
                "snapshot_digest": snapshot.digest,
            },
        )
    absolute_min = min(cost for cost, _ in candidates)
    tolerance_amount = (absolute_min * plan_options.tolerance_ppm) // 1_000_000
    threshold = absolute_min + tolerance_amount
    eligible = [(cost, sequence) for cost, sequence in candidates if cost <= threshold]
    eligible.sort(
        key=lambda item: (
            item[0],
            tuple(snapshot.relations[index].name for index in item[1]),
        )
    )
    minimum_cost_candidate = min(
        candidates,
        key=lambda item: (
            item[0],
            tuple(snapshot.relations[index].name for index in item[1]),
        ),
    )
    selected_cost, selected_sequence = min(
        eligible,
        key=lambda item: tuple(snapshot.relations[index].name for index in item[1]),
    )
    selected_total, selected_join, selected_write = _candidate_cost(
        selected_sequence, subset_cache, scan_cost
    )
    selected_names = tuple(snapshot.relations[index].name for index in selected_sequence)
    minimum_names = tuple(
        snapshot.relations[index].name for index in minimum_cost_candidate[1]
    )
    tolerance_selection = {
        "tolerance_ppm": plan_options.tolerance_ppm,
        "comparison": "integer fixed-point relative tolerance; no float comparison",
        "absolute_minimum_cost_fixed": str(absolute_min),
        "absolute_minimum_sequence": list(minimum_names),
        "selected_cost_fixed": str(selected_cost),
        "selected_sequence": list(selected_names),
        "selected_via_tolerance": selected_cost > absolute_min,
        "equivalent_candidates_within_tolerance": len(eligible),
        "tie_break": "lexicographic relation-name sequence",
        "is_close_to_minimum": tolerance_is_close(
            selected_cost, absolute_min, plan_options.tolerance_ppm
        ),
    }
    search = {
        "relation_count": relation_count,
        "max_relations": effective_max_relations,
        "enumerated_complete_plans": len(candidates),
        "pruned_branches": pruned_branches,
        "pruned_branch_definition": (
            "candidate next relations skipped because no predicate edge connects them "
            "to the current left-deep prefix"
        ),
        "cartesian_products_allowed": False,
        "search_complete": True,
    }
    return _plan_details(
        request_id=request_id,
        snapshot=snapshot,
        predicates=parsed_predicates,
        estimates=estimates,
        subset_cache=subset_cache,
        sequence=selected_sequence,
        scan_cost=scan_cost,
        join_cost=selected_join,
        write_cost=selected_write,
        total_cost=selected_total,
        search=search,
        tolerance_selection=tolerance_selection,
    )

