"""Query planner: selectivity -> access paths -> join order -> plan JSON.

The plan output is deterministic for a given snapshot and query: fixed
iteration orders, Kahan compensated summation for histograms, log-domain
join cardinality, tolerant cost comparison with stable tie-breaks.
"""

from .cost import CPU_TUPLE_COST, CostBreakdown
from .errors import QueryInvalidError, StatsMissingError
from .indexsel import select_access_path
from .joincard import estimate_join, parse_join_predicates
from .numeric import COST_DECIMALS, PLAN_ABS_TOL, PLAN_REL_TOL, kahan_sum
from .selectivity import combine_conjuncts, estimate_predicate, parse_predicates


def _validate_query(query):
    if not isinstance(query, dict):
        raise QueryInvalidError("query must be a JSON object")
    tables = query.get("tables")
    if not isinstance(tables, list) or not tables \
            or not all(isinstance(t, str) for t in tables):
        raise QueryInvalidError("query.tables must be a non-empty list of table names")
    if len(set(tables)) != len(tables):
        raise QueryInvalidError("query.tables must not contain duplicates")
    for key in ("predicates", "join_predicates"):
        if key in query and not isinstance(query[key], list):
            raise QueryInvalidError("query.%s must be a list" % key)
    return list(tables)


def plan_query(query, snapshot):
    """Build a full plan for a query dict against a loaded snapshot."""
    tables = _validate_query(query)
    table_set = set(tables)
    for t in tables:
        if snapshot.table(t) is None:
            raise StatsMissingError("no statistics for table", table=t)
    predicates = parse_predicates(query.get("predicates", []), table_set)
    join_preds = parse_join_predicates(query.get("join_predicates", []), table_set)

    per_table_preds = {t: [] for t in tables}
    for pred in predicates:
        per_table_preds[pred.table].append(pred)

    table_reports = []
    base_cards = {}
    chosen_paths = []
    for t in sorted(tables):
        stats = snapshot.table(t)
        preds = per_table_preds[t]
        pred_sels = [estimate_predicate(p, snapshot) for p in preds]
        joint_sel, notes = combine_conjuncts(
            t, list(zip(preds, pred_sels)), snapshot)
        chosen, candidates = select_access_path(
            stats, preds, pred_sels, snapshot.indexes_for(t))
        chosen_paths.append(chosen)
        est_rows = stats.row_count * joint_sel
        base_cards[t] = est_rows
        table_reports.append({
            "table": t,
            "row_count": stats.row_count,
            "local_selectivity": joint_sel,
            "estimated_rows": est_rows,
            "predicate_selectivities": [
                {"column": p.column, "type": p.type,
                 "selectivity": s}
                for p, s in zip(preds, pred_sels)
            ],
            "assumptions": notes,
            "chosen_access": chosen.to_json("chosen"),
            "candidates": candidates,
        })

    join = estimate_join(base_cards, join_preds, snapshot)

    # Total plan cost: chosen access paths plus per-intermediate-row join CPU.
    join_cpu_terms = [step["intermediate_cardinality"] * CPU_TUPLE_COST
                      for step in join["steps"]]
    total = CostBreakdown(
        cpu_cost=kahan_sum([p.cost.cpu_cost for p in chosen_paths]
                           + [kahan_sum(join_cpu_terms)]),
        io_cost=kahan_sum([p.cost.io_cost for p in chosen_paths]),
        sel_sensitive_cost=kahan_sum([p.cost.sel_sensitive_cost
                                      for p in chosen_paths]),
    )

    return {
        "snapshot_version": snapshot.version,
        "snapshot_generated_at": snapshot.generated_at,
        "snapshot_fingerprint": snapshot.fingerprint,
        "join_order": join["join_order"],
        "join_steps": join["steps"],
        "join_notes": join["notes"],
        "final_cardinality": join["final_cardinality"],
        "final_log_cardinality": join["final_log_cardinality"],
        "cartesian_cap_policy": {
            "cap": join["cartesian_cap"],
            "cap_applied": join["cap_applied"],
            "policy": "cartesian intermediates are flagged and capped at the "
                      "explicit ceiling; uncapped log-domain value reported",
        },
        "tables": table_reports,
        "total_cost": total.to_json(),
        "precision": {
            "cost_decimals": COST_DECIMALS,
            "plan_rel_tol": PLAN_REL_TOL,
            "plan_abs_tol": PLAN_ABS_TOL,
            "summation": "Kahan-Neumaier compensated, fixed iteration order",
            "tie_break": "stable lexicographic path key after tolerant compare",
        },
    }
