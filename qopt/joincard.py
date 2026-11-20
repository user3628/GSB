"""Join cardinality estimation in the log domain.

Intermediate cardinalities are accumulated as natural logarithms
(equivalently: geometric-mean style accumulation of selectivity
factors), which keeps products of huge row counts and tiny selectivities
away from float overflow/underflow. Equi-join predicates divide by
max(NDV_left, NDV_right) under the containment assumption. Joins with no
connecting predicate are cartesian products: they are flagged and capped
by an explicit, documented ceiling instead of being silently truncated.
"""

import math

from .errors import QueryInvalidError, StatsMissingError
from .numeric import xlog

# Explicit ceiling for cartesian intermediates (rows). The uncapped
# log-domain value is always reported alongside when the cap engages.
CARTESIAN_CAP = 1e15
LOG_CARTESIAN_CAP = math.log(CARTESIAN_CAP)


class JoinPred:
    __slots__ = ("left_table", "left_column", "right_table", "right_column")

    def __init__(self, left_table, left_column, right_table, right_column):
        self.left_table = left_table
        self.left_column = left_column
        self.right_table = right_table
        self.right_column = right_column

    def connects(self, joined_tables, candidate):
        tables = {self.left_table, self.right_table}
        return candidate in tables \
            and any(t in joined_tables for t in tables) \
            and not (self.left_table in joined_tables
                     and self.right_table in joined_tables)

    def to_dict(self):
        return {
            "left": {"table": self.left_table, "column": self.left_column},
            "right": {"table": self.right_table, "column": self.right_column},
        }


def parse_join_predicates(raw_list, tables):
    out = []
    for raw in raw_list or []:
        if not isinstance(raw, dict):
            raise QueryInvalidError("join predicate must be an object")
        if raw.get("type", "eq") != "eq":
            raise QueryInvalidError("only equality join predicates are supported",
                                    type=raw.get("type"))
        left = raw.get("left")
        right = raw.get("right")
        for side_name, side in (("left", left), ("right", right)):
            if not isinstance(side, dict) or not isinstance(side.get("table"), str) \
                    or not isinstance(side.get("column"), str):
                raise QueryInvalidError(
                    "join predicate requires %s.table and %s.column"
                    % (side_name, side_name))
            if side["table"] not in tables:
                raise QueryInvalidError(
                    "join predicate references a table not in the query",
                    table=side["table"], query_tables=sorted(tables))
        out.append(JoinPred(left["table"], left["column"],
                            right["table"], right["column"]))
    out.sort(key=lambda jp: (jp.left_table, jp.left_column,
                             jp.right_table, jp.right_column))
    return out


def _join_ndv(jp, snapshot):
    ndv_l = snapshot.column_ndv(jp.left_table, jp.left_column)
    ndv_r = snapshot.column_ndv(jp.right_table, jp.right_column)
    missing = []
    if ndv_l is None:
        missing.append("%s.%s" % (jp.left_table, jp.left_column))
    if ndv_r is None:
        missing.append("%s.%s" % (jp.right_table, jp.right_column))
    if missing:
        raise StatsMissingError("no statistics for join column", columns=missing)
    return max(ndv_l, ndv_r, 1)


def _materialise(log_card):
    """Convert a log-domain cardinality to the reported (capped) value."""
    if log_card == -math.inf:
        return 0.0, False
    if log_card > LOG_CARTESIAN_CAP:
        return CARTESIAN_CAP, True
    return math.exp(log_card), False


def estimate_join(base_cards, join_preds, snapshot):
    """Greedy left-deep join order with a single enumeration pass.

    Starts from the smallest base cardinality and repeatedly appends the
    table yielding the smallest intermediate log-cardinality (ties broken
    by table name). No backtracking: exactly one pass, O(n^2) candidates.
    """
    tables = sorted(base_cards)
    if not tables:
        raise QueryInvalidError("query must reference at least one table")
    first = min(tables, key=lambda t: (base_cards[t], t))
    order = [first]
    remaining = set(tables) - {first}
    log_card = xlog(base_cards[first])
    steps = []
    notes = []
    while remaining:
        best = None
        for t in sorted(remaining):
            connecting = [jp for jp in join_preds if jp.connects(order, t)]
            lc = log_card + xlog(base_cards[t])
            for jp in connecting:
                lc -= xlog(_join_ndv(jp, snapshot))
            key = (lc, t)
            if best is None or key < best[0]:
                best = (key, t, connecting)
        (_, t, connecting) = best
        log_card = best[0][0]
        cartesian = not connecting
        cardinality, capped = _materialise(log_card)
        if cartesian:
            notes.append(
                "cartesian product with table '%s' (no join predicate); "
                "explicit ceiling policy cap=%.3e applied=%s"
                % (t, CARTESIAN_CAP, capped))
        steps.append({
            "added_table": t,
            "join_predicates": [jp.to_dict() for jp in connecting],
            "cartesian": cartesian,
            "log_cardinality": log_card if math.isfinite(log_card) else None,
            "intermediate_cardinality": cardinality,
            "cap_applied": capped,
        })
        order.append(t)
        remaining.discard(t)
    final_card, final_capped = _materialise(log_card)
    return {
        "join_order": order,
        "steps": steps,
        "final_cardinality": final_card,
        "final_log_cardinality": log_card if math.isfinite(log_card) else None,
        "cap_applied": final_capped,
        "cartesian_cap": CARTESIAN_CAP,
        "notes": notes,
    }
