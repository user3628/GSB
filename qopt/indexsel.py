"""Index selection over the candidate index set.

Prefix matching rule: leading index columns are consumed by equality or
IN predicates; at most one further column may carry a range predicate,
which terminates the usable prefix. Columns after the range column are
not usable for seeking and their predicates become residual filters.
Every candidate that is not chosen is reported with a concrete reason.
"""

from .cost import (CPU_INDEX_TUPLE_COST, CPU_OPERATOR_COST, CPU_TUPLE_COST,
                   RANDOM_PAGE_COST, SEQ_PAGE_COST, CostBreakdown, choose_path)
from .numeric import clamp01

SEEK_EQ_TYPES = {"eq", "in"}


class AccessPath:
    __slots__ = ("key", "method", "index_name", "matched_columns",
                 "residual_columns", "sel_matched", "cost", "note")

    def __init__(self, key, method, index_name, matched_columns,
                 residual_columns, sel_matched, cost, note=""):
        self.key = key
        self.method = method
        self.index_name = index_name
        self.matched_columns = tuple(matched_columns)
        self.residual_columns = tuple(residual_columns)
        self.sel_matched = sel_matched
        self.cost = cost
        self.note = note

    def to_json(self, status, reason=""):
        out = {
            "access": self.key,
            "method": self.method,
            "status": status,
            "matched_columns": list(self.matched_columns),
            "residual_columns": list(self.residual_columns),
            "matched_selectivity": self.sel_matched,
            "cost": self.cost.to_json(),
        }
        if self.index_name is not None:
            out["index"] = self.index_name
        if reason:
            out["reason"] = reason
        if self.note:
            out["note"] = self.note
        return out


def analyse_prefix(index, preds_by_column):
    """Split index columns into (equality prefix, range column, unused)."""
    matched_eq = []
    range_col = None
    for col in index.columns:
        preds = preds_by_column.get(col)
        if not preds:
            break
        if any(p.type in SEEK_EQ_TYPES for p in preds):
            matched_eq.append(col)
            continue
        if any(p.type == "range" for p in preds):
            range_col = col
        break
    used = list(matched_eq) + ([range_col] if range_col else [])
    return matched_eq, range_col, used


def _full_scan_path(table, sel_total, n_predicates):
    rows = float(table.row_count)
    io = table.pages * SEQ_PAGE_COST
    cpu = rows * CPU_TUPLE_COST + rows * CPU_OPERATOR_COST * n_predicates
    sel_sensitive = rows * sel_total * CPU_TUPLE_COST
    return AccessPath(
        key="seq_scan",
        method="full_table_scan",
        index_name=None,
        matched_columns=(),
        residual_columns=(),
        sel_matched=1.0,
        cost=CostBreakdown(cpu, io, sel_sensitive),
    )


def _index_path(index, table, matched_eq, range_col, sel_matched, n_residual):
    rows = float(table.row_count)
    ms = clamp01(sel_matched)
    index_io = index.pages * ms * SEQ_PAGE_COST + RANDOM_PAGE_COST
    lookup_io = min(rows * ms, float(table.pages)) * RANDOM_PAGE_COST
    io = index_io + lookup_io
    cpu = (rows * ms * CPU_INDEX_TUPLE_COST
           + rows * ms * CPU_TUPLE_COST
           + rows * ms * CPU_OPERATOR_COST * n_residual)
    sel_sensitive = lookup_io + rows * ms * (CPU_INDEX_TUPLE_COST + CPU_TUPLE_COST)
    matched = list(matched_eq) + ([range_col] if range_col else [])
    note = ""
    if range_col:
        note = ("prefix stops at range column '%s'; later index columns unused"
                % range_col)
    return AccessPath(
        key="idx:%s" % index.name,
        method="index_scan",
        index_name=index.name,
        matched_columns=matched,
        residual_columns=(),
        sel_matched=ms,
        cost=CostBreakdown(cpu, io, sel_sensitive),
        note=note,
    )


def select_access_path(table, predicates, pred_sels, indexes):
    """Evaluate every candidate index plus the full scan for one table.

    predicates/pred_sels are aligned lists for this table only.
    Returns (chosen AccessPath, candidate report list).
    """
    preds_by_column = {}
    sel_by_column = {}
    for pred, sel in zip(predicates, pred_sels):
        preds_by_column.setdefault(pred.column, []).append(pred)
        sel_by_column[pred.column] = sel_by_column.get(pred.column, 1.0) * sel
    sel_total = 1.0
    for s in pred_sels:
        sel_total *= s
    sel_total = clamp01(sel_total)

    paths = [_full_scan_path(table, sel_total, len(predicates))]
    unusable = []
    for index in sorted(indexes, key=lambda i: i.name):
        matched_eq, range_col, used = analyse_prefix(index, preds_by_column)
        if not used:
            unusable.append({
                "access": "idx:%s" % index.name,
                "index": index.name,
                "status": "excluded",
                "reason": ("prefix_mismatch: leading index column '%s' has no "
                           "equality/IN/range predicate" % index.columns[0]),
            })
            continue
        sel_matched = 1.0
        for col in used:
            sel_matched *= sel_by_column.get(col, 1.0)
        n_residual = sum(len(preds_by_column[c]) for c in preds_by_column
                         if c not in used)
        path = _index_path(index, table, matched_eq, range_col,
                           clamp01(sel_matched), n_residual)
        path.residual_columns = tuple(sorted(c for c in preds_by_column
                                             if c not in used))
        paths.append(path)

    best, excluded = choose_path(paths)
    candidates = [best.to_json("chosen")]
    candidates.extend(p.to_json("excluded", reason) for p, reason in excluded)
    candidates.extend(unusable)
    candidates.sort(key=lambda c: (c["status"] != "chosen", c["access"]))
    return best, candidates
