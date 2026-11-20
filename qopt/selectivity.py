"""Predicate selectivity estimation.

Supported predicate types: eq, range, in, is_null, is_not_null, and.
Every estimate is clamped to [0, 1]; empty tables and zero-NDV columns
return deterministic values instead of raising division-by-zero errors.

Histogram semantics: equi-depth, right-closed buckets (b_i, b_{i+1}]
with the first bucket closed on both ends; degenerate buckets are point
masses. Cumulative counts use Kahan compensated summation (see
qopt.numeric), so repeated evaluation of the same snapshot is bitwise
reproducible and the accumulated rounding error stays within the
reported 2*u*sum(|x|) bound.
"""

from .errors import QueryInvalidError, StatsMissingError
from .numeric import clamp01, kahan_sum

PRED_TYPES = {"eq", "range", "in", "is_null", "is_not_null", "and"}


class Pred:
    """Normalised single-column predicate (AND is flattened upstream)."""

    __slots__ = ("type", "table", "column", "value", "values",
                 "low", "high", "low_inclusive", "high_inclusive")

    def __init__(self, type, table, column, value=None, values=None,
                 low=None, high=None, low_inclusive=True, high_inclusive=True):
        self.type = type
        self.table = table
        self.column = column
        self.value = value
        self.values = values
        self.low = low
        self.high = high
        self.low_inclusive = low_inclusive
        self.high_inclusive = high_inclusive

    def sort_key(self):
        import json
        payload = json.dumps(
            {"v": self.value, "vs": self.values, "lo": self.low, "hi": self.high},
            sort_keys=True, default=str)
        return (self.table or "", self.column or "", self.type, payload)


def _bad(message, **details):
    raise QueryInvalidError(message, details)


def parse_predicates(raw_list, tables):
    """Validate and flatten the query predicate list (AND semantics)."""
    out = []
    for raw in raw_list or []:
        if not isinstance(raw, dict):
            _bad("predicate must be an object")
        ptype = raw.get("type")
        if ptype not in PRED_TYPES:
            _bad("unsupported predicate type", type=ptype)
        if ptype == "and":
            nested = raw.get("predicates")
            if not isinstance(nested, list):
                _bad("'and' predicate requires a 'predicates' list")
            out.extend(parse_predicates(nested, tables))
            continue
        table = raw.get("table")
        column = raw.get("column")
        if not isinstance(table, str) or not isinstance(column, str):
            _bad("predicate requires string 'table' and 'column'", type=ptype)
        if table not in tables:
            _bad("predicate references a table not in the query",
                 table=table, query_tables=sorted(tables))
        if ptype == "eq":
            if "value" not in raw:
                _bad("eq predicate requires 'value'", column=column)
            out.append(Pred("eq", table, column, value=raw.get("value")))
        elif ptype == "in":
            values = raw.get("values")
            if not isinstance(values, list):
                _bad("in predicate requires a 'values' list", column=column)
            out.append(Pred("in", table, column, values=list(values)))
        elif ptype == "range":
            low = raw.get("low")
            high = raw.get("high")
            if low is None and high is None:
                _bad("range predicate requires 'low' and/or 'high'", column=column)
            for bound_name, bound in (("low", low), ("high", high)):
                if bound is not None and not isinstance(bound, (int, float)):
                    _bad("range bounds must be numeric", column=column, bound=bound_name)
            if low is not None and high is not None and low > high:
                _bad("range low must not exceed high", column=column)
            out.append(Pred("range", table, column, low=low, high=high,
                            low_inclusive=bool(raw.get("low_inclusive", True)),
                            high_inclusive=bool(raw.get("high_inclusive", True))))
        else:  # is_null / is_not_null
            out.append(Pred(ptype, table, column))
    out.sort(key=lambda p: p.sort_key())
    return out


def _column_stats(snapshot, table, column):
    t = snapshot.table(table)
    if t is None:
        raise StatsMissingError("no statistics for table", table=table)
    c = t.column(column)
    if c is None:
        raise StatsMissingError("no statistics for column", table=table, column=column)
    return t, c


def _bucket_index(hist, v):
    """Bucket containing v, else None.

    Degenerate point-mass buckets (b_i == b_{i+1} == v) are checked first:
    they isolate heavy hitters and give the best equality estimate.
    Interval buckets follow right-closed semantics (b_i, b_{i+1}] with
    the first bucket closed on both ends.
    """
    bounds = hist.boundaries
    n = len(hist.counts)
    if v < bounds[0] or v > bounds[-1]:
        return None
    for i in range(n):
        if bounds[i] == bounds[i + 1] == v:
            return i
    for i in range(n):
        lo, hi = bounds[i], bounds[i + 1]
        if lo < v <= hi:
            return i
        if i == 0 and lo <= v <= hi:
            return i
    return None


def _eq_selectivity_hist(hist, v, null_frac):
    total = hist.total_count
    if total <= 0.0:
        return 0.0
    idx = _bucket_index(hist, v)
    if idx is None:
        return 0.0
    ndv_b = hist.ndv_per_bucket[idx]
    if ndv_b <= 0:
        return 0.0
    return clamp01((1.0 - null_frac) * (hist.counts[idx] / total) / ndv_b)


def _range_fraction_hist(hist, low, high, low_inc, high_inc):
    total = hist.total_count
    if total <= 0.0:
        return 0.0
    lo = -float("inf") if low is None else float(low)
    hi = float("inf") if high is None else float(high)
    terms = []
    bounds = hist.boundaries
    for i, count in enumerate(hist.counts):
        b_lo, b_hi = bounds[i], bounds[i + 1]
        if b_lo == b_hi:
            v = b_lo
            if lo == hi:
                inside = v == lo and low_inc and high_inc
            else:
                inside = ((lo < v < hi) or (v == lo and low_inc)
                          or (v == hi and high_inc))
            terms.append(count if inside else 0.0)
            continue
        ov_lo = max(lo, b_lo)
        ov_hi = min(hi, b_hi)
        if ov_hi <= ov_lo:
            terms.append(0.0)
            continue
        terms.append(count * ((ov_hi - ov_lo) / (b_hi - b_lo)))
    return clamp01(kahan_sum(terms) / total)


def estimate_predicate(pred, snapshot):
    """Selectivity of one normalised predicate; always in [0, 1]."""
    t, c = _column_stats(snapshot, pred.table, pred.column)
    if t.row_count == 0:
        return 0.0  # empty table: deterministic zero
    null_frac = c.null_frac
    if pred.type == "is_null":
        return clamp01(null_frac)
    if pred.type == "is_not_null":
        return clamp01(1.0 - null_frac)
    if c.ndv == 0:
        return 0.0  # no non-null distinct values: deterministic zero
    if pred.type == "eq":
        if pred.value is None:
            return 0.0  # NULL never satisfies equality
        if c.histogram is not None and isinstance(pred.value, (int, float)):
            return _eq_selectivity_hist(c.histogram, float(pred.value), null_frac)
        return clamp01((1.0 - null_frac) / c.ndv)
    if pred.type == "in":
        if not pred.values:
            return 0.0
        terms = []
        for v in pred.values:
            if v is None:
                terms.append(0.0)
            elif c.histogram is not None and isinstance(v, (int, float)):
                terms.append(_eq_selectivity_hist(c.histogram, float(v), null_frac))
            else:
                terms.append((1.0 - null_frac) / c.ndv)
        return clamp01(min(kahan_sum(terms), 1.0 - null_frac))
    if pred.type == "range":
        if c.histogram is None:
            # Without a histogram only a deterministic default is possible.
            return clamp01((1.0 - null_frac) * 0.25)
        frac = _range_fraction_hist(c.histogram, pred.low, pred.high,
                                    pred.low_inclusive, pred.high_inclusive)
        return clamp01((1.0 - null_frac) * frac)
    raise QueryInvalidError("unsupported predicate type", type=pred.type)


def combine_conjuncts(table, pred_sels, snapshot):
    """Combine per-predicate selectivities of one table into a joint value.

    Declared correlations override the independence assumption:
    * functional_dependency (a -> b): joint = max(per-column selectivities)
    * coefficient rho: joint = (1-rho)*independent + rho*min(per-column)
    Both forms stay inside [0, 1]. Undeclared column combinations keep
    the independence assumption and are labelled as such in the notes.
    """
    remaining = list(enumerate(pred_sels))
    factors = []
    notes = []
    for corr in snapshot.correlations_for(table):
        involved = [(i, ps) for (i, ps) in remaining if ps[0].column in corr.columns]
        covered = {ps[0].column for _, ps in involved}
        if len(covered) < 2:
            continue
        per_col = {}
        for _, (pred, sel) in involved:
            per_col[pred.column] = per_col.get(pred.column, 1.0) * sel
        sels = [per_col[c] for c in sorted(per_col)]
        independent = 1.0
        for s in sels:
            independent *= s
        if corr.kind == "functional_dependency":
            joint = max(sels)
            notes.append(
                "declared functional dependency %s: joint=max(%s)=%.6g "
                "replaces independence estimate %.6g"
                % ("->".join(corr.columns),
                   ",".join("%.6g" % s for s in sels), joint, independent))
        else:
            rho = corr.value
            joint = (1.0 - rho) * independent + rho * min(sels)
            notes.append(
                "declared correlation coefficient rho=%.6g on %s: "
                "joint=(1-rho)*indep+rho*min=%.6g (indep=%.6g)"
                % (rho, "/".join(corr.columns), joint, independent))
        factors.append(clamp01(joint))
        used = {i for i, _ in involved}
        remaining = [(i, ps) for (i, ps) in remaining if i not in used]
    for _, (pred, sel) in remaining:
        factors.append(sel)
        notes.append(
            "independence assumption (no declared correlation) for column '%s'"
            % pred.column)
    joint = 1.0
    for f in factors:
        joint *= f
    return clamp01(joint), notes
