"""Stage 1 - column type inference.

Columns are classified from the non-null value count and the parseable
numeric ratio into ``numeric`` / ``categorical`` / ``missing`` (plus the
configured ``label`` column).  Numeric columns are binned with deterministic
equal-width edges; categorical and missing cells are explicitly labelled
with the ``__MISSING__`` token.  Literal NaN/Inf values raise E_NUMERIC and
fully empty columns raise E_EMPTY_FEATURE - never a silent empty matrix.
"""
from __future__ import annotations

import csv
import math

from .errors import E_EMPTY_FEATURE, E_NUMERIC, PipelineError

MISSING_LABEL = "__MISSING__"
_NON_FINITE_TOKENS = {"nan", "inf", "+inf", "-inf", "infinity", "+infinity", "-infinity"}


def load_csv(path: str) -> tuple[list[str], list[dict[str, str]]]:
    """Load the sample CSV in a deterministic row/column order."""
    with open(path, "r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        header = list(reader.fieldnames or [])
        rows = [dict(row) for row in reader]
    if not header:
        raise PipelineError(E_EMPTY_FEATURE, "sample CSV has no header", path=path)
    if not rows:
        raise PipelineError(E_EMPTY_FEATURE, "sample CSV has no data rows", path=path)
    return header, rows


def _scan_non_finite(column: str, rows: list[dict[str, str]]) -> None:
    bad_rows: list[int] = []
    for idx, row in enumerate(rows):
        raw = (row.get(column) or "").strip()
        if not raw:
            continue
        low = raw.lower()
        if low in _NON_FINITE_TOKENS:
            bad_rows.append(idx)
            continue
        try:
            value = float(raw)
        except ValueError:
            continue  # genuine categorical token, handled by the classifier
        if not math.isfinite(value):
            bad_rows.append(idx)
    if bad_rows:
        raise PipelineError(
            E_NUMERIC,
            f"column {column!r} contains non-finite values",
            column=column,
            rows=bad_rows[:32],
            non_finite_count=len(bad_rows),
        )


def infer_columns(header: list[str], rows: list[dict[str, str]], cfg: dict) -> dict:
    inf_cfg = cfg["inference"]
    parse_ratio_threshold = float(inf_cfg.get("numeric_parse_ratio", 0.9))
    max_cardinality = int(inf_cfg.get("max_categorical_cardinality", 48))
    missing_threshold = float(inf_cfg.get("missing_ratio_threshold", 0.9))
    numeric_bins = int(inf_cfg.get("numeric_bins", 8))
    label_column = inf_cfg.get("label_column", "label")

    row_count = len(rows)
    columns: list[dict] = []
    row_tokens: dict[str, list[str]] = {}

    for name in header:
        _scan_non_finite(name, rows)
        raw_values = [(row.get(name) or "").strip() for row in rows]
        non_null = [v for v in raw_values if v != ""]
        non_null_count = len(non_null)
        missing_count = row_count - non_null_count
        missing_ratio = missing_count / row_count

        if name == label_column:
            labels = sorted(set(non_null))
            columns.append({
                "name": name, "type": "label",
                "non_null": non_null_count, "missing": missing_count,
                "cardinality": len(labels), "labels": labels,
            })
            continue

        if non_null_count == 0:
            raise PipelineError(
                E_EMPTY_FEATURE,
                f"column {name!r} is entirely missing",
                column=name, rows=list(range(min(row_count, 32))),
            )

        numeric_hits = 0
        numeric_values: list[float] = []
        for v in non_null:
            try:
                numeric_values.append(float(v))
                numeric_hits += 1
            except ValueError:
                pass
        parse_ratio = numeric_hits / non_null_count

        if missing_ratio >= missing_threshold:
            col_type = "missing"
        elif parse_ratio >= parse_ratio_threshold:
            col_type = "numeric"
        else:
            col_type = "categorical"

        if col_type == "numeric":
            lo = min(numeric_values)
            hi = max(numeric_values)
            if hi == lo:
                edges = [lo, hi]
            else:
                step = (hi - lo) / numeric_bins
                edges = [lo + step * i for i in range(numeric_bins + 1)]
                edges[-1] = hi
            tokens = []
            for raw in raw_values:
                if raw == "":
                    tokens.append(MISSING_LABEL)
                else:
                    tokens.append(f"bin_{_bin_index(float(raw), edges)}")
            row_tokens[name] = tokens
            columns.append({
                "name": name, "type": "numeric",
                "non_null": non_null_count, "missing": missing_count,
                "parse_ratio": round(parse_ratio, 6),
                "bins": {"strategy": "equal_width", "count": numeric_bins,
                         "edges": [f"{e:.10f}" for e in edges]},
                "missing_label": MISSING_LABEL,
            })
        else:
            labels = sorted(set(non_null))
            excluded = col_type == "categorical" and len(labels) > max_cardinality
            tokens = [v if v else MISSING_LABEL for v in raw_values]
            row_tokens[name] = tokens
            entry = {
                "name": name, "type": col_type,
                "non_null": non_null_count, "missing": missing_count,
                "cardinality": len(labels), "labels": labels,
                "missing_label": MISSING_LABEL,
            }
            if excluded:
                entry["excluded_from_crossing"] = True
                entry["exclusion_reason"] = (
                    f"cardinality {len(labels)} exceeds cap {max_cardinality}"
                )
            columns.append(entry)

    return {
        "stage": "infer",
        "row_count": row_count,
        "columns": columns,
        "row_tokens": {name: row_tokens[name] for name in sorted(row_tokens)},
    }


def _bin_index(value: float, edges: list[float]) -> int:
    if len(edges) <= 2:
        return 0
    for i in range(len(edges) - 1):
        if edges[i] <= value < edges[i + 1]:
            return i
    return len(edges) - 2  # value == edges[-1]
