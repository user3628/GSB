"""Stage 1: column type inference from non-empty counts and numeric ratio."""

from __future__ import annotations

from .errors import ConfigError


def infer_stage(columns: dict, cfg: dict) -> dict:
    label_col = cfg["label_column"]
    if label_col not in columns:
        raise ConfigError("label column not found in sample",
                          {"label_column": label_col, "columns": sorted(columns)})
    threshold = float(cfg["inference"]["numeric_ratio_threshold"])
    n_rows = len(next(iter(columns.values()))) if columns else 0
    cols_meta = []
    counts = {"numeric": 0, "categorical": 0, "label": 0}
    for name, values in columns.items():
        non_empty = [v for v in values if v != ""]
        n_non_empty = len(non_empty)
        if name == label_col:
            kind = "label"
            numeric_ok = n_non_empty
        else:
            numeric_ok = 0
            for v in non_empty:
                try:
                    float(v)
                    numeric_ok += 1
                except ValueError:
                    pass
            ratio = (numeric_ok / n_non_empty) if n_non_empty else 0.0
            kind = "numeric" if (n_non_empty > 0 and ratio >= threshold) else "categorical"
        counts[kind] += 1
        cols_meta.append(
            {
                "name": name,
                "kind": kind,
                "non_empty": n_non_empty,
                "missing": n_rows - n_non_empty,
                "numeric_ratio": (numeric_ok / n_non_empty) if n_non_empty else 0.0,
                "distinct": len(set(non_empty)),
            }
        )
    return {"stage": "infer", "rows": n_rows, "columns": cols_meta, "counts": counts}
