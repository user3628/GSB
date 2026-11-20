"""Projection alignment into the shared 64-dim retrieval space + unit norm."""

import json

import numpy as np

from .errors import CmrError, E_NUMERIC, E_PROJECTION_INVALID

NORM_TOL = 1e-9


def load_projections(path, cfg, stage="projection"):
    """Load and validate both projection matrices. Raises E_PROJECTION_INVALID."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            obj = json.load(fh)
    except (OSError, ValueError) as exc:
        raise CmrError(E_PROJECTION_INVALID, f"cannot read projection file: {exc}",
                       stage=stage, position="file")
    mats = {}
    for name, expect_cols in (("image", cfg.image_dim), ("text", cfg.text_dim)):
        if name not in obj or not isinstance(obj[name], dict):
            raise CmrError(E_PROJECTION_INVALID, f"projection matrix '{name}' missing",
                           stage=stage, position=name)
        spec = obj[name]
        data = spec.get("data")
        if data is None:
            raise CmrError(E_PROJECTION_INVALID, f"projection matrix '{name}' has no data",
                           stage=stage, position=name)
        try:
            mat = np.asarray(data, dtype=np.float64)
        except (ValueError, TypeError) as exc:
            raise CmrError(E_PROJECTION_INVALID, f"projection matrix '{name}' unreadable: {exc}",
                           stage=stage, position=name)
        if mat.shape != (cfg.embed_dim, expect_cols):
            raise CmrError(
                E_PROJECTION_INVALID,
                f"projection matrix '{name}' shape {mat.shape} != expected "
                f"({cfg.embed_dim}, {expect_cols})",
                stage=stage, position=name,
                detail={"got": list(mat.shape), "expect": [cfg.embed_dim, expect_cols]},
            )
        bad = np.argwhere(~np.isfinite(mat))
        if bad.shape[0] > 0:
            row, col = (int(bad[0][0]), int(bad[0][1]))
            kind = "NaN" if np.isnan(mat[row, col]) else "Inf"
            raise CmrError(
                E_PROJECTION_INVALID,
                f"projection matrix '{name}' contains {kind} at [{row}][{col}]",
                stage=stage, position=f"{name}[{row}][{col}]",
                detail={"matrix": name, "row": row, "col": col, "kind": kind},
            )
        mats[name] = mat
    return mats["image"], mats["text"]


def project_and_normalize(mat, vec, stage="projection"):
    """z = M v, then L2-normalize with a strict 1e-9 norm check (E_NUMERIC)."""
    vec = np.asarray(vec, dtype=np.float64)
    if mat.shape[1] != vec.shape[0]:
        raise CmrError(
            E_PROJECTION_INVALID,
            f"matrix cols {mat.shape[1]} != feature dim {vec.shape[0]}",
            stage=stage, position="matmul",
        )
    z = mat @ vec
    norm = float(np.linalg.norm(z))
    if not np.isfinite(norm) or norm == 0.0:
        raise CmrError(E_NUMERIC, f"cannot normalize vector with norm={norm!r}", stage=stage)
    z = z / norm
    check = abs(float(np.linalg.norm(z)) - 1.0)
    if check > NORM_TOL:
        raise CmrError(E_NUMERIC, f"unit-norm deviation {check:.3e} exceeds 1e-9", stage=stage,
                       detail={"deviation": check})
    return z
