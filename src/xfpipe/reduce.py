"""Stage 3 - dimensionality reduction (PCA / truncated SVD / random projection).

PCA runs on the explicit feature covariance, truncated SVD on the sample
matrix, and the random projection uses a fixed-seed Gaussian matrix whose
fingerprint is recorded.  Component signs are normalised deterministically
(largest-magnitude loading positive).  Accumulations (column sums, Gram
matrix, projection) are carried out in the scaled integer domain; the
resulting projections are stored as fixed-point values with per-column
scales, and an error bound versus the float64 reference is reported.
"""
from __future__ import annotations

import numpy as np

from .errors import E_DIM_INVALID, PipelineError
from .fingerprint import hash_obj
from .quantize import QuantizedMatrix, error_bound, quantize


def _design_matrix(cross_artifact: dict) -> np.ndarray:
    matrix = cross_artifact["matrix"]
    arr = np.asarray(matrix["values"], dtype=np.int64)
    scales = np.asarray(matrix["scales"], dtype=np.float64)
    return arr.astype(np.float64) / scales


def _check_target_dim(method: str, target_dim: int, n_features: int, n_rows: int) -> None:
    if target_dim > n_features:
        raise PipelineError(
            E_DIM_INVALID,
            f"{method}: target dimension exceeds the feature space",
            method=method, target_dim=target_dim,
            actual_dim=n_features, n_rows=n_rows,
        )


def _check_finite(method: str, components: np.ndarray, n_features: int) -> None:
    if not np.all(np.isfinite(components)):
        raise PipelineError(
            E_DIM_INVALID,
            f"{method}: non-finite principal components",
            method=method, actual_dim=n_features,
        )


def _normalise_signs(components: np.ndarray) -> np.ndarray:
    """Flip each component so its largest-magnitude loading is positive."""
    out = components.copy()
    for j in range(out.shape[1]):
        pivot = int(np.argmax(np.abs(out[:, j])))
        if out[pivot, j] < 0:
            out[:, j] = -out[:, j]
    return out


def _scaled_projection(
    design_q: np.ndarray,  # int64 scaled design matrix
    center_q: np.ndarray | None,  # int64 scaled column means or None
    components: np.ndarray,  # float64 (features x k)
    scale: float,
) -> tuple[QuantizedMatrix, np.ndarray, float]:
    """Accumulate the projection in the scaled integer domain.

    Returns the fixed-point projection, the float64 reference and a
    deterministic bound for the error introduced by quantising the
    projection factors (the integer accumulation itself is exact).
    """
    comp_q = np.rint(components * scale).astype(np.int64)
    base = design_q if center_q is None else design_q - center_q
    projected_q = base.astype(np.int64) @ comp_q  # exact integer accumulation
    out_scale = float(scale * scale)
    q = QuantizedMatrix(
        values=[[int(v) for v in row] for row in projected_q.tolist()],
        scales=[out_scale] * components.shape[1],
        shape=(int(projected_q.shape[0]), int(projected_q.shape[1])),
    )
    reference = (design_q.astype(np.float64) / scale - (center_q.astype(np.float64) / scale if center_q is not None else 0.0)) @ components
    # |x.(v - vq)| <= ||x||_1 * 0.5/scale per output dimension; the centre
    # rounding contributes at most ||v||_1 * 0.5/scale on top.
    base_float = base.astype(np.float64) / scale
    row_l1_max = float(np.max(np.abs(base_float).sum(axis=1))) if base_float.size else 0.0
    component_quantum = 0.5 / scale
    propagated = row_l1_max * component_quantum
    if center_q is not None:
        col_l1_max = float(np.max(np.abs(components).sum(axis=0))) if components.size else 0.0
        propagated += col_l1_max * component_quantum
    return q, reference, propagated


def _pca(design: np.ndarray, design_q: np.ndarray, target_dim: int, scale: float) -> dict:
    n_rows, n_features = design.shape
    _check_target_dim("pca", target_dim, n_features, n_rows)
    col_sums = design_q.sum(axis=0, dtype=np.int64)  # exact scaled sums
    center_q = np.rint(col_sums / n_rows).astype(np.int64)
    centered_q = design_q - center_q
    gram_q = centered_q.T.astype(np.int64) @ centered_q.astype(np.int64)
    covariance = gram_q.astype(np.float64) / ((n_rows - 1) * float(scale) ** 2)
    eigenvalues, eigenvectors = np.linalg.eigh(covariance)
    order = np.argsort(eigenvalues)[::-1]
    eigenvalues = eigenvalues[order]
    eigenvectors = _normalise_signs(eigenvectors[:, order])
    _check_finite("pca", eigenvectors, n_features)
    components = eigenvectors[:, :target_dim]
    total = float(np.sum(np.maximum(eigenvalues, 0.0)))
    ratio = [float(max(v, 0.0) / total) if total > 0 else 0.0 for v in eigenvalues[:target_dim]]
    projected, reference, propagated = _scaled_projection(design_q, center_q, components, scale)
    return {
        "method": "pca",
        "basis": "feature_covariance",
        "components": components,
        "explained_variance_ratio": ratio,
        "projected": projected,
        "reference": reference,
        "propagated_bound": propagated,
    }


def _truncated_svd(design: np.ndarray, design_q: np.ndarray, target_dim: int, scale: float) -> dict:
    n_rows, n_features = design.shape
    _check_target_dim("truncated_svd", target_dim, n_features, n_rows)
    _, singular_values, vt = np.linalg.svd(design, full_matrices=False)
    components = _normalise_signs(vt.T)
    _check_finite("truncated_svd", components, n_features)
    components = components[:, :target_dim]
    total = float(np.sum(singular_values ** 2))
    ratio = [float(v ** 2 / total) if total > 0 else 0.0 for v in singular_values[:target_dim]]
    projected, reference, propagated = _scaled_projection(design_q, None, components, scale)
    return {
        "method": "truncated_svd",
        "basis": "sample_matrix",
        "components": components,
        "explained_variance_ratio": ratio,
        "projected": projected,
        "reference": reference,
        "propagated_bound": propagated,
    }


def _random_projection(design: np.ndarray, design_q: np.ndarray, target_dim: int, seed: int, scale: float) -> dict:
    n_rows, n_features = design.shape
    _check_target_dim("random_projection", target_dim, n_features, n_rows)
    rng = np.random.RandomState(seed)
    matrix = rng.normal(0.0, 1.0 / np.sqrt(target_dim), size=(n_features, target_dim))
    _check_finite("random_projection", matrix, n_features)
    matrix_q = quantize(matrix, scale=scale)
    projected, reference, propagated = _scaled_projection(design_q, None, matrix, scale)
    return {
        "method": "random_projection",
        "basis": "fixed_seed_gaussian",
        "components": matrix,
        "projection_matrix_fingerprint": hash_obj(matrix_q.digest_obj()),
        "explained_variance_ratio": None,
        "projected": projected,
        "reference": reference,
        "propagated_bound": propagated,
    }


def reduce_features(cross_artifact: dict, cfg: dict) -> dict:
    design = _design_matrix(cross_artifact)
    scale = float(cfg.get("quantization", {}).get("scale", 1_000_000))
    design_q = np.rint(design * scale).astype(np.int64)
    seed = int(cfg["reduction"].get("seed", 0))
    results: list[dict] = []
    for candidate in cfg["reduction"]["candidates"]:
        method = candidate["method"]
        target_dim = int(candidate["target_dim"])
        if method == "pca":
            outcome = _pca(design, design_q, target_dim, scale)
        elif method == "truncated_svd":
            outcome = _truncated_svd(design, design_q, target_dim, scale)
        elif method == "random_projection":
            outcome = _random_projection(design, design_q, target_dim, seed, scale)
        else:  # guarded by config validation
            raise PipelineError(E_DIM_INVALID, f"unknown method {method!r}", method=method)

        projected = outcome["projected"]
        bound = error_bound(projected, outcome["reference"])
        bound["propagated_factor_bound"] = outcome["propagated_bound"]
        bound["total_bound"] = bound["total_bound"] + outcome["propagated_bound"]
        entry = {
            "method": method,
            "basis": outcome["basis"],
            "target_dim": target_dim,
            "input_dim": int(design.shape[1]),
            "n_rows": int(design.shape[0]),
            "explained_variance_ratio": outcome["explained_variance_ratio"],
            "components": quantize(outcome["components"], scale=scale).digest_obj(),
            "components_fingerprint": hash_obj(
                quantize(outcome["components"], scale=scale).digest_obj()
            ),
            "projected": projected.digest_obj(),
            "projected_fingerprint": hash_obj(projected.digest_obj()),
            "error_bound": bound,
        }
        if "projection_matrix_fingerprint" in outcome:
            entry["projection_matrix_fingerprint"] = outcome["projection_matrix_fingerprint"]
        results.append(entry)

    return {
        "stage": "reduce",
        "feature_count": int(design.shape[1]),
        "row_count": int(design.shape[0]),
        "quantization": {"scale": scale, "output_scale": float(scale * scale)},
        "candidates": results,
    }
