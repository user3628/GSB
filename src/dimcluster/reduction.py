from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.decomposition import PCA, TruncatedSVD
from sklearn.random_projection import GaussianRandomProjection

from .config import stage_config
from .errors import invalid_config, invalid_stage
from .preprocessing import PreprocessedData
from .stage import ArtifactBundle
from .util import (
    array_bytes,
    canonical_json_bytes,
    load_array,
    sha256_bytes,
    stable_round,
)

CANDIDATES = ("pca", "truncated_svd", "random_projection")


def reduce_input_fingerprint(
    preprocess_digest: str,
    preprocess_metadata: dict[str, Any],
    config: dict[str, Any],
) -> str:
    payload = {
        "preprocess_digest": preprocess_digest,
        "rows": preprocess_metadata["rows"],
        "encoded_dimension": preprocess_metadata["encoded_dimension"],
        "reduction": stage_config(config, "reduce"),
    }
    return sha256_bytes(canonical_json_bytes(payload))


def _sign_flip(
    reduced: np.ndarray, components: np.ndarray
) -> tuple[np.ndarray, np.ndarray]:
    reduced = np.array(reduced, copy=True)
    components = np.array(components, copy=True)
    for index in range(components.shape[0]):
        row = components[index]
        pivot = int(np.argmax(np.abs(row)))
        if row[pivot] < 0:
            components[index] = -row
            reduced[:, index] = -reduced[:, index]
    return reduced, components


def _fit_candidate(
    kind: str,
    X: np.ndarray,
    target_dim: int,
    config: dict[str, Any],
) -> dict[str, Any]:
    red = config["reduction"]
    seed = int(red["random_state"])
    rows, encoded_dim = X.shape
    if target_dim < 1 or target_dim > min(rows, encoded_dim):
        raise invalid_config(
            "Reduction target dimension is out of range",
            {
                "target_dimension": target_dim,
                "rows": rows,
                "encoded_dimension": encoded_dim,
            },
        )
    if kind == "pca":
        if rows <= 5000 or encoded_dim <= 32:
            solver = "full"
        else:
            solver = "randomized"
        model = PCA(
            n_components=target_dim,
            svd_solver=solver,
            random_state=seed if solver == "randomized" else None,
        )
        reduced = model.fit_transform(X)
        components = np.asarray(model.components_, dtype=np.float64)
        mean = np.asarray(model.mean_, dtype=np.float64)
        reduced, components = _sign_flip(reduced, components)
        reconstruction = reduced @ components + mean
        explained = np.asarray(model.explained_variance_ratio_, dtype=np.float64)
        explained_basis = "pca_explained_variance_ratio"
        method = {
            "kind": "pca",
            "svd_solver": solver,
            "center": True,
            "random_state": seed,
        }
    elif kind == "truncated_svd":
        model = TruncatedSVD(
            n_components=target_dim,
            algorithm="randomized",
            n_iter=int(red["svd_n_iter"]),
            random_state=seed,
        )
        reduced = model.fit_transform(X)
        components = np.asarray(model.components_, dtype=np.float64)
        mean = np.zeros(encoded_dim, dtype=np.float64)
        reduced, components = _sign_flip(reduced, components)
        reconstruction = reduced @ components
        explained = np.asarray(model.explained_variance_ratio_, dtype=np.float64)
        explained_basis = "svd_explained_variance_ratio"
        method = {
            "kind": "truncated_svd",
            "algorithm": "randomized",
            "n_iter": int(red["svd_n_iter"]),
            "center": False,
            "random_state": seed,
        }
    elif kind == "random_projection":
        model = GaussianRandomProjection(
            n_components=target_dim,
            eps=float(red["projection_eps"]),
            random_state=seed,
        )
        reduced = model.fit_transform(X)
        components = np.asarray(model.components_, dtype=np.float64)
        mean = np.zeros(encoded_dim, dtype=np.float64)
        reduced, components = _sign_flip(reduced, components)
        reconstruction = reduced @ np.linalg.pinv(components).T
        component_variances = np.var(reduced, axis=0, ddof=0)
        order = np.argsort(-component_variances, kind="stable")
        total_variance = float(np.sum(np.var(X, axis=0, ddof=0)))
        if total_variance <= 1e-15:
            explained = np.zeros(target_dim, dtype=np.float64)
        else:
            explained = np.clip(
                component_variances[order] / total_variance, 0.0, 1.0
            )
        explained_basis = "projected_component_variance_energy"
        method = {
            "kind": "random_projection",
            "eps": float(red["projection_eps"]),
            "center": False,
            "random_state": seed,
        }
    else:
        raise invalid_config("Unknown reduction candidate", {"candidate": kind})

    X64 = np.asarray(X, dtype=np.float64)
    reduced = stable_round(reduced)
    components = stable_round(components)
    mean = stable_round(mean)
    reconstruction = stable_round(reconstruction)
    residual = X64 - reconstruction
    row_error = np.sum(residual * residual, axis=1)
    denominator = float(np.sum(X64 * X64))
    reconstruction_error = float(np.sum(row_error) / denominator) if denominator > 0 else 0.0
    cumulative = np.minimum(np.cumsum(explained), 1.0)
    return {
        "candidate": kind,
        "target_dim": int(target_dim),
        "reduced": reduced,
        "components": components,
        "mean": mean,
        "reconstruction": reconstruction,
        "row_reconstruction_error": row_error,
        "reconstruction_error": reconstruction_error,
        "explained_variance_ratio": explained,
        "cumulative_explained_variance": float(cumulative[-1]),
        "explained_basis": explained_basis,
        "method": method,
    }


def _tier_profile(
    rows: int,
    encoded_dim: int,
    missing_ratio: float,
    compute_budget: str,
) -> dict[str, Any]:
    if rows < 1000:
        tier_class = "small"
        base_cap = 5
        variance_target = 0.82
        prior = {"pca": 0.98, "truncated_svd": 0.72, "random_projection": 0.50}
    elif rows < 10000:
        tier_class = "medium"
        base_cap = 10
        variance_target = 0.85
        prior = {"pca": 0.78, "truncated_svd": 0.96, "random_projection": 0.52}
    else:
        tier_class = "large"
        base_cap = 6
        variance_target = 0.88
        prior = {"pca": 0.28, "truncated_svd": 0.99, "random_projection": 0.68}
    budget_delta = {"low": -2, "balanced": 0, "high": 2}[compute_budget]
    missing_bonus = 1 if missing_ratio > 0.10 else 0
    dimension_cap = base_cap + budget_delta + missing_bonus
    upper = max(2, min(encoded_dim, rows - 1))
    dimension_cap = max(2, min(dimension_cap, upper))
    return {
        "tier_class": tier_class,
        "dimension_cap": dimension_cap,
        "base_dimension_cap": base_cap,
        "variance_target": variance_target,
        "missing_dimension_bonus": missing_bonus,
        "compute_budget_delta": budget_delta,
        "algorithm_prior": prior,
    }


def _choose_curve_dimension(
    curve: dict[str, Any],
    profile: dict[str, Any],
    recon_limit: float,
) -> tuple[int, str]:
    max_dim = profile["dimension_cap"]
    cumulative = curve["cumulative_curve"]
    recon = curve["reconstruction_curve"]
    for index, cumulative_value in enumerate(cumulative):
        dimension = index + 2
        if (
            cumulative_value >= profile["variance_target"]
            and recon[index] <= recon_limit
        ):
            return dimension, (
                f"smallest k>={dimension} meeting variance >= "
                f"{profile['variance_target']:.3f} and reconstruction_error <= {recon_limit:.3f}"
            )
    return max_dim, (
        f"cap reached: no k<={max_dim} met both variance and reconstruction targets; "
        f"deterministic fallback is the size-aware cap"
    )


def _curve(
    kind: str,
    X: np.ndarray,
    max_dim: int,
    config: dict[str, Any],
) -> dict[str, Any]:
    """Build a deterministic 2..max_dim curve from one max-dimension fit.

    A fixed max-dimension decomposition/candidate is fitted once, then prefixes are
    evaluated. This keeps model-selection cost bounded while making every k in the
    recorded curve reproducible from the same fixed random state.
    """
    fitted = _fit_candidate(kind, X, max_dim, config)
    denominator = max(float(np.sum(X * X)), 1e-15)
    cumulative: list[float] = []
    reconstruction: list[float] = []
    for dimension in range(2, max_dim + 1):
        prefix = fitted["reduced"][:, :dimension] @ fitted["components"][:dimension]
        prefix += fitted["mean"]
        residual = X - prefix
        reconstruction.append(float(np.sum(residual * residual) / denominator))
        cumulative.append(
            float(np.sum(fitted["explained_variance_ratio"][:dimension]))
        )
    return {
        "cumulative_curve": cumulative,
        "reconstruction_curve": reconstruction,
    }


def _candidate_target(
    kind: str,
    profile: dict[str, Any],
    curves: dict[str, dict[str, Any]],
    recon_limit: float,
    rows: int,
    compute_budget: str,
) -> tuple[int, str]:
    if kind in {"pca", "truncated_svd"}:
        return _choose_curve_dimension(curves[kind], profile, recon_limit)
    if profile["tier_class"] == "small":
        target = 4
    elif profile["tier_class"] == "medium":
        target = 8
    else:
        target = 6
    if compute_budget == "low":
        target -= 2
    elif compute_budget == "high":
        target += 1
    target = max(2, min(target, profile["dimension_cap"]))
    reason = (
        f"random projection uses a size-tier target ({profile['tier_class']}) "
        f"adjusted by compute_budget={compute_budget}; cap={profile['dimension_cap']}"
    )
    return target, reason


def _score_candidate(
    candidate: dict[str, Any],
    profile: dict[str, Any],
    rows: int,
    encoded_dim: int,
    missing_ratio: float,
    compute_budget: str,
    max_cost: float,
    recon_limit: float,
) -> dict[str, Any]:
    target_dim = candidate["target_dim"]
    reconstruction_error = candidate["reconstruction_error"]
    variance_ratio = candidate["cumulative_explained_variance"]
    recon_quality = float(np.clip(1.0 - reconstruction_error, 0.0, 1.0))
    variance_quality = float(
        np.clip(variance_ratio / profile["variance_target"], 0.0, 1.0)
    )
    dimension_efficiency = float(
        np.clip(1.0 - target_dim / max(encoded_dim, 1), 0.0, 1.0)
    )
    cost_factor = {
        "pca": 1.15,
        "truncated_svd": 1.00,
        "random_projection": 0.55,
    }[candidate["candidate"]]
    cost = float(rows * encoded_dim * target_dim * cost_factor)
    cost_normalized = cost / max_cost if max_cost > 0 else 0.0
    prior = float(profile["algorithm_prior"][candidate["candidate"]])
    budget_cost_weight = {"low": 0.22, "balanced": 0.10, "high": 0.04}[compute_budget]
    budget_prior_bonus = {
        "low": {
            "pca": -0.04,
            "truncated_svd": 0.02,
            "random_projection": 0.10,
        },
        "balanced": {name: 0.0 for name in CANDIDATES},
        "high": {
            "pca": 0.05,
            "truncated_svd": 0.01,
            "random_projection": -0.03,
        },
    }[compute_budget][candidate["candidate"]]
    effective_prior = float(np.clip(prior + budget_prior_bonus, 0.0, 1.0))
    missing_penalty = 0.04 * missing_ratio
    dimension_penalty = 0.03 * target_dim / max(profile["dimension_cap"], 1)
    score = (
        0.44 * recon_quality
        + 0.22 * variance_quality
        + 0.18 * effective_prior
        + 0.10 * dimension_efficiency
        - budget_cost_weight * cost_normalized
        - missing_penalty
        - dimension_penalty
    )
    constraint_met = reconstruction_error <= recon_limit
    return {
        "score": float(score),
        "score_components": {
            "reconstruction_quality": recon_quality,
            "variance_quality": variance_quality,
            "dimension_efficiency": dimension_efficiency,
            "algorithm_prior": effective_prior,
            "cost_normalized": cost_normalized,
            "budget_cost_weight": budget_cost_weight,
            "missing_penalty": missing_penalty,
            "dimension_penalty": dimension_penalty,
        },
        "reconstruction_constraint_met": bool(constraint_met),
    }


def _elimination_reason(
    candidate: dict[str, Any], selected: dict[str, Any]
) -> str:
    gap = selected["score"] - candidate["score"]
    components = candidate["score_components"]
    labels = {
        "reconstruction_quality": "higher reconstruction error",
        "variance_quality": "lower cumulative explained variance",
        "algorithm_prior": "weaker size/budget suitability prior",
        "dimension_efficiency": "higher dimensional representation",
        "cost_normalized": "higher normalized compute cost",
    }
    ordered = sorted(
        labels,
        key=lambda key: (
            components[key] if key in {"cost_normalized", "missing_penalty", "dimension_penalty"} else -components[key]
        ),
        reverse=True,
    )
    main = labels[ordered[0]]
    return (
        f"eliminated: deterministic score {candidate['score']:.6f} is "
        f"{gap:.6f} below selected {selected['candidate']}; main factor: {main}"
    )


def compute_reduction(
    preprocessed: PreprocessedData,
    preprocess_digest: str,
    tier: str,
    config: dict[str, Any],
) -> ArtifactBundle:
    X = np.asarray(preprocessed.X, dtype=np.float64)
    rows, encoded_dim = X.shape
    metadata_pre = preprocessed.metadata
    red = config["reduction"]
    compute_budget = red["compute_budget"]
    profile = _tier_profile(
        rows, encoded_dim, metadata_pre["missing_ratio"], compute_budget
    )
    recon_limit = min(float(red["reconstruction_error_limit"]), 0.30)
    max_dim = profile["dimension_cap"]

    curves = {
        kind: _curve(kind, X, max_dim, config)
        for kind in ("pca", "truncated_svd")
    }
    # A random-projection curve is useful for an auditable comparison even though
    # its target rule is size-tier based rather than variance-threshold based.
    curves["random_projection"] = _curve("random_projection", X, max_dim, config)

    manual_target = red["target_dimension"]
    manual_candidate = red["candidate"] if red["mode"] == "manual" else None
    if manual_candidate is not None and manual_candidate not in CANDIDATES:
        raise invalid_config("Unknown manual reduction candidate", {"candidate": manual_candidate})
    if manual_target is not None:
        if manual_target < 2 or manual_target > min(rows, encoded_dim):
            raise invalid_config(
                "reduction.target_dimension is invalid for this tier",
                {
                    "target_dimension": manual_target,
                    "rows": rows,
                    "encoded_dimension": encoded_dim,
                },
            )

    candidate_targets: dict[str, tuple[int, str]] = {}
    for kind in CANDIDATES:
        if manual_target is not None:
            candidate_targets[kind] = (
                int(manual_target),
                "manual target_dimension override; all candidates compared at the same k",
            )
        else:
            candidate_targets[kind] = _candidate_target(
                kind, profile, curves, recon_limit, rows, compute_budget
            )

    fitted_candidates: dict[str, dict[str, Any]] = {}
    for kind in CANDIDATES:
        target_dim, target_reason = candidate_targets[kind]
        fitted = _fit_candidate(kind, X, target_dim, config)
        fitted["target_reason"] = target_reason
        fitted_candidates[kind] = fitted

    raw_costs = {
        kind: float(
            rows
            * encoded_dim
            * fitted_candidates[kind]["target_dim"]
            * {"pca": 1.15, "truncated_svd": 1.0, "random_projection": 0.55}[kind]
        )
        for kind in CANDIDATES
    }
    max_cost = max(raw_costs.values())
    for kind in CANDIDATES:
        fitted = fitted_candidates[kind]
        fitted.update(
            _score_candidate(
                fitted,
                profile,
                rows,
                encoded_dim,
                metadata_pre["missing_ratio"],
                compute_budget,
                max_cost,
                recon_limit,
            )
        )

    if manual_candidate is not None:
        selected_kind = manual_candidate
        selection_mode = "manual_candidate"
    else:
        selected_kind = sorted(
            CANDIDATES, key=lambda kind: (-fitted_candidates[kind]["score"], kind)
        )[0]
        selection_mode = "automatic"

    selected = fitted_candidates[selected_kind]
    candidate_records: list[dict[str, Any]] = []
    for kind in CANDIDATES:
        fitted = fitted_candidates[kind]
        status = "selected" if kind == selected_kind else "eliminated"
        record = {
            "candidate": kind,
            "target_dim": fitted["target_dim"],
            "status": status,
            "score": fitted["score"],
            "score_components": fitted["score_components"],
            "metrics": {
                "reconstruction_error": fitted["reconstruction_error"],
                "cumulative_explained_variance": fitted[
                    "cumulative_explained_variance"
                ],
                "explained_variance_ratio": [
                    float(value) for value in fitted["explained_variance_ratio"]
                ],
                "explained_basis": fitted["explained_basis"],
                "reconstruction_constraint_met": fitted[
                    "reconstruction_constraint_met"
                ],
            },
            "target_reason": fitted["target_reason"],
            "method": fitted["method"],
        }
        if status == "selected":
            record["selection_reason"] = (
                f"selected by {selection_mode}: highest deterministic score "
                f"among size-aware candidates"
            )
        else:
            record["eliminated_reason"] = _elimination_reason(fitted, selected)
        candidate_records.append(record)

    if profile["tier_class"] == "small":
        tier_reason = (
            f"small tier (rows={rows}) caps target dimension at {max_dim} to avoid "
            f"an obviously over-high representation; variance target={profile['variance_target']}"
        )
    elif profile["tier_class"] == "medium":
        tier_reason = (
            f"medium tier (rows={rows}) balances reconstruction quality and cost; "
            f"cap={max_dim}, variance target={profile['variance_target']}"
        )
    else:
        tier_reason = (
            f"large tier (rows={rows}) prefers a lower-dimensional representation under "
            f"the reconstruction constraint {recon_limit}; cap={max_dim}"
        )
    missing_reason = (
        f"missing_ratio={metadata_pre['missing_ratio']:.6f} informs robustness penalty"
    )
    budget_reason = f"compute_budget={compute_budget} adjusts cap and cost weight"
    selected_reason = [
        tier_reason,
        missing_reason,
        budget_reason,
        f"selected {selected_kind} at k={selected['target_dim']}",
        f"reconstruction_error={selected['reconstruction_error']:.6f}, "
        f"cumulative_explained_variance={selected['cumulative_explained_variance']:.6f}",
    ]
    selection = {
        "rule_version": "size-aware-v1",
        "tier_profile": profile,
        "reconstruction_error_limit": recon_limit,
        "selection_mode": selection_mode,
        "selected_candidate": selected_kind,
        "selected_target_dimension": selected["target_dim"],
        "reasons": selected_reason,
        "candidates": candidate_records,
    }
    component_ratio = np.asarray(
        selected["explained_variance_ratio"], dtype=np.float64
    )
    cumulative_ratio = np.cumsum(component_ratio)
    metadata = {
        "schema_version": "reduction-v1",
        "tier": tier,
        "rows": rows,
        "encoded_dimension": encoded_dim,
        "missing_ratio": metadata_pre["missing_ratio"],
        "candidate": selected_kind,
        "target_dimension": selected["target_dim"],
        "cumulative_explained_variance": selected[
            "cumulative_explained_variance"
        ],
        "component_explained_variance": [float(value) for value in component_ratio],
        "cumulative_explained_variance_curve": [
            float(value) for value in cumulative_ratio
        ],
        "explained_basis": selected["explained_basis"],
        "reconstruction_error": selected["reconstruction_error"],
        "reconstruction_error_std": float(
            np.std(selected["row_reconstruction_error"], ddof=1)
            / max(float(np.sum(X * X)), 1e-15)
        ),
        "reconstruction_error_limit": recon_limit,
        "method": selected["method"],
        "selection": selection,
        "preprocess_digest": preprocess_digest,
    }
    artifacts = {
        "reduced.npy": array_bytes(selected["reduced"]),
        "loadings.npy": array_bytes(selected["components"]),
        "mean.npy": array_bytes(selected["mean"]),
        "reconstruction_error.npy": array_bytes(
            selected["row_reconstruction_error"]
        ),
        "metadata.json": canonical_json_bytes(metadata),
    }
    return ArtifactBundle(artifacts=artifacts, metadata=metadata)


def load_reduction_artifacts(paths: dict[str, Path]) -> dict[str, Any]:
    required = {
        "reduced.npy",
        "loadings.npy",
        "mean.npy",
        "reconstruction_error.npy",
        "metadata.json",
    }
    missing = sorted(required - set(paths))
    if missing:
        raise invalid_stage("Reduction artifacts are incomplete", {"missing": missing})
    metadata = json.loads(paths["metadata.json"].read_text(encoding="utf-8"))
    reduced = np.asarray(load_array(paths["reduced.npy"]), dtype=np.float64)
    loadings = np.asarray(load_array(paths["loadings.npy"]), dtype=np.float64)
    mean = np.asarray(load_array(paths["mean.npy"]), dtype=np.float64)
    row_errors = np.asarray(
        load_array(paths["reconstruction_error.npy"]), dtype=np.float64
    )
    expected = (
        int(metadata["rows"]),
        int(metadata["target_dimension"]),
    )
    if reduced.shape != expected:
        raise invalid_stage(
            "Reduced matrix shape does not match metadata",
            {"shape": list(reduced.shape), "expected": list(expected)},
        )
    if loadings.shape != (expected[1], int(metadata["encoded_dimension"])):
        raise invalid_stage(
            "Loading matrix shape does not match metadata",
            {"shape": list(loadings.shape), "metadata": metadata},
        )
    if mean.shape != (int(metadata["encoded_dimension"]),):
        raise invalid_stage(
            "Reduction mean vector shape does not match metadata",
            {"shape": list(mean.shape), "metadata": metadata},
        )
    if row_errors.shape != (expected[0],):
        raise invalid_stage(
            "Per-row reconstruction error shape does not match metadata",
            {"shape": list(row_errors.shape), "expected": expected[0]},
        )
    return {
        "reduced": reduced,
        "loadings": loadings,
        "mean": mean,
        "reconstruction_error": row_errors,
        "metadata": metadata,
    }
