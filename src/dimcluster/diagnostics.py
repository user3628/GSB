from __future__ import annotations

import json
import math
import statistics
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.metrics import silhouette_score

from .config import stage_config
from .errors import invalid_stage
from .util import canonical_json_bytes, sha256_bytes


def diagnose_input_fingerprint(
    cluster_digest: str,
    cluster_metadata: dict[str, Any],
    reduction_metadata: dict[str, Any],
    config: dict[str, Any],
) -> str:
    payload = {
        "cluster_digest": cluster_digest,
        "reduction_digest": reduction_metadata.get("preprocess_digest"),
        "rows": cluster_metadata["rows"],
        "n_clusters": cluster_metadata["n_clusters"],
        "diagnostics": stage_config(config, "diagnose"),
    }
    return sha256_bytes(canonical_json_bytes(payload))


def _z_value(confidence: float) -> float:
    return float(statistics.NormalDist().inv_cdf((1.0 + confidence) / 2.0))


def _band(
    value: float,
    halfwidth: float,
    n_effective: int,
    method: str,
) -> dict[str, Any]:
    return {
        "value": float(value),
        "lower": float(value - halfwidth),
        "upper": float(value + halfwidth),
        "halfwidth": float(halfwidth),
        "n_effective": int(n_effective),
        "confidence": None,
        "method": method,
    }


def _hoeffding_halfwidth(n_effective: int, confidence: float, value_range: float = 2.0) -> float:
    alpha = 1.0 - confidence
    return float(value_range * math.sqrt(math.log(2.0 / alpha) / (2.0 * max(n_effective, 1))))


def _silhouette(
    X: np.ndarray,
    labels: np.ndarray,
    sample_size: int,
    seed: int,
) -> tuple[float | None, int, str, np.ndarray]:
    rows = X.shape[0]
    if len(np.unique(labels)) < 2:
        return None, min(rows, sample_size), "undefined_single_cluster", np.asarray([], dtype=np.int64)
    if rows <= sample_size:
        indexes = np.arange(rows, dtype=np.int64)
        method = "full"
    else:
        rng = np.random.default_rng(seed)
        indexes = np.sort(rng.choice(rows, size=sample_size, replace=False))
        method = "deterministic_subsample"
    sampled_labels = labels[indexes]
    if len(np.unique(sampled_labels)) < 2:
        indexes = np.arange(rows, dtype=np.int64)
        sampled_labels = labels
        method = "full_fallback_subsample_not_clusterable"
    value = float(silhouette_score(X[indexes], sampled_labels, metric="euclidean"))
    return value, int(len(indexes)), method, indexes


def _alternative_k(cluster_meta: dict[str, Any], dominant_triggered: bool) -> dict[str, Any] | None:
    selected = int(cluster_meta["n_clusters"])
    candidates = [
        item for item in cluster_meta.get("k_search", []) if int(item["k"]) != selected
    ]
    if not candidates:
        return None
    if dominant_triggered:
        def key(item: dict[str, Any]) -> tuple[float, float, int]:
            rows = sum(item["cluster_sizes"])
            dominant = max(item["cluster_sizes"]) / max(rows, 1)
            silhouette_value = (
                float(item["silhouette_sample"])
                if item["silhouette_sample"] is not None
                else -1.0
            )
            return (dominant, -silhouette_value, int(item["k"]))

        return sorted(candidates, key=key)[0]
    return sorted(
        candidates,
        key=lambda item: (
            -(float(item["silhouette_sample"]) if item["silhouette_sample"] is not None else -1.0),
            int(item["k"]),
        ),
    )[0]


def compute_diagnosis(
    reduced: np.ndarray,
    labels: np.ndarray,
    centers: np.ndarray,
    reduction_metadata: dict[str, Any],
    cluster_metadata: dict[str, Any],
    tier: str,
    config: dict[str, Any],
    row_reconstruction_errors: np.ndarray,
) -> dict[str, Any]:
    X = np.asarray(reduced, dtype=np.float64)
    labels = np.asarray(labels, dtype=np.int64)
    centers = np.asarray(centers, dtype=np.float64)
    rows = X.shape[0]
    if labels.shape != (rows,):
        raise invalid_stage(
            "Diagnosis labels are not aligned with the reduced representation",
            {"rows": rows, "label_shape": list(labels.shape)},
        )
    if centers.shape != (int(cluster_metadata["n_clusters"]), X.shape[1]):
        raise invalid_stage(
            "Diagnosis centers are not aligned with the reduced representation",
            {
                "reduced_shape": list(X.shape),
                "center_shape": list(centers.shape),
                "n_clusters": cluster_metadata["n_clusters"],
            },
        )
    diag = config["diagnostics"]
    confidence = float(diag["confidence"])
    z_value = _z_value(confidence)

    silhouette_value, n_effective, silhouette_method, _ = _silhouette(
        X,
        labels,
        int(diag["silhouette_sample_size"]),
        int(config["clustering"]["random_state"]) + 15485863,
    )
    if silhouette_value is None:
        silhouette_band = {
            "value": None,
            "lower": None,
            "upper": None,
            "halfwidth": None,
            "n_effective": n_effective,
            "confidence": confidence,
            "method": silhouette_method,
        }
    else:
        silhouette_halfwidth = _hoeffding_halfwidth(
            n_effective, confidence, value_range=2.0
        )
        silhouette_band = _band(
            silhouette_value,
            silhouette_halfwidth,
            n_effective,
            f"bounded Hoeffding interval; {silhouette_method}",
        )
        silhouette_band["confidence"] = confidence

    squared_distances = np.sum((X - centers[labels]) ** 2, axis=1)
    inertia = float(np.sum(squared_distances))
    inertia_per_sample = float(np.mean(squared_distances))
    inertia_se = float(np.std(squared_distances, ddof=1) / math.sqrt(rows)) if rows > 1 else 0.0
    inertia_band = _band(
        inertia_per_sample,
        z_value * inertia_se,
        rows,
        "mean point squared distance; normal standard-error interval",
    )
    inertia_band["confidence"] = confidence
    inertia_total_halfwidth = z_value * float(
        np.std(squared_distances, ddof=1) * math.sqrt(rows)
    ) if rows > 1 else 0.0

    total_x_energy = max(float(np.sum(X * X)), 1e-15)
    row_reconstruction = np.asarray(row_reconstruction_errors, dtype=np.float64) / total_x_energy
    reconstruction_error = float(reduction_metadata["reconstruction_error"])
    if row_reconstruction.size == rows:
        recon_se = float(np.std(row_reconstruction, ddof=1) / math.sqrt(rows)) if rows > 1 else 0.0
        reconstruction_band = _band(
            reconstruction_error,
            z_value * recon_se,
            rows,
            "per-row relative squared reconstruction error; normal standard-error interval",
        )
    else:
        reconstruction_band = _band(
            reconstruction_error,
            z_value * float(reduction_metadata.get("reconstruction_error_std", 0.0)) / math.sqrt(rows),
            rows,
            "stored reduction standard error",
        )
    reconstruction_band["confidence"] = confidence

    cumulative_variance = float(reduction_metadata["cumulative_explained_variance"])
    cumulative_band = _band(
        cumulative_variance,
        _hoeffding_halfwidth(rows, confidence, value_range=1.0),
        rows,
        "bounded interval for cumulative explained variance",
    )
    cumulative_band["confidence"] = confidence

    sizes = np.bincount(labels, minlength=int(cluster_metadata["n_clusters"]))
    dominant_ratio = float(np.max(sizes) / rows) if rows else 0.0
    silhouette_floor = float(diag["silhouette_floor"])
    dominant_floor = float(diag["dominant_cluster_ratio"])
    silhouette_triggered = silhouette_value is not None and silhouette_value < silhouette_floor
    dominant_triggered = dominant_ratio > dominant_floor
    flagged = bool(silhouette_triggered or dominant_triggered)

    alternate_k = _alternative_k(cluster_metadata, dominant_triggered)
    alternate_reductions = sorted(
        [
            candidate
            for candidate in reduction_metadata["selection"]["candidates"]
            if candidate["candidate"] != reduction_metadata["candidate"]
        ],
        key=lambda item: (-float(item["score"]), item["candidate"]),
    )
    fallback_reduction = alternate_reductions[0] if alternate_reductions else None
    fallback: dict[str, Any] = {
        "required": flagged,
        "reduction_candidate": fallback_reduction["candidate"] if fallback_reduction else None,
        "reduction_target_dimension": fallback_reduction["target_dim"] if fallback_reduction else None,
        "reduction_basis": (
            fallback_reduction.get("eliminated_reason") if fallback_reduction else None
        ),
        "cluster_count": int(alternate_k["k"]) if alternate_k else None,
        "cluster_basis": (
            {
                "sampled_silhouette": alternate_k["silhouette_sample"],
                "cluster_sizes": alternate_k["cluster_sizes"],
            }
            if alternate_k
            else None
        ),
        "actions": [],
        "reason": [],
    }
    if silhouette_triggered:
        fallback["reason"].append(
            f"silhouette {silhouette_value:.6f} is below floor {silhouette_floor:.6f}"
        )
        fallback["actions"].append("try_alternative_cluster_count")
        fallback["actions"].append("try_alternative_reduction_candidate")
    if dominant_triggered:
        fallback["reason"].append(
            f"dominant cluster ratio {dominant_ratio:.6f} exceeds threshold {dominant_floor:.6f}"
        )
        fallback["actions"].append("try_alternative_cluster_count")
        if not silhouette_triggered:
            fallback["actions"].append("try_alternative_reduction_candidate")
    if flagged:
        fallback["summary"] = (
            "Fallback recommended: rerun with cluster count "
            f"{fallback['cluster_count']} and/or reduction candidate "
            f"{fallback['reduction_candidate']}; basis is recorded in tier metrics."
        )
    else:
        fallback["summary"] = "No fallback required by current thresholds."

    return {
        "schema_version": "diagnosis-v1",
        "tier": tier,
        "rows": rows,
        "status": "fallback_recommended" if flagged else "ok",
        "metrics": {
            "silhouette": silhouette_band,
            "within_cluster_sum_of_squares": {
                "value": inertia,
                "cluster_sizes": [int(value) for value in sizes],
                "dominant_cluster_ratio": dominant_ratio,
                "dominant_ratio_error_band": {
                    "lower": max(0.0, dominant_ratio - z_value * math.sqrt(max(dominant_ratio * (1.0 - dominant_ratio), 0.0) / max(rows, 1))),
                    "upper": min(1.0, dominant_ratio + z_value * math.sqrt(max(dominant_ratio * (1.0 - dominant_ratio), 0.0) / max(rows, 1))),
                    "n_effective": rows,
                    "confidence": confidence,
                    "method": "binomial standard-error interval",
                },
                "error_band": {
                    "lower": max(0.0, inertia - inertia_total_halfwidth),
                    "upper": inertia + inertia_total_halfwidth,
                    "halfwidth": inertia_total_halfwidth,
                    "n_effective": rows,
                    "confidence": confidence,
                    "method": "sum of per-point squared distances; normal standard-error interval",
                },
            },
            "inertia_per_sample": inertia_band,
            "reconstruction_error": reconstruction_band,
            "cumulative_explained_variance": cumulative_band,
        },
        "thresholds": {
            "silhouette_floor": silhouette_floor,
            "dominant_cluster_ratio": dominant_floor,
            "silhouette_sample_size": int(diag["silhouette_sample_size"]),
            "confidence": confidence,
        },
        "triggers": {
            "silhouette_below_floor": bool(silhouette_triggered),
            "dominant_cluster_ratio_exceeded": bool(dominant_triggered),
        },
        "fallback": fallback,
        "selection_context": {
            "reduction": {
                "candidate": reduction_metadata["candidate"],
                "target_dimension": reduction_metadata["target_dimension"],
                "rule_version": reduction_metadata["selection"]["rule_version"],
                "reasons": reduction_metadata["selection"]["reasons"],
                "candidate_scores": [
                    {
                        "candidate": item["candidate"],
                        "target_dim": item["target_dim"],
                        "score": item["score"],
                        "status": item["status"],
                        "eliminated_reason": item.get("eliminated_reason"),
                    }
                    for item in reduction_metadata["selection"]["candidates"]
                ],
            },
            "clustering": {
                "algorithm": cluster_metadata["algorithm"],
                "algorithm_reason": cluster_metadata["algorithm_reason"],
                "n_clusters": cluster_metadata["n_clusters"],
                "k_reason": cluster_metadata["k_reason"],
                "k_search": cluster_metadata["k_search"],
                "iterations": len(cluster_metadata["iterations"]),
                "empty_clusters_handled": cluster_metadata["empty_clusters_handled"],
                "empty_cluster_policy": cluster_metadata["empty_cluster_policy"],
                "label_convention": cluster_metadata["label_convention"],
            },
        },
        "assumptions": [
            "Silhouette uses a deterministic fixed-seed subsample when rows exceed the configured sample size.",
            "Error bands are analytic approximations for local stability, not population guarantees.",
            "Report bytes exclude timestamps and checkpoint sequence numbers.",
        ],
    }


def compute_report(
    tier_diagnoses: dict[str, dict[str, Any]],
    config: dict[str, Any],
    config_sha256: str,
) -> tuple[dict[str, Any], bytes]:
    tiers = {tier: tier_diagnoses[tier] for tier in sorted(tier_diagnoses)}
    flagged = sorted(
        tier for tier, diagnosis in tiers.items() if diagnosis["status"] != "ok"
    )
    algorithm_matrix = {
        tier: {
            "reduction_candidate": diagnosis["selection_context"]["reduction"]["candidate"],
            "target_dimension": diagnosis["selection_context"]["reduction"][
                "target_dimension"
            ],
            "clustering_algorithm": diagnosis["selection_context"]["clustering"][
                "algorithm"
            ],
            "n_clusters": diagnosis["selection_context"]["clustering"]["n_clusters"],
            "silhouette": diagnosis["metrics"]["silhouette"]["value"],
            "reconstruction_error": diagnosis["metrics"]["reconstruction_error"][
                "value"
            ],
            "dominant_cluster_ratio": diagnosis["metrics"][
                "within_cluster_sum_of_squares"
            ]["dominant_cluster_ratio"],
        }
        for tier, diagnosis in tiers.items()
    }
    base: dict[str, Any] = {
        "schema_version": "diagnosis-report-v1",
        "report_version": "1",
        "seed": config["seed"],
        "config_sha256": config_sha256,
        "tier_order": list(tiers),
        "overall": {
            "tier_count": len(tiers),
            "all_ok": not flagged,
            "flagged_tiers": flagged,
            "algorithm_matrix": algorithm_matrix,
            "recommendation": (
                "All tiers pass configured thresholds."
                if not flagged
                else "Review flagged tiers and apply the recorded reduction/cluster fallback actions."
            ),
        },
        "tiers": tiers,
    }
    report_sha256 = sha256_bytes(canonical_json_bytes(base))
    report = {**base, "report_sha256": report_sha256}
    report_bytes = canonical_json_bytes(report)
    return report, report_bytes


def diagnosis_artifact(metadata: dict[str, Any]) -> bytes:
    return canonical_json_bytes(metadata)


def load_diagnosis_artifact(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise invalid_stage("Diagnosis artifact not found", {"path": str(path)})
    return json.loads(path.read_text(encoding="utf-8"))
