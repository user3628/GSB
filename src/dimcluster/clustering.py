from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics import silhouette_score

from .config import stage_config
from .errors import invalid_config, invalid_stage
from .stage import ArtifactBundle
from .util import (
    array_bytes,
    canonical_json_bytes,
    load_array,
    sha256_bytes,
    stable_round,
)


def cluster_input_fingerprint(
    reduction_digest: str,
    reduction_metadata: dict[str, Any],
    config: dict[str, Any],
) -> str:
    payload = {
        "reduction_digest": reduction_digest,
        "rows": reduction_metadata["rows"],
        "target_dimension": reduction_metadata["target_dimension"],
        "clustering": stage_config(config, "cluster"),
    }
    return sha256_bytes(canonical_json_bytes(payload))


def _kmeans_plus_plus(
    X: np.ndarray, k: int, rng: np.random.Generator
) -> np.ndarray:
    rows = X.shape[0]
    centers = np.empty((k, X.shape[1]), dtype=np.float64)
    first = int(rng.integers(0, rows))
    centers[0] = X[first]
    closest = np.sum((X - centers[0]) ** 2, axis=1)
    for index in range(1, k):
        total = float(np.sum(closest))
        if total <= 1e-15:
            candidate = int(rng.integers(0, rows))
        else:
            probabilities = closest / total
            candidate = int(rng.choice(rows, p=probabilities))
        centers[index] = X[candidate]
        distance = np.sum((X - centers[index]) ** 2, axis=1)
        closest = np.minimum(closest, distance)
    return centers


def _assign(X: np.ndarray, centers: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    x_norm = np.sum(X * X, axis=1)[:, None]
    c_norm = np.sum(centers * centers, axis=1)[None, :]
    distances = x_norm + c_norm - 2.0 * (X @ centers.T)
    np.maximum(distances, 0.0, out=distances)
    labels = np.argmin(distances, axis=1)
    selected = distances[np.arange(X.shape[0]), labels]
    return labels.astype(np.int64), selected


def _centers_from_labels(
    X: np.ndarray, labels: np.ndarray, k: int
) -> np.ndarray:
    centers = np.zeros((k, X.shape[1]), dtype=np.float64)
    counts = np.bincount(labels, minlength=k)
    np.add.at(centers, labels, X)
    nonempty = counts > 0
    centers[nonempty] /= counts[nonempty, None]
    return centers


def _canonicalize_labels(
    X: np.ndarray, labels: np.ndarray
) -> tuple[np.ndarray, np.ndarray, dict[int, int], np.ndarray]:
    unique = sorted(int(value) for value in np.unique(labels))
    old_centers = np.vstack(
        [np.mean(X[labels == old_label], axis=0) for old_label in unique]
    )
    dimensions = old_centers.shape[1]
    keys = tuple(old_centers[:, index] for index in range(dimensions - 1, -1, -1))
    order = np.lexsort(keys)
    mapping = {unique[int(old_index)]: int(new_index) for new_index, old_index in enumerate(order)}
    canonical = np.asarray([mapping[int(label)] for label in labels], dtype=np.int64)
    centers = old_centers[order]
    return canonical, centers, mapping, old_centers


def _kmeans_lloyd(
    X: np.ndarray,
    k: int,
    seed: int,
    max_iter: int,
    tol: float,
    empty_policy: str,
) -> dict[str, Any]:
    if k < 2:
        raise invalid_config("KMeans requires at least 2 clusters", {"n_clusters": k})
    if k > X.shape[0]:
        raise invalid_config(
            "n_clusters cannot exceed rows",
            {"n_clusters": k, "rows": int(X.shape[0])},
        )
    if empty_policy != "relocate_farthest":
        raise invalid_config(
            "Unsupported empty cluster policy", {"empty_cluster_policy": empty_policy}
        )
    rng = np.random.default_rng(seed)
    centers = _kmeans_plus_plus(X, k, rng)
    history: list[dict[str, Any]] = []
    relocations: list[dict[str, Any]] = []
    converged = False
    previous_labels: np.ndarray | None = None

    for iteration in range(1, max_iter + 1):
        old_centers = centers.copy()
        labels, point_distances = _assign(X, centers)
        counts = np.bincount(labels, minlength=k)
        empty_before = [int(index) for index in np.flatnonzero(counts == 0)]
        moved_points: set[int] = set()
        iteration_relocations: list[dict[str, Any]] = []
        for empty_cluster in empty_before:
            eligible = [
                int(index)
                for index in np.argsort(-point_distances, kind="stable")
                if int(labels[index]) != empty_cluster
                and int(counts[int(labels[index])]) > 1
                and int(index) not in moved_points
            ]
            if not eligible:
                raise invalid_stage(
                    "Empty cluster relocation found no eligible donor point",
                    {"iteration": iteration, "empty_cluster": empty_cluster},
                )
            point_index = eligible[0]
            donor_cluster = int(labels[point_index])
            labels[point_index] = empty_cluster
            counts[donor_cluster] -= 1
            counts[empty_cluster] += 1
            moved_points.add(point_index)
            event = {
                "iteration": iteration,
                "empty_cluster": int(empty_cluster),
                "donor_cluster": donor_cluster,
                "point_index": point_index,
                "distance_to_donor_center": float(point_distances[point_index]),
            }
            relocations.append(event)
            iteration_relocations.append(event)

        new_centers = _centers_from_labels(X, labels, k)
        shift = float(
            np.max(np.linalg.norm(new_centers - old_centers, axis=1))
        ) if k else 0.0
        sizes = np.bincount(labels, minlength=k).astype(int)
        history.append(
            {
                "iteration": iteration,
                "centroid_shift": shift,
                "cluster_sizes": [int(value) for value in sizes],
                "empty_clusters_before": empty_before,
                "relocations": iteration_relocations,
                "converged_after_iteration": False,
            }
        )
        labels_unchanged = (
            previous_labels is not None and np.array_equal(labels, previous_labels)
        )
        centers = new_centers
        previous_labels = labels.copy()
        if labels_unchanged or shift <= tol:
            history[-1]["converged_after_iteration"] = True
            converged = True
            break

    canonical_labels, canonical_centers, mapping, original_centers = _canonicalize_labels(
        X, labels
    )
    sizes = np.bincount(canonical_labels, minlength=k).astype(int)
    inertia = float(
        np.sum(
            (
                X
                - canonical_centers[canonical_labels]
            )
            ** 2
        )
    )
    return {
        "algorithm": "kmeans",
        "labels": canonical_labels,
        "centers": stable_round(canonical_centers),
        "sizes": sizes,
        "inertia": inertia,
        "iterations": history,
        "converged": converged,
        "relocations": relocations,
        "empty_cluster_policy": empty_policy,
        "raw_label_to_canonical": {
            str(key): int(value) for key, value in sorted(mapping.items())
        },
    }


def _hierarchical(
    X: np.ndarray, k: int
) -> dict[str, Any]:
    if k < 2:
        raise invalid_config("Hierarchical clustering requires at least 2 clusters")
    if k > X.shape[0]:
        raise invalid_config(
            "n_clusters cannot exceed rows",
            {"n_clusters": k, "rows": int(X.shape[0])},
        )
    model = AgglomerativeClustering(
        n_clusters=k,
        linkage="ward",
        metric="euclidean",
    )
    raw_labels = np.asarray(model.fit_predict(X), dtype=np.int64)
    labels, centers, mapping, _ = _canonicalize_labels(X, raw_labels)
    sizes = np.bincount(labels, minlength=k).astype(int)
    inertia = float(np.sum((X - centers[labels]) ** 2))
    history = [
        {
            "iteration": 1,
            "centroid_shift": 0.0,
            "cluster_sizes": [int(value) for value in sizes],
            "empty_clusters_before": [],
            "relocations": [],
            "converged_after_iteration": True,
            "note": "Agglomerative ward clustering is a single merge pass; no Lloyd iterations occur.",
        }
    ]
    return {
        "algorithm": "hierarchical",
        "labels": labels,
        "centers": stable_round(centers),
        "sizes": sizes,
        "inertia": inertia,
        "iterations": history,
        "converged": True,
        "relocations": [],
        "empty_cluster_policy": "not_applicable_agglomerative",
        "raw_label_to_canonical": {
            str(key): int(value) for key, value in sorted(mapping.items())
        },
    }


def _deterministic_silhouette(
    X: np.ndarray,
    labels: np.ndarray,
    sample_size: int,
    seed: int,
) -> float | None:
    if len(np.unique(labels)) < 2:
        return None
    rows = X.shape[0]
    if sample_size >= rows:
        return float(silhouette_score(X, labels, metric="euclidean"))
    rng = np.random.default_rng(seed)
    indexes = np.sort(rng.choice(rows, size=sample_size, replace=False))
    sampled_labels = labels[indexes]
    if len(np.unique(sampled_labels)) < 2:
        return None
    return float(
        silhouette_score(
            X[indexes], sampled_labels, metric="euclidean"
        )
    )


def _run_algorithm(
    algorithm: str,
    X: np.ndarray,
    k: int,
    config: dict[str, Any],
) -> dict[str, Any]:
    clu = config["clustering"]
    if algorithm == "kmeans":
        return _kmeans_lloyd(
            X,
            k,
            int(clu["random_state"]) + 7919 * k,
            int(clu["max_iter"]),
            float(clu["tol"]),
            clu["empty_cluster_policy"],
        )
    if algorithm == "hierarchical":
        return _hierarchical(X, k)
    raise invalid_config("Unknown clustering algorithm", {"algorithm": algorithm})


def compute_clustering(
    reduced: np.ndarray,
    reduction_digest: str,
    reduction_metadata: dict[str, Any],
    tier: str,
    config: dict[str, Any],
) -> ArtifactBundle:
    X = np.asarray(reduced, dtype=np.float64)
    rows = X.shape[0]
    clu = config["clustering"]
    requested_algorithm = clu["algorithm"]
    if requested_algorithm == "auto":
        selected_algorithm = "hierarchical" if rows < 1000 else "kmeans"
        algorithm_reason = (
            "auto: small tiers use deterministic Ward hierarchical clustering; "
            "medium/large tiers use memory-bounded Lloyd KMeans"
        )
    else:
        selected_algorithm = requested_algorithm
        algorithm_reason = f"manual algorithm override: {selected_algorithm}"

    explicit_k = clu["n_clusters"]
    search_records: list[dict[str, Any]] = []
    cache: dict[int, dict[str, Any]] = {}
    if explicit_k is not None:
        if explicit_k > rows:
            raise invalid_config(
                "clustering.n_clusters cannot exceed tier rows",
                {"n_clusters": explicit_k, "rows": rows, "tier": tier},
            )
        selected_k = int(explicit_k)
        k_reason = "manual clustering.n_clusters override"
        selected_result = _run_algorithm(
            selected_algorithm, X, selected_k, config
        )
        cache[selected_k] = selected_result
    elif clu["auto_k"]:
        k_min = int(clu["k_min"])
        k_max = min(int(clu["k_max"]), rows - 1)
        if k_min > k_max:
            raise invalid_config(
                "No valid auto-k range for tier",
                {"k_min": clu["k_min"], "k_max": clu["k_max"], "rows": rows},
            )
        sample_size = min(2000, rows)
        for k in range(k_min, k_max + 1):
            result = _run_algorithm(selected_algorithm, X, k, config)
            cache[k] = result
            score = _deterministic_silhouette(
                X,
                result["labels"],
                sample_size,
                int(clu["random_state"]) + 104729,
            )
            search_records.append(
                {
                    "k": k,
                    "algorithm": selected_algorithm,
                    "silhouette_sample": score,
                    "inertia": result["inertia"],
                    "cluster_sizes": [int(value) for value in result["sizes"]],
                }
            )
        eligible = [item for item in search_records if item["silhouette_sample"] is not None]
        if not eligible:
            raise invalid_stage(
                "auto-k could not evaluate any candidate", {"tier": tier}
            )
        best = sorted(
            eligible,
            key=lambda item: (-float(item["silhouette_sample"]), int(item["k"])),
        )[0]
        selected_k = int(best["k"])
        selected_result = cache[selected_k]
        k_reason = (
            "auto-k selected the highest deterministic sampled silhouette; "
            "ties resolve to the smaller k"
        )
    else:
        raise invalid_config(
            "clustering.n_clusters is required when auto_k is false"
        )

    labels = np.asarray(selected_result["labels"], dtype=np.int64)
    if labels.shape != (rows,):
        raise invalid_stage(
            "Cluster labels are not aligned with the reduced representation",
            {"rows": rows, "label_shape": list(labels.shape)},
        )
    if np.asarray(selected_result["centers"]).shape != (
        selected_k,
        X.shape[1],
    ):
        raise invalid_stage(
            "Cluster centers are not aligned with the reduced representation",
            {
                "reduced_shape": list(X.shape),
                "center_shape": list(np.asarray(selected_result["centers"]).shape),
                "n_clusters": selected_k,
            },
        )
    sizes = np.bincount(labels, minlength=selected_k).astype(int)
    dominant_ratio = float(np.max(sizes) / rows) if rows else 0.0
    metadata = {
        "schema_version": "clustering-v1",
        "tier": tier,
        "rows": rows,
        "requested_algorithm": requested_algorithm,
        "algorithm": selected_algorithm,
        "algorithm_reason": algorithm_reason,
        "n_clusters": selected_k,
        "n_clusters_requested": explicit_k,
        "auto_k": bool(explicit_k is None and clu["auto_k"]),
        "k_reason": k_reason,
        "k_search": search_records,
        "cluster_sizes": [int(value) for value in sizes],
        "dominant_cluster_ratio": dominant_ratio,
        "inertia": selected_result["inertia"],
        "iterations": selected_result["iterations"],
        "empty_cluster_policy": selected_result["empty_cluster_policy"],
        "empty_clusters_handled": len(selected_result["relocations"]),
        "relocations": selected_result["relocations"],
        "converged": bool(selected_result["converged"]),
        "label_convention": "lexicographic_centroid_v1",
        "raw_label_to_canonical": selected_result["raw_label_to_canonical"],
        "reduction_digest": reduction_digest,
        "reduction_candidate": reduction_metadata["candidate"],
        "reduction_dimension": reduction_metadata["target_dimension"],
    }
    artifacts = {
        "labels.npy": array_bytes(labels, kind="int"),
        "centers.npy": array_bytes(selected_result["centers"]),
        "metadata.json": canonical_json_bytes(metadata),
    }
    return ArtifactBundle(artifacts=artifacts, metadata=metadata)


def load_cluster_artifacts(paths: dict[str, Path]) -> dict[str, Any]:
    required = {"labels.npy", "centers.npy", "metadata.json"}
    missing = sorted(required - set(paths))
    if missing:
        raise invalid_stage("Clustering artifacts are incomplete", {"missing": missing})
    metadata = json.loads(paths["metadata.json"].read_text(encoding="utf-8"))
    labels = np.asarray(load_array(paths["labels.npy"]), dtype=np.int64)
    centers = np.asarray(load_array(paths["centers.npy"]), dtype=np.float64)
    if labels.shape != (int(metadata["rows"]),):
        raise invalid_stage(
            "Cluster label artifact shape does not match metadata",
            {"shape": list(labels.shape), "metadata": metadata},
        )
    if centers.shape != (
        int(metadata["n_clusters"]),
        int(metadata["reduction_dimension"]),
    ):
        raise invalid_stage(
            "Cluster center artifact shape does not match metadata",
            {"shape": list(centers.shape), "metadata": metadata},
        )
    return {
        "labels": labels,
        "centers": centers,
        "metadata": metadata,
    }
