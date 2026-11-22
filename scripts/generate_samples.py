"""Generate deterministic local samples used by tests and the end-to-end demo."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from gitops_convergence.models import Application, Cluster
from gitops_convergence.validation import compute_checksum


SAMPLES = ROOT / "samples"


def dump(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )


def build_large_graph() -> dict[str, Any]:
    clusters = [
        {"cluster_id": "cluster-alpha", "replica_quota": 220},
        {"cluster_id": "cluster-beta", "replica_quota": 220},
        {"cluster_id": "cluster-gamma", "replica_quota": 220},
    ]
    cluster_ids = [item["cluster_id"] for item in clusters]
    applications: list[dict[str, Any]] = []
    layers = 30
    width = 100
    for index in range(layers * width):
        layer, column = divmod(index, width)
        dependencies: list[str] = []
        if layer > 0:
            first = (layer - 1) * width + ((column * 7 + 3) % width)
            second = (layer - 1) * width + ((column * 13 + 11) % width)
            dependencies = sorted({f"app-{first:04d}", f"app-{second:04d}"})
        pending = index < 6
        applications.append(
            {
                "app_id": f"app-{index:04d}",
                "cluster_id": cluster_ids[(layer + column) % len(cluster_ids)],
                "desired_version": "v1.1" if pending else "v1.0",
                "effective_version": "v1.0",
                "replica_budget": (index % 4) + 1,
                "dependencies": dependencies,
            }
        )
    spec = model_spec(1, "2026-01-01T00:00:00Z", clusters, applications)
    return snapshot_document(spec)


def model_spec(
    version: int,
    generated_at: str,
    clusters: list[dict[str, Any]],
    applications: list[dict[str, Any]],
) -> dict[str, Any]:
    cluster_models = [
        Cluster(item["cluster_id"], item["replica_quota"]) for item in clusters
    ]
    app_models = [
        Application(
            app_id=item["app_id"],
            cluster_id=item["cluster_id"],
            desired_version=item["desired_version"],
            effective_version=item["effective_version"],
            replica_budget=item["replica_budget"],
            dependencies=tuple(item["dependencies"]),
        )
        for item in applications
    ]
    checksum = compute_checksum(version, generated_at, cluster_models, app_models)
    return {
        "version": version,
        "generated_at": generated_at,
        "clusters": clusters,
        "applications": applications,
        "checksum": checksum,
    }


def snapshot_document(spec: dict[str, Any]) -> dict[str, Any]:
    return spec


def find_app(spec: dict[str, Any], app_id: str) -> dict[str, Any]:
    return next(item for item in spec["applications"] if item["app_id"] == app_id)


def build_increment(
    base: dict[str, Any],
    *,
    version: int,
    generated_at: str,
    changes: list[dict[str, Any]],
) -> dict[str, Any]:
    resulting = resulting_spec(base, version, generated_at, changes)
    return {
        "version": version,
        "base_version": base["version"],
        "generated_at": generated_at,
        "base_checksum": base["checksum"],
        "changes": changes,
        "checksum": resulting["checksum"],
    }


def resulting_spec(
    base: dict[str, Any],
    version: int,
    generated_at: str,
    changes: list[dict[str, Any]],
) -> dict[str, Any]:
    applications = [dict(item, dependencies=list(item["dependencies"])) for item in base["applications"]]
    by_id = {item["app_id"]: item for item in applications}
    for change in changes:
        target = by_id[change["app_id"]]
        for field in ("desired_version", "effective_version", "replica_budget", "cluster_id"):
            if field in change:
                target[field] = change[field]
        if "dependencies" in change:
            target["dependencies"] = sorted(change["dependencies"])
    return model_spec(version, generated_at, base["clusters"], applications)


def main() -> None:
    SAMPLES.mkdir(parents=True, exist_ok=True)
    base = build_large_graph()
    dump(SAMPLES / "large_cluster_graph.json", base)

    single = build_increment(
        base,
        version=2,
        generated_at="2026-01-01T00:05:00Z",
        changes=[
            {
                "app_id": "app-2999",
                "desired_version": "v2.0",
            }
        ],
    )
    dump(SAMPLES / "increment_single_app.json", single)
    single_result = resulting_spec(
        base,
        2,
        single["generated_at"],
        [{"app_id": "app-2999", "desired_version": "v2.0"}],
    )

    rollback = build_increment(
        single_result,
        version=3,
        generated_at="2026-01-01T00:10:00Z",
        changes=[
            {
                "app_id": "app-2885",
                "desired_version": "v0.9",
                "rollback": True,
            }
        ],
    )
    dump(SAMPLES / "rollback_cascade_snapshot.json", rollback)
    rollback_changes = [
        {
            "app_id": "app-2885",
            "desired_version": "v0.9",
            "rollback": True,
        }
    ]
    rollback_result = resulting_spec(
        single_result, 3, rollback["generated_at"], rollback_changes
    )

    conflict_a = build_increment(
        rollback_result,
        version=4,
        generated_at="2026-01-01T00:15:00Z",
        changes=[{"app_id": "app-2998", "desired_version": "v2.0"}],
    )
    conflict_b = build_increment(
        rollback_result,
        version=4,
        generated_at="2026-01-01T00:15:01Z",
        changes=[{"app_id": "app-2997", "desired_version": "v2.0"}],
    )
    dump(SAMPLES / "increment_conflict_a.json", conflict_a)
    dump(SAMPLES / "increment_conflict_b.json", conflict_b)

    print(
        json.dumps(
            {
                "samples_dir": str(SAMPLES),
                "applications": len(base["applications"]),
                "dependency_edges": sum(len(app["dependencies"]) for app in base["applications"]),
                "files": sorted(path.name for path in SAMPLES.glob("*.json")),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
