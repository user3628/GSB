from __future__ import annotations

import csv
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .errors import invalid_stage
from .util import atomic_write_json, canonical_json_bytes, read_json, sha256_bytes, sha256_file


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    kind: str
    latent_index: int | None = None
    categories: tuple[str, ...] = ()
    missing_rate: float = 0.0
    redundant_of: str | None = None
    noise: float = 0.0
    role: str = "feature"

    def as_manifest(self) -> dict[str, Any]:
        value: dict[str, Any] = {
            "name": self.name,
            "kind": self.kind,
            "role": self.role,
            "missing_rate_declared": self.missing_rate,
            "redundant_of": self.redundant_of,
        }
        if self.categories:
            value["categories"] = list(self.categories)
        if self.latent_index is not None:
            value["latent_index"] = self.latent_index
        if self.noise:
            value["redundancy_noise"] = self.noise
        return value


def _cont(
    name: str,
    latent_index: int,
    missing_rate: float = 0.0,
    redundant_of: str | None = None,
    noise: float = 0.02,
) -> ColumnSpec:
    return ColumnSpec(
        name=name,
        kind="continuous",
        latent_index=latent_index,
        missing_rate=missing_rate,
        redundant_of=redundant_of,
        noise=noise,
    )


def _cat(
    name: str,
    latent_index: int,
    categories: tuple[str, ...],
    missing_rate: float = 0.0,
    redundant_of: str | None = None,
) -> ColumnSpec:
    return ColumnSpec(
        name=name,
        kind="categorical",
        latent_index=latent_index,
        categories=categories,
        missing_rate=missing_rate,
        redundant_of=redundant_of,
        role="feature",
    )


def tier_specs() -> dict[str, tuple[int, int, list[ColumnSpec]]]:
    small = [
        ColumnSpec("row_id", "identifier", role="identifier"),
        _cont("age", 0),
        _cont("income", 1),
        _cat("region", 0, ("north", "south", "east", "west")),
        _cat("plan", 1, ("basic", "plus", "pro")),
        _cont("age_redundant", 0, redundant_of="age", noise=0.03),
        _cont("income_redundant", 1, redundant_of="income", noise=0.05),
        _cont("satisfaction", 2, missing_rate=0.15),
    ]
    medium: list[ColumnSpec] = [ColumnSpec("row_id", "identifier", role="identifier")]
    medium.extend(_cont(f"c{i}", i % 5) for i in range(9))
    medium.extend(
        [
            _cat("cat0", 0, ("a", "b", "c")),
            _cat("cat1", 1, ("a", "b", "c", "d")),
            _cat("cat2", 2, ("low", "mid", "high")),
            _cat("cat3", 3, ("x", "y", "z", "w")),
        ]
    )
    medium.extend(
        [
            _cont("missing_num_0", 2, missing_rate=0.12),
            _cont("missing_num_1", 3, missing_rate=0.18),
            _cont("missing_num_2", 4, missing_rate=0.09),
            _cat("missing_cat_0", 2, ("p", "q", "r"), missing_rate=0.11),
            _cat("missing_cat_1", 0, ("m", "n", "o"), missing_rate=0.16),
        ]
    )
    medium.extend(
        [
            _cont("c0_redundant", 0, redundant_of="c0", noise=0.02),
            _cont("c4_redundant", 4, redundant_of="c4", noise=0.03),
            _cont("c8_redundant", 3, redundant_of="c8", noise=0.04),
            _cat("cat0_redundant", 0, ("a", "b", "c"), redundant_of="cat0"),
            _cat("cat2_redundant", 2, ("low", "mid", "high"), redundant_of="cat2"),
        ]
    )
    large: list[ColumnSpec] = [ColumnSpec("row_id", "identifier", role="identifier")]
    large.extend(_cont(f"c{i}", i % 5) for i in range(20))
    large.extend(
        _cat(f"cat{i}", i % 5, tuple(chr(ord("a") + j) for j in range(3 + (i % 3))))
        for i in range(8)
    )
    large.extend(
        _cont(f"missing_num_{i}", i % 5, missing_rate=(0.08 + 0.02 * i))
        for i in range(6)
    )
    large.extend(
        _cat(
            f"missing_cat_{i}",
            i % 5,
            tuple(chr(ord("k") + j) for j in range(3 + (i % 2))),
            missing_rate=(0.10 + 0.015 * i),
        )
        for i in range(5)
    )
    large.extend(
        [
            _cont("c0_redundant", 0, redundant_of="c0", noise=0.02),
            _cont("c5_redundant", 0, redundant_of="c5", noise=0.03),
            _cont("c10_redundant", 0, redundant_of="c10", noise=0.025),
            _cont("c15_redundant", 0, redundant_of="c15", noise=0.035),
            _cont("c19_redundant", 4, redundant_of="c19", noise=0.04),
            _cat("cat0_redundant", 0, ("a", "b", "c"), redundant_of="cat0"),
            _cat("cat3_redundant", 3, ("a", "b", "c"), redundant_of="cat3"),
            _cat("cat7_redundant", 2, ("a", "b", "c", "d", "e"), redundant_of="cat7"),
        ]
    )
    return {
        "small": (400, 8, small),
        "medium": (4000, 24, medium),
        "large": (20000, 48, large),
    }


def _tier_seed(seed: int, tier: str) -> int:
    offset = {"small": 101, "medium": 202, "large": 303}[tier]
    return seed + offset


def _generate_values(
    rows: int, seed: int, specs: list[ColumnSpec]
) -> dict[str, list[Any]]:
    rng = np.random.default_rng(seed)
    latent_dim = 5
    centers = np.array(
        [
            [-2.0, -0.8, 0.4, 0.1, -0.5],
            [0.3, 1.8, -1.1, 0.7, 0.4],
            [2.1, -1.3, 0.8, -1.4, 0.6],
            [-1.1, -2.0, -1.6, 1.7, -1.1],
            [0.9, 0.5, 2.1, 0.2, 1.8],
        ],
        dtype=np.float64,
    )
    groups = rng.integers(0, len(centers), size=rows, dtype=np.int64)
    latent = rng.normal(size=(rows, latent_dim)) * 0.55 + centers[groups] * 0.95
    values: dict[str, list[Any]] = {}
    category_arrays: dict[str, np.ndarray] = {}

    for spec in specs:
        if spec.kind == "identifier":
            values[spec.name] = [str(i) for i in range(rows)]
            continue
        if spec.kind == "continuous":
            if spec.redundant_of is not None:
                base_name = spec.redundant_of
                if base_name in category_arrays:
                    base = category_arrays[base_name].astype(np.float64)
                else:
                    base = np.asarray(values[base_name], dtype=np.float64)
                array = base + rng.normal(scale=spec.noise, size=rows)
                array = array + rng.normal(scale=0.006, size=rows)
            else:
                coefficients = rng.normal(
                    scale=np.array([1.2, 1.0, 0.8, 0.9, 0.7]), size=latent_dim
                )
                array = latent @ coefficients
                array = array + rng.normal(scale=0.35, size=rows)
                array = array * 10.0 + float(spec.latent_index or 0) * 3.0
            missing = rng.random(rows) < spec.missing_rate
            values[spec.name] = [
                "" if missing[i] else f"{array[i]:.6f}" for i in range(rows)
            ]
            continue
        if spec.kind == "categorical":
            if spec.redundant_of is not None:
                base = category_arrays[spec.redundant_of]
                source = np.asarray(values[spec.redundant_of], dtype=object)
                array = source.copy()
                flip = rng.random(rows) < 0.004
                alternatives = np.asarray(spec.categories, dtype=object)
                array[flip] = np.asarray(
                    [alternatives[rng.integers(0, len(alternatives))] for _ in range(int(flip.sum()))],
                    dtype=object,
                )
            else:
                score = latent[:, int(spec.latent_index or 0)]
                score = score + rng.normal(scale=0.35, size=rows)
                bins = np.quantile(
                    score, np.linspace(0.0, 1.0, len(spec.categories) + 1)[1:-1]
                )
                indexes = np.digitize(score, bins, right=False)
                candidates = np.asarray(spec.categories, dtype=object)
                array = candidates[np.clip(indexes, 0, len(candidates) - 1)]
            category_arrays[spec.name] = np.asarray(array, dtype=object)
            missing = rng.random(rows) < spec.missing_rate
            values[spec.name] = ["" if missing[i] else str(array[i]) for i in range(rows)]
            continue
        raise invalid_stage(f"Unknown sample column kind: {spec.kind}")
    return values


def _column_stats(values: list[Any], spec: ColumnSpec) -> dict[str, Any]:
    non_empty = [value for value in values if value != ""]
    missing_count = len(values) - len(non_empty)
    stats: dict[str, Any] = {
        "name": spec.name,
        "kind": spec.kind,
        "non_empty_count": len(non_empty),
        "missing_count": missing_count,
        "missing_ratio": missing_count / max(len(values), 1),
    }
    if spec.kind == "continuous":
        numeric_count = 0
        for value in non_empty:
            try:
                parsed = float(value)
            except (TypeError, ValueError):
                continue
            numeric_count += int(math.isfinite(parsed))
        stats["numeric_parse_ratio"] = numeric_count / max(len(non_empty), 1)
        stats["unique_non_empty"] = len(set(non_empty))
    elif spec.kind == "categorical":
        counts: dict[str, int] = {}
        for value in non_empty:
            counts[str(value)] = counts.get(str(value), 0) + 1
        stats["observed_categories"] = sorted(counts)
        stats["observed_cardinality"] = len(counts)
        stats["category_counts"] = {key: counts[key] for key in sorted(counts)}
    return stats


def _write_csv(path: Path, columns: list[str], values: dict[str, list[Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(columns)
        for index in range(len(next(iter(values.values())))):
            writer.writerow([values[column][index] for column in columns])
        handle.flush()
    temporary.replace(path)


def generate_samples(
    samples_dir: Path,
    seed: int = 20261120,
) -> dict[str, Any]:
    samples_dir = samples_dir.resolve()
    samples_dir.mkdir(parents=True, exist_ok=True)
    files: list[dict[str, Any]] = []
    for tier, (rows, columns, specs) in tier_specs().items():
        actual_columns = len(specs)
        if actual_columns != columns:
            raise AssertionError(f"{tier}: expected {columns} columns, got {actual_columns}")
        filename = f"{tier}_{rows}x{columns}.csv"
        values = _generate_values(rows, _tier_seed(seed, tier), specs)
        column_names = [spec.name for spec in specs]
        path = samples_dir / filename
        _write_csv(path, column_names, values)
        stats = [_column_stats(values[spec.name], spec) for spec in specs]
        missing_cells = sum(item["missing_count"] for item in stats)
        semantic_columns = []
        for spec, stat in zip(specs, stats):
            semantic = spec.as_manifest()
            semantic["detected"] = stat
            semantic_columns.append(semantic)
        files.append(
            {
                "tier": tier,
                "file": filename,
                "rows": rows,
                "columns": columns,
                "sha256": sha256_file(path),
                "column_semantics": semantic_columns,
                "summary": {
                    "missing_cells": missing_cells,
                    "missing_ratio": missing_cells / (rows * columns),
                    "continuous_columns": sum(
                        1 for spec in specs if spec.kind == "continuous"
                    ),
                    "categorical_columns": sum(
                        1 for spec in specs if spec.kind == "categorical"
                    ),
                    "missing_columns": sum(
                        1 for item in stats if item["missing_count"] > 0
                    ),
                    "redundant_columns": sum(
                        1 for spec in specs if spec.redundant_of is not None
                    ),
                },
            }
        )
    manifest = {
        "schema_version": "sample-manifest-v1",
        "generation_algorithm": "latent-cluster-v1",
        "seed": seed,
        "files": files,
    }
    manifest_path = samples_dir / "manifest.json"
    atomic_write_json(manifest_path, manifest, pretty=True)
    summary = {
        "schema_version": "generation-summary-v1",
        "seed": seed,
        "manifest": manifest_path.name,
        "manifest_sha256": sha256_file(manifest_path),
        "tiers": [
            {
                "tier": item["tier"],
                "file": item["file"],
                "rows": item["rows"],
                "columns": item["columns"],
                "sha256": item["sha256"],
                "missing_ratio": item["summary"]["missing_ratio"],
            }
            for item in files
        ],
    }
    summary["summary_sha256"] = sha256_bytes(canonical_json_bytes(summary))
    atomic_write_json(samples_dir / "generation_summary.json", summary, pretty=True)
    return summary


def verify_samples(manifest_path: Path) -> dict[str, Any]:
    manifest_path = manifest_path.resolve()
    if not manifest_path.exists():
        raise invalid_stage("Sample manifest is missing", {"path": str(manifest_path)})
    manifest = read_json(manifest_path)
    errors: list[str] = []
    checks: list[dict[str, Any]] = []
    for item in manifest.get("files", []):
        path = (manifest_path.parent / item["file"]).resolve()
        exists = path.exists()
        actual_sha = sha256_file(path) if exists else None
        row_count = 0
        column_count = 0
        if exists:
            with path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.reader(handle)
                header = next(reader, [])
                column_count = len(header)
                row_count = sum(1 for _ in reader)
        ok = (
            exists
            and row_count == item["rows"]
            and column_count == item["columns"]
            and actual_sha == item["sha256"]
        )
        if not ok:
            errors.append(
                f"{item['tier']}: exists={exists}, rows={row_count}/{item['rows']}, "
                f"columns={column_count}/{item['columns']}, sha_match={actual_sha == item['sha256']}"
            )
        checks.append(
            {
                "tier": item["tier"],
                "file": item["file"],
                "ok": ok,
                "rows": row_count,
                "columns": column_count,
                "sha256": actual_sha,
            }
        )
    if errors:
        raise invalid_stage("Sample verification failed", {"errors": errors, "checks": checks})
    summary_path = manifest_path.parent / "generation_summary.json"
    summary = read_json(summary_path) if summary_path.exists() else None
    return {
        "ok": True,
        "manifest": str(manifest_path),
        "manifest_sha256": sha256_file(manifest_path),
        "checks": checks,
        "generation_summary_matches": (
            summary is None or summary.get("manifest_sha256") == sha256_file(manifest_path)
        ),
    }
