from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

import numpy as np


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from join_shuffle_engine.snapshot import save_snapshot  # noqa: E402


SEED = 20261121
V1_TIME = "2026-01-15T00:00:00.000Z"
V2_TIME = "2026-02-15T00:00:00.000Z"


def _with_nulls(measures: np.ndarray, null_rate: float, rng: np.random.Generator):
    measures = np.asarray(measures, dtype="<f8").copy()
    nulls = (rng.random(measures.shape[0]) < null_rate).astype("|u1")
    measures[nulls.astype(bool)] = 0.0
    return measures, nulls


def _mixed_measures(count: int, rng: np.random.Generator, null_rate: float):
    signs = np.where(rng.random(count) < 0.42, -1.0, 1.0)
    magnitudes = 10.0 ** rng.uniform(-4.0, 12.0, size=count)
    return _with_nulls(signs * magnitudes, null_rate, rng)


def _prepare_target(path: Path, force: bool) -> None:
    if path.exists():
        if not force:
            raise FileExistsError(f"snapshot already exists: {path}; use --force to replace")
        shutil.rmtree(path)


def generate_samples(root: Path, *, seed: int = SEED, force: bool = False) -> list[dict]:
    root.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)
    created: list[dict] = []

    # Regular dataset: duplicate left keys, 1-3 right rows, null measures, two versions.
    key_space = 1200
    left_keys_v1 = rng.integers(0, key_space, size=9000, dtype=np.int64)
    left_measures_v1, left_nulls_v1 = _mixed_measures(9000, rng, 0.07)
    right_keys_parts = []
    right_measures_parts = []
    right_nulls_parts = []
    for key in range(key_space):
        if key % 17 == 5:
            continue
        multiplicity = int(rng.integers(1, 4))
        right_keys_parts.append(np.full(multiplicity, key, dtype=np.int64))
        values, nulls = _mixed_measures(multiplicity, rng, 0.09)
        right_measures_parts.append(values)
        right_nulls_parts.append(nulls)
    right_keys_v1 = np.concatenate(right_keys_parts)
    right_measures_v1 = np.concatenate(right_measures_parts)
    right_nulls_v1 = np.concatenate(right_nulls_parts)

    left_measures_v2 = left_measures_v1.copy()
    left_nulls_v2 = left_nulls_v1.copy()
    for index in range(0, left_measures_v2.shape[0], 311):
        if not left_nulls_v2[index]:
            left_measures_v2[index] += 0.125
    left_measures_v2[13] = 123.5
    left_nulls_v2[13] = 0
    right_measures_v2 = right_measures_v1.copy()
    right_nulls_v2 = right_nulls_v1.copy()
    for index in range(0, right_measures_v2.shape[0], 197):
        if not right_nulls_v2[index]:
            right_measures_v2[index] -= 0.0625

    regular = {
        "demo-left-v1": (
            "left",
            1,
            V1_TIME,
            left_keys_v1,
            left_measures_v1,
            left_nulls_v1,
        ),
        "demo-right-v1": (
            "right",
            1,
            V1_TIME,
            right_keys_v1,
            right_measures_v1,
            right_nulls_v1,
        ),
        "demo-left-v2": (
            "left",
            2,
            V2_TIME,
            left_keys_v1,
            left_measures_v2,
            left_nulls_v2,
        ),
        "demo-right-v2": (
            "right",
            2,
            V2_TIME,
            right_keys_v1,
            right_measures_v2,
            right_nulls_v2,
        ),
    }
    for snapshot_id, (side, version, generated_at, keys, measures, nulls) in regular.items():
        path = root / snapshot_id
        _prepare_target(path, force)
        snapshot = save_snapshot(
            path,
            snapshot_id=snapshot_id,
            side=side,
            version=version,
            generated_at=generated_at,
            keys=keys,
            measures=measures,
            nulls=nulls,
            measure_dimension="dimensionless",
            measure_unit="score",
        )
        created.append(snapshot.metadata())

    # Precision dataset. Group 1001 has 1e16 followed by 1000 ones; naive summation
    # loses the small increments while compensated summation preserves them.
    precision_keys = np.concatenate(
        (
            np.full(1001, 1001, dtype=np.int64),
            np.array([2002, 2002, 3003, 3003], dtype=np.int64),
        )
    )
    precision_values = np.concatenate(
        (
            np.array([1.0e16] + [1.0] * 1000, dtype="<f8"),
            np.array([5.0, 7.0, 2.0, 3.0], dtype="<f8"),
        )
    )
    precision_nulls = np.zeros(precision_keys.shape[0], dtype="|u1")
    precision_right_keys = np.array([1001, 3003, 3003], dtype=np.int64)
    precision_right_values = np.array([1.0, 0.0, 0.0], dtype="<f8")
    precision_right_nulls = np.array([0, 1, 1], dtype="|u1")
    precision = {
        "precision-left-v1": (
            "left",
            precision_keys,
            precision_values,
            precision_nulls,
        ),
        "precision-right-v1": (
            "right",
            precision_right_keys,
            precision_right_values,
            precision_right_nulls,
        ),
    }
    for snapshot_id, (side, keys, measures, nulls) in precision.items():
        path = root / snapshot_id
        _prepare_target(path, force)
        snapshot = save_snapshot(
            path,
            snapshot_id=snapshot_id,
            side=side,
            version=1,
            generated_at=V1_TIME,
            keys=keys,
            measures=measures,
            nulls=nulls,
            measure_dimension="dimensionless",
            measure_unit="score",
        )
        created.append(snapshot.metadata())

    # Skew dataset: key 7 contributes most of the input and therefore dominates one hash partition.
    skew_hot = np.full(7000, 7, dtype=np.int64)
    skew_other = np.arange(1500, dtype=np.int64) % 750 + 100
    skew_left_keys = np.concatenate((skew_hot, skew_other))
    skew_left_values, skew_left_nulls = _mixed_measures(
        skew_left_keys.shape[0], rng, 0.04
    )
    skew_right_keys = np.concatenate(
        (np.array([7], dtype=np.int64), np.arange(100, 850, dtype=np.int64))
    )
    skew_right_values, skew_right_nulls = _mixed_measures(
        skew_right_keys.shape[0], rng, 0.05
    )
    skew = {
        "skew-left-v1": (
            "left",
            skew_left_keys,
            skew_left_values,
            skew_left_nulls,
        ),
        "skew-right-v1": (
            "right",
            skew_right_keys,
            skew_right_values,
            skew_right_nulls,
        ),
    }
    for snapshot_id, (side, keys, measures, nulls) in skew.items():
        path = root / snapshot_id
        _prepare_target(path, force)
        snapshot = save_snapshot(
            path,
            snapshot_id=snapshot_id,
            side=side,
            version=1,
            generated_at=V1_TIME,
            keys=keys,
            measures=measures,
            nulls=nulls,
            measure_dimension="dimensionless",
            measure_unit="score",
        )
        created.append(snapshot.metadata())

    return created


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate deterministic columnar snapshots")
    parser.add_argument("--root", default=str(PROJECT_ROOT / "data" / "snapshots"))
    parser.add_argument("--seed", type=int, default=SEED)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    created = generate_samples(Path(args.root), seed=args.seed, force=args.force)
    print(f"seed={args.seed} root={Path(args.root).resolve()}")
    for item in created:
        print(
            f"{item['snapshot_id']}: side={item['side']} version={item['version']} "
            f"rows={item['row_count']} nulls={item['null_count']} "
            f"sha256={item['content_sha256']}"
        )
    print(f"generated_snapshots={len(created)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
