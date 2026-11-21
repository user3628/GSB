"""Deterministic sample generator.

Produces three CSV shards plus a JSON manifest using a fixed-seed
:class:`numpy.random.RandomState`.  Every series contains a daily cycle,
a linear trend, injected missing timestamps / missing target values and
level-shift changepoints.  Two deliberately broken series exercise the
``E_MISSING_RATIO`` and ``E_SERIES_TOO_SHORT`` business error paths.

Run::
    python scripts/generate_samples.py
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys

import numpy as np

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__),
                                                "..")))

from src.config import canonical_json  # noqa: E402
from src.data import (SERIES_COLUMN, TARGET_COLUMN, TIMESTAMP_COLUMN,
                      write_csv_shard)  # noqa: E402


SEED = 20240517
CADENCE_MS = 60 * 60 * 1000          # hourly slots
SLOTS_PER_SERIES = 120               # five daily cycles
DAILY_PERIOD = 24
HEALTHY_SERIES = ("series-a", "series-b", "series-c", "series-d")


def _base_series(series_index: int, slots: int,
                 rng: np.random.RandomState):
    t = np.arange(slots, dtype=np.float64)
    amplitude = 4.0 + series_index * 0.7
    level = 10.0 + series_index * 2.5
    trend = 0.05 * t * (1 + 0.1 * series_index)
    cycle = amplitude * np.sin(2.0 * math.pi * t / DAILY_PERIOD
                               + series_index * 0.6)
    noise = rng.normal(0.0, 0.35 + 0.05 * series_index, size=slots)
    return level + trend + cycle + noise


def _inject_changepoints(values: np.ndarray, series_index: int) -> list:
    points = []
    first = 40 + series_index * 5
    second = 85 + series_index * 3
    shift = 6.0 + series_index
    values[first:] += shift
    values[second:] -= shift * 0.8
    points.extend([first, second])
    return points


def _inject_missing(slots: int, series_index: int,
                    rng: np.random.RandomState):
    # deterministic pseudo-random but bounded missing slots, kept below 20%
    candidates = np.arange(10, slots - 10)
    chosen = rng.choice(candidates, size=4, replace=False)
    missing_ts = set(int(x) for x in chosen[:2])
    missing_target = set(int(x) for x in chosen[2:])
    return missing_ts, missing_target


def generate(output_dir: str) -> dict:
    rng = np.random.RandomState(SEED)
    os.makedirs(output_dir, exist_ok=True)
    shard_entries = []
    header = [TIMESTAMP_COLUMN, SERIES_COLUMN, TARGET_COLUMN,
              "exo_0", "exo_1"]

    for shard_index in range(3):
        shard_id = f"shard-0{shard_index + 1}"
        start_ms = shard_index * SLOTS_PER_SERIES * CADENCE_MS
        timestamps = [start_ms + slot * CADENCE_MS
                      for slot in range(SLOTS_PER_SERIES)]
        rows = []
        series_ids = list(HEALTHY_SERIES)
        # broken series live only in the last shard to keep shard 1/2 clean
        if shard_index == 2:
            series_ids += ["series-broken-missing", "series-broken-short"]

        for series_index, series_id in enumerate(series_ids):
            if series_id == "series-broken-short":
                slots = 6  # shorter than one configured window
            else:
                slots = SLOTS_PER_SERIES
            values = _base_series(series_index, slots, rng)
            if series_id.startswith("series-") and not \
                    series_id.startswith("series-broken"):
                _inject_changepoints(values, series_index)
            if series_id == "series-broken-missing":
                missing_ts = set(range(20, 70, 2))   # ~42% missing rows
                missing_target = set()
            elif series_id == "series-broken-short":
                missing_ts, missing_target = set(), set()
            else:
                missing_ts, missing_target = _inject_missing(
                    slots, series_index, rng)

            exo0 = rng.normal(0.0, 1.0, size=slots)
            exo1 = rng.normal(2.0, 0.5, size=slots)
            for slot in range(slots):
                if slot in missing_ts:
                    continue  # missing timestamp: no CSV row at all
                target = "" if slot in missing_target \
                    else f"{values[slot]:.10f}"
                rows.append([
                    timestamps[slot], series_id, target,
                    f"{exo0[slot]:.10f}", f"{exo1[slot]:.10f}",
                ])

        filename = f"{shard_id}.csv"
        path = os.path.join(output_dir, filename)
        write_csv_shard(path, header, rows)
        import hashlib
        fingerprint = hashlib.sha256(
            canonical_json(_canonical_rows(path)).encode("utf-8")
        ).hexdigest()
        shard_entries.append({
            "shard_id": shard_id,
            "path": filename,
            "rows": len(rows),
            "timestamp_start_ms": start_ms,
            "timestamp_end_ms": start_ms
            + (SLOTS_PER_SERIES - 1) * CADENCE_MS,
            "cadence_ms": CADENCE_MS,
            "series": sorted(series_ids),
            "series_grid_slots": (
                {"series-broken-short": 6}
                if shard_index == 2 else {}),
            "content_fingerprint": fingerprint,
        })

    manifest = {
        "generator_seed": SEED,
        "cadence_ms": CADENCE_MS,
        "slots_per_series": SLOTS_PER_SERIES,
        "shards": shard_entries,
    }
    manifest_path = os.path.join(output_dir, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(manifest, sort_keys=True, indent=2))
    return manifest


def _canonical_rows(path: str):
    import csv
    with open(path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = []
        for row in reader:
            parsed = {}
            for key, value in row.items():
                if key == TIMESTAMP_COLUMN:
                    parsed[key] = int(value)
                elif value == "":
                    parsed[key] = None
                elif key != SERIES_COLUMN:
                    parsed[key] = float(value)
                else:
                    parsed[key] = value
            rows.append(parsed)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="generate deterministic "
                                                 "sample shards")
    parser.add_argument("--output", default="samples")
    args = parser.parse_args()
    manifest = generate(args.output)
    for entry in manifest["shards"]:
        print(f"generated {entry['path']}: {entry['rows']} rows, "
              f"{len(entry['series'])} series, "
              f"fingerprint={entry['content_fingerprint'][:12]}...")
    print("manifest: samples/manifest.json")


if __name__ == "__main__":
    main()
