#!/usr/bin/env python3
"""Fixed-seed generator for local evaluation shards and calibration configs.

Produces (deterministically, seed=20261122):
  data/shards/shard_v1_a.jsonl   schema v1, batch b2026_01, zh/en mixed
  data/shards/shard_v1_b.jsonl   schema v1, batch b2026_02
  data/shards/shard_v2_a.jsonl   schema v2, batch b2026_01
  data/shards/shard_v2_b.jsonl   schema v2, batch b2026_02
  data/configs/config_v1.json    legacy config (alias method, bins_pct)
  data/configs/config_v2.json    native v2 config, semantically equal to v1
  data/inject/shard_bad_version.jsonl   declared v2 but missing float score
  data/inject/config_out_of_range.json  n_bins out of range
  data/inject/config_unknown_alias.json unknown v1 method alias
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

SEED = 20261122
MODELS = ["gpt-zh-01", "gpt-en-02"]
LANGS = ["zh", "en"]
BATCHES = ["b2026_01", "b2026_02"]


def make_scores_labels(rng: np.random.Generator, n: int):
    logits = rng.normal(0.2, 1.4, size=n)
    probs = 1.0 / (1.0 + np.exp(-logits))
    labels = (rng.random(n) < probs).astype(int)
    # two-decimal scores: v1 percent precision, lossless across versions
    pcts = np.clip(np.round(probs * 100).astype(int), 0, 100)
    return pcts, labels


def v1_line(rng, model_id, batch, seq, pct, label):
    return {
        "schema_version": 1,
        "model_id": model_id,
        "sample_uid": f"{model_id}:{batch}:{seq:05d}",
        "score_pct": int(pct),
        "label": int(label),
        "lang": LANGS[rng.integers(0, 2)],
        "batch": batch,
    }


def v2_line(rng, model_id, batch, seq, pct, label, n_bins=10):
    score = pct / 100.0
    return {
        "schema_version": 2,
        "model_id": model_id,
        "sample_id": f"{seq:05d}",
        "score": score,
        "label": int(label),
        "lang": LANGS[rng.integers(0, 2)],
        "batch": batch,
        "bucket": min(int(score * n_bins), n_bins - 1),
    }


def write_jsonl(path: Path, rows: list[dict]):
    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", default="data", help="output root directory")
    parser.add_argument("--seed", type=int, default=SEED)
    args = parser.parse_args()

    root = Path(args.out)
    (root / "shards").mkdir(parents=True, exist_ok=True)
    (root / "configs").mkdir(parents=True, exist_ok=True)
    (root / "inject").mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(args.seed)

    specs = [
        ("shard_v1_a", 1, "b2026_01", 60),
        ("shard_v1_b", 1, "b2026_02", 48),
        ("shard_v2_a", 2, "b2026_01", 56),
        ("shard_v2_b", 2, "b2026_02", 44),
    ]
    for name, version, batch, n in specs:
        pcts, labels = make_scores_labels(rng, n)
        rows = []
        for i in range(n):
            model_id = MODELS[rng.integers(0, 2)]
            if version == 1:
                rows.append(v1_line(rng, model_id, batch, i + 1, pcts[i], labels[i]))
            else:
                rows.append(v2_line(rng, model_id, batch, i + 1, pcts[i], labels[i]))
        write_jsonl(root / "shards" / f"{name}.jsonl", rows)
        print(f"wrote {name}: {n} records (v{version}, batch {batch})")

    config_v2 = {
        "config_version": 2,
        "n_bins": 10,
        "binning": "equal_width",
        "calibration": {"method": "platt"},
        "reliability_min_samples": 8,
        "expected_positive_rate": [0.0, 1.0],
        "rounding": "half_up",
        "id_delimiter": ":",
    }
    config_v1 = {
        "config_version": 1,
        "method": "platt_scaling",
        "bins_pct": 10,
        "reliability_min_samples": 8,
    }
    for name, cfg in (("config_v1.json", config_v1), ("config_v2.json", config_v2)):
        with open(root / "configs" / name, "w", encoding="utf-8") as fh:
            json.dump(cfg, fh, ensure_ascii=False, sort_keys=True, indent=2)
            fh.write("\n")
        print(f"wrote configs/{name}")

    # --- injected fault samples (provided as input files, no source edits) ---
    bad_version_rows = [
        {"schema_version": 2, "model_id": "gpt-zh-01", "sample_id": "00001",
         "label": 1, "lang": "zh", "batch": "b2026_01", "bucket": 3},
        {"schema_version": 2, "model_id": "gpt-en-02", "sample_uid": "gpt-en-02:b2026_01:00002",
         "score_pct": 41, "label": 0, "lang": "en", "batch": "b2026_01"},
    ]
    write_jsonl(root / "inject" / "shard_bad_version.jsonl", bad_version_rows)
    with open(root / "inject" / "config_out_of_range.json", "w", encoding="utf-8") as fh:
        json.dump({"config_version": 2, "n_bins": 0, "binning": "equal_width",
                   "calibration": {"method": "platt"}, "reliability_min_samples": 8,
                   "expected_positive_rate": [0.2, 0.8]}, fh, indent=2)
        fh.write("\n")
    with open(root / "inject" / "config_unknown_alias.json", "w", encoding="utf-8") as fh:
        json.dump({"config_version": 1, "method": "magic_calibrator", "bins_pct": 10},
                fh, indent=2)
        fh.write("\n")
    print("wrote inject/ fault samples")
    print(f"seed={args.seed} (fixed); all outputs deterministic")


if __name__ == "__main__":
    main()
