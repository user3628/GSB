"""Deterministic local sample generator.

Produces JSONL shards and a YAML scheduling config from a fixed seed.
Re-running with the same seed yields byte-identical files.
"""

from __future__ import annotations

import json
import os
import random

import yaml

DEFAULT_SEED = 20261121
CATEGORIES = ["news", "video", "docs", "shopping"]
LANGUAGES = ["zh", "en"]
LOCALES = {"zh": "zh-CN", "en": "en-US"}
CHANNELS = ["web", "app", "api"]
FEATURES = ["ctr", "dwell", "freshness", "authority"]


def _round6(x: float) -> float:
    return float(f"{x:.6f}")


def build_shard_records(rng: random.Random, shard_index: int,
                        groups_per_shard: int = 3,
                        min_candidates: int = 24,
                        max_candidates: int = 48) -> list[dict]:
    records = []
    for g in range(groups_per_shard):
        query_id = f"q-s{shard_index}-{g}"
        lang = LANGUAGES[(shard_index + g) % len(LANGUAGES)]
        records.append({
            "type": "query_group",
            "query_id": query_id,
            "locale": LOCALES[lang],
            "channel": CHANNELS[rng.randrange(len(CHANNELS))],
        })
        n = rng.randrange(min_candidates, max_candidates + 1)
        for c in range(n):
            records.append({
                "type": "candidate",
                "query_id": query_id,
                "doc_id": f"doc-s{shard_index}-g{g}-c{c:03d}",
                "coarse_score": _round6(rng.random()),
                "category": CATEGORIES[rng.randrange(len(CATEGORIES))],
                "language": LANGUAGES[rng.randrange(len(LANGUAGES))],
                "features": {
                    "ctr": _round6(rng.random()),
                    "dwell": _round6(rng.random() * 300.0),
                    "freshness": _round6(rng.random()),
                    "authority": _round6(rng.random() * 10.0),
                },
            })
    return records


def default_config() -> dict:
    return {
        "config_version": "1.0.0",
        "features": list(FEATURES),
        "precision": {
            "weights": {"ctr": 0.35, "dwell": 0.002, "freshness": 0.15,
                        "authority": 0.05},
            "embedding_weight": 0.20,
            "embedding_dim": 8,
            "quantize_scale": 1000000,
        },
        "rerank": {
            "window_size": 8,
            "dedup_penalty": 5.0,
            "category_penalty": 0.08,
            "language_penalty": 0.05,
        },
        "parallel": {
            "max_workers": 4,
            "global_eval_budget": 100000,
            "per_worker_quota": 50000,
            "shard_latency_budget_ms": 60000,
        },
    }


def generate(data_dir, seed: int = DEFAULT_SEED, num_shards: int = 4) -> dict:
    """Generate shards + config into data_dir. Returns a manifest."""
    os.makedirs(data_dir, exist_ok=True)
    rng = random.Random(seed)
    manifest = {"seed": seed, "num_shards": num_shards, "shards": []}
    for shard_index in range(num_shards):
        records = build_shard_records(rng, shard_index)
        name = f"shard-{shard_index:03d}.jsonl"
        path = os.path.join(data_dir, name)
        with open(path, "w", encoding="utf-8", newline="\n") as fh:
            for rec in records:
                fh.write(json.dumps(rec, ensure_ascii=False, sort_keys=True))
                fh.write("\n")
        n_candidates = sum(1 for r in records if r["type"] == "candidate")
        manifest["shards"].append({
            "shard_id": f"shard-{shard_index:03d}",
            "shard_index": shard_index,
            "file": name,
            "candidates": n_candidates,
        })
    config_path = os.path.join(data_dir, "schedule.yaml")
    with open(config_path, "w", encoding="utf-8", newline="\n") as fh:
        yaml.safe_dump(default_config(), fh, allow_unicode=True, sort_keys=False)
    manifest_path = os.path.join(data_dir, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(manifest, fh, ensure_ascii=False, sort_keys=True, indent=2)
        fh.write("\n")
    return manifest
