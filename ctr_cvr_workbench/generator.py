"""Deterministic local JSONL shard generator (stdlib + numpy only)."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Any

import numpy as np


GENERATOR_FORMAT = "ctr-cvr-shards-v1"

CITIES = ["shanghai", "beijing", "shenzhen", "hangzhou", "chengdu", "wuhan"]
DEVICES = ["ios", "android", "web"]
MEMBER_TIERS = ["basic", "silver", "gold"]
ITEM_CATEGORIES = ["electronics", "fashion", "grocery", "travel", "beauty"]
BRAND_TIERS = ["value", "standard", "premium"]
NETWORKS = ["wifi", "5g", "4g"]
SOURCES = ["feed", "search", "external"]
LANGS = ["zh", "en"]

CITY_EFFECT = dict(zip(CITIES, [0.22, -0.08, 0.16, 0.04, -0.18, 0.0]))
DEVICE_EFFECT = dict(zip(DEVICES, [0.18, 0.08, -0.20]))
MEMBER_EFFECT = dict(zip(MEMBER_TIERS, [-0.12, 0.18, 0.38]))
CATEGORY_EFFECT = dict(zip(ITEM_CATEGORIES, [0.28, 0.16, 0.02, 0.22, 0.12]))
BRAND_EFFECT = dict(zip(BRAND_TIERS, [-0.15, 0.05, 0.22]))
NETWORK_EFFECT = dict(zip(NETWORKS, [0.10, 0.05, -0.12]))


def _sigmoid(value: float) -> float:
    if value >= 0:
        return 1.0 / (1.0 + math.exp(-value))
    exponent = math.exp(value)
    return exponent / (1.0 + exponent)


def _decorate(rng: np.random.Generator, value: str) -> str:
    draw = float(rng.random())
    if draw < 0.12:
        return value.upper()
    if draw < 0.24:
        return f"  {value}  "
    if draw < 0.34:
        fullwidth = "".join(
            chr(ord(character) + 0xFEE0) if 33 <= ord(character) <= 126 else character
            for character in value
        )
        return f" {fullwidth} "
    return value


def _canonical_line(value: Any) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        + "\n"
    ).encode("utf-8")


def _drop_if_missing(
    rng: np.random.Generator,
    mapping: dict[str, Any],
    key: str,
    path: str,
    missing_counts: dict[str, int],
    probability: float = 0.045,
) -> None:
    if float(rng.random()) < probability:
        mapping.pop(key, None)
        missing_counts[path] = missing_counts.get(path, 0) + 1


def write_dataset(
    out_dir: str | Path,
    *,
    seed: int = 20261122,
    shards: int = 6,
    rows_per_shard: int = 220,
) -> dict[str, Any]:
    if shards <= 0 or rows_per_shard <= 0:
        raise ValueError("shards and rows_per_shard must be positive")
    output = Path(out_dir)
    output.mkdir(parents=True, exist_ok=True)
    manifest_shards: list[dict[str, Any]] = []

    for shard_index in range(shards):
        rng = np.random.Generator(np.random.PCG64(seed + shard_index * 104729))
        rows = rows_per_shard + (shard_index % 3) * 13
        records: list[dict[str, Any]] = []
        missing_counts: dict[str, int] = {}
        ctr_positive = 0
        cvr_positive = 0
        cvr_observed = 0
        shard_offset = (shard_index % 4 - 1.5) * 0.055

        for row_index in range(rows):
            city = CITIES[int(rng.integers(0, len(CITIES)))]
            device = DEVICES[int(rng.integers(0, len(DEVICES)))]
            member = MEMBER_TIERS[int(rng.integers(0, len(MEMBER_TIERS)))]
            category = ITEM_CATEGORIES[int(rng.integers(0, len(ITEM_CATEGORIES)))]
            brand = BRAND_TIERS[int(rng.integers(0, len(BRAND_TIERS)))]
            network = NETWORKS[int(rng.integers(0, len(NETWORKS)))]
            source = SOURCES[int(rng.integers(0, len(SOURCES)))]
            lang = LANGS[int(rng.integers(0, len(LANGS)))]

            age = int(rng.integers(18, 66))
            activity = float(np.round(rng.beta(2.2, 2.0), 6))
            price = float(np.round(np.exp(rng.uniform(math.log(8.0), math.log(650.0))), 2))
            hour = int(rng.integers(0, 24))

            hour_effect = 0.12 if 18 <= hour <= 22 else (-0.08 if 1 <= hour <= 5 else 0.0)
            ctr_logit = (
                -0.92
                + shard_offset
                + CITY_EFFECT[city]
                + DEVICE_EFFECT[device]
                + MEMBER_EFFECT[member]
                + 0.10 * BRAND_EFFECT[brand]
                + NETWORK_EFFECT[network]
                + 0.82 * (activity - 0.5)
                + 0.18 * math.log(max(price, 1.0))
                + hour_effect
            )
            ctr_probability = _sigmoid(ctr_logit)
            ctr_label = int(float(rng.random()) < ctr_probability)

            cvr_logit = (
                -2.25
                + 0.34 * CATEGORY_EFFECT[category]
                + 0.42 * MEMBER_EFFECT[member]
                - 0.08 * DEVICE_EFFECT[device]
                + 0.55 * (activity - 0.5)
                - 0.12 * math.log(max(price, 1.0))
                + 0.35 * BRAND_EFFECT[brand]
            )
            cvr_probability = _sigmoid(cvr_logit)

            user_features: dict[str, Any] = {
                "age": age,
                "activity_score": activity,
                "city": _decorate(rng, city),
                "device": _decorate(rng, device),
                "member_tier": _decorate(rng, member),
            }
            item_features: dict[str, Any] = {
                "price": price,
                "category": _decorate(rng, category),
                "brand_tier": _decorate(rng, brand),
            }
            context_features: dict[str, Any] = {
                "hour": hour,
                "network": _decorate(rng, network),
            }

            _drop_if_missing(rng, user_features, "age", "user_features.age", missing_counts)
            _drop_if_missing(
                rng, user_features, "activity_score", "user_features.activity_score", missing_counts
            )
            _drop_if_missing(rng, user_features, "city", "user_features.city", missing_counts)
            _drop_if_missing(rng, user_features, "device", "user_features.device", missing_counts)
            _drop_if_missing(
                rng, user_features, "member_tier", "user_features.member_tier", missing_counts
            )
            _drop_if_missing(rng, item_features, "price", "item_features.price", missing_counts)
            _drop_if_missing(
                rng, item_features, "category", "item_features.category", missing_counts
            )
            _drop_if_missing(
                rng, item_features, "brand_tier", "item_features.brand_tier", missing_counts
            )
            _drop_if_missing(
                rng, context_features, "hour", "context_features.hour", missing_counts
            )
            _drop_if_missing(
                rng, context_features, "network", "context_features.network", missing_counts
            )

            record: dict[str, Any] = {
                "impression_id": f"imp-{shard_index:02d}-{row_index:06d}",
                "user_features": user_features,
                "item_features": item_features,
                "context_features": context_features,
                "ctr_label": ctr_label,
                "source": source,
                "lang": lang,
            }
            if ctr_label == 1:
                cvr_label = int(float(rng.random()) < cvr_probability)
                record["cvr_label"] = cvr_label
                cvr_observed += 1
                ctr_positive += 1
                cvr_positive += cvr_label
            records.append(record)

        shard_id = f"shard-{shard_index:05d}"
        filename = f"{shard_id}.jsonl"
        payload = b"".join(_canonical_line(record) for record in records)
        shard_path = output / filename
        shard_path.write_bytes(payload)
        digest = hashlib.sha256(payload).hexdigest()
        actual_rows = len(records)
        manifest_shards.append(
            {
                "shard_id": shard_id,
                "index": shard_index,
                "path": filename,
                "rows": actual_rows,
                "ctr_positive_count": ctr_positive,
                "ctr_positive_ratio": ctr_positive / actual_rows,
                "cvr_positive_count": cvr_positive,
                "cvr_positive_ratio": cvr_positive / actual_rows,
                "cvr_observed_count": cvr_observed,
                "cvr_observed_ratio": cvr_observed / actual_rows,
                "conversion_rate_on_exposure": cvr_positive / ctr_positive if ctr_positive else 0.0,
                "missing_fields": dict(sorted(missing_counts.items())),
                "sha256": digest,
                "bytes": len(payload),
            }
        )

    manifest = {
        "format": GENERATOR_FORMAT,
        "seed": seed,
        "generated_by": "ctr_cvr_workbench.generator",
        "shards": manifest_shards,
    }
    manifest_path = output / "manifest.json"
    manifest_path.write_bytes(_canonical_line(manifest))
    return {
        "format": GENERATOR_FORMAT,
        "seed": seed,
        "manifest": manifest_path.as_posix(),
        "shards": len(manifest_shards),
        "rows": sum(item["rows"] for item in manifest_shards),
        "ctr_positive_ratio": sum(item["ctr_positive_count"] for item in manifest_shards)
        / sum(item["rows"] for item in manifest_shards),
        "cvr_positive_ratio": sum(item["cvr_positive_count"] for item in manifest_shards)
        / sum(item["rows"] for item in manifest_shards),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate deterministic local CTR/CVR JSONL shards")
    parser.add_argument("--out", default="data/shards", help="relative output directory")
    parser.add_argument("--seed", type=int, default=20261122)
    parser.add_argument("--shards", type=int, default=6)
    parser.add_argument("--rows-per-shard", type=int, default=220)
    args = parser.parse_args()
    summary = write_dataset(
        args.out,
        seed=args.seed,
        shards=args.shards,
        rows_per_shard=args.rows_per_shard,
    )
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
