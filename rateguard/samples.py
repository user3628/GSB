"""Deterministic local sample generation for the reproducible demo."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import numpy as np
except ImportError as exc:  # pragma: no cover - exercised only on missing dependency
    raise SystemExit("numpy is required to generate fixtures: python -m pip install numpy") from exc


BASE_TIMESTAMP_MS = 1_700_000_000_000
EVENT_DURATION_MS = 64_000
OUTAGE_START_MS = 24_000
OUTAGE_END_MS = 44_000
GENERATOR_VERSION = "1"


@dataclass(frozen=True, slots=True)
class TierSpec:
    name: str
    seed: int
    event_count: int
    batch_size_ms: int = 4_000


TIERS: dict[str, TierSpec] = {
    "small": TierSpec("small", 37001, 72),
    "medium": TierSpec("medium", 37002, 720),
    "large": TierSpec("large", 37003, 4_800),
}


def _latency_payload(rng: np.random.Generator, outage: bool) -> tuple[int, float]:
    if outage:
        return int(min(450_000, max(120_000, 150_000 + rng.random() * 260_000))), 0.12
    return int(min(50_000, max(12_000, rng.lognormal(np.log(30_000), 0.18)))), 0.995


def generate_tier(spec: TierSpec) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rng = np.random.default_rng(spec.seed)
    offsets = np.sort(rng.integers(0, EVENT_DURATION_MS, size=spec.event_count))
    events: list[dict[str, Any]] = []
    outage_checkout_ordinal = 0

    for index, offset in enumerate(offsets.tolist()):
        route = f"/{spec.name}/api/catalog" if index % 4 == 0 else f"/{spec.name}/api/checkout"
        outage = (
            route.endswith("/checkout")
            and OUTAGE_START_MS <= offset < OUTAGE_END_MS
        )
        latency_us, success_probability = _latency_payload(rng, outage)
        success = bool(rng.random() < success_probability)
        if outage:
            # Force a deterministic block of failures so every tier exercises the
            # consecutive-failure/rate rules even at small sample sizes.
            if outage_checkout_ordinal < 12:
                success = False
            outage_checkout_ordinal += 1
        events.append(
            {
                "timestamp_ms": BASE_TIMESTAMP_MS + int(offset),
                "route": route,
                "success": success,
                "latency_us": latency_us,
            }
        )

    batches_by_index: dict[int, list[dict[str, Any]]] = {}
    for event in events:
        relative = event["timestamp_ms"] - BASE_TIMESTAMP_MS
        batch_index = relative // spec.batch_size_ms
        batches_by_index.setdefault(int(batch_index), []).append(event)

    batches: list[dict[str, Any]] = []
    for batch_index in range(EVENT_DURATION_MS // spec.batch_size_ms):
        batch_events = batches_by_index.get(batch_index, [])
        if not batch_events:
            continue
        batch_id = f"{spec.name}-b{batch_index + 1:03d}"
        batches.append(
            {
                "batch_id": batch_id,
                "sequence": 1,
                "events": batch_events,
            }
        )

    route_counts: dict[str, int] = {}
    success_counts: dict[str, int] = {}
    for event in events:
        route = event["route"]
        route_counts[route] = route_counts.get(route, 0) + 1
        success_counts[route] = success_counts.get(route, 0) + int(event["success"])

    metadata = {
        "name": spec.name,
        "seed": spec.seed,
        "event_count": len(events),
        "batch_count": len(batches),
        "base_timestamp_ms": BASE_TIMESTAMP_MS,
        "duration_ms": EVENT_DURATION_MS,
        "outage": {"start_ms": OUTAGE_START_MS, "end_ms": OUTAGE_END_MS},
        "routes": sorted(route_counts),
        "route_event_counts": route_counts,
        "route_success_counts": success_counts,
    }
    return batches, metadata


def _jsonl_bytes(batches: list[dict[str, Any]]) -> bytes:
    lines = [
        json.dumps(batch, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        for batch in batches
    ]
    return ("\n".join(lines) + "\n").encode("utf-8")


def generate_all(out_dir: str | Path) -> dict[str, Any]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    manifest: dict[str, Any] = {
        "generator_version": GENERATOR_VERSION,
        "rng": "numpy.random.Generator(PCG64)",
        "tiers": {},
    }
    for name, spec in TIERS.items():
        batches, metadata = generate_tier(spec)
        payload = _jsonl_bytes(batches)
        target = out / f"{name}.jsonl"
        target.write_bytes(payload)
        metadata["sha256"] = hashlib.sha256(payload).hexdigest()
        metadata["path"] = target.name
        manifest["tiers"][name] = metadata
    manifest_path = out / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest


def load_batches(path: str | Path) -> list[dict[str, Any]]:
    batches: list[dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                batches.append(json.loads(line))
    return batches


def load_manifest(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))

