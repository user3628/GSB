"""Manifest loading and declared spectral parameter checks."""

import json

from .config import TARGET_SAMPLE_RATE
from .errors import ManifestError

REQUIRED_TOP_KEYS = ("manifest_version", "sample_rate", "frame_ms", "hop_ms", "bands", "shards")
REQUIRED_SHARD_KEYS = (
    "shard_id",
    "order",
    "wav_file",
    "declared_samples",
    "spectral",
)


def load_manifest(path):
    with open(path, "r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    return validate_manifest(manifest)


def validate_manifest(manifest):
    for key in REQUIRED_TOP_KEYS:
        if key not in manifest:
            raise ManifestError("manifest missing required key", {"key": key})
    if manifest["sample_rate"] != TARGET_SAMPLE_RATE:
        raise ManifestError(
            "manifest sample rate must be 16000 Hz",
            {"sample_rate": manifest["sample_rate"]},
        )
    shards = manifest["shards"]
    if not shards:
        raise ManifestError("manifest contains no shards")
    seen_ids = set()
    orders = []
    for entry in shards:
        for key in REQUIRED_SHARD_KEYS:
            if key not in entry:
                raise ManifestError(
                    "shard entry missing required key",
                    {"shard_id": entry.get("shard_id"), "key": key},
                )
        if entry["shard_id"] in seen_ids:
            raise ManifestError("duplicate shard id", {"shard_id": entry["shard_id"]})
        seen_ids.add(entry["shard_id"])
        orders.append(entry["order"])
    if sorted(orders) != list(range(1, len(shards) + 1)):
        raise ManifestError("shard order must be a contiguous 1..N sequence", {"orders": orders})
    total = sum(int(entry["declared_samples"]) for entry in shards)
    if total != int(manifest.get("declared_total_samples", total)):
        raise ManifestError(
            "declared total sample count does not match shard declarations",
            {"declared_total": manifest.get("declared_total_samples"), "computed": total},
        )
    return manifest


def ordered_shards(manifest):
    return sorted(manifest["shards"], key=lambda entry: entry["order"])
