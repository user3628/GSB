"""Operator manifest (YAML) loading, validation and resource estimation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import yaml

from .errors import InvalidBitwidthError
from .fingerprints import canonical_json_bytes, sha256_hex
from .ops import LayerSpec
from .quantization import SUPPORTED_BITWIDTHS

BUDGET_REASON_COMPUTE = "total_compute_budget_exceeded"
BUDGET_REASON_MEMORY = "peak_memory_budget_exceeded"
BUDGET_REASON_QUOTA = "worker_element_quota_exceeded"


@dataclass(frozen=True)
class Budget:
    total_element_budget: int
    worker_element_quota: int
    peak_memory_bytes: int

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "Budget":
        return cls(
            total_element_budget=int(payload["total_element_budget"]),
            worker_element_quota=int(payload["worker_element_quota"]),
            peak_memory_bytes=int(payload["peak_memory_bytes"]),
        )

    def to_dict(self) -> Dict[str, int]:
        return {
            "total_element_budget": self.total_element_budget,
            "worker_element_quota": self.worker_element_quota,
            "peak_memory_bytes": self.peak_memory_bytes,
        }


@dataclass(frozen=True)
class ShardSpec:
    shard_id: str
    index: int
    npz_file: str
    batch: int
    input_features: int
    layers: List[LayerSpec]
    expected_output_shape: List[int]
    expected_rel_error_bound: float

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "ShardSpec":
        layers = [LayerSpec.from_dict(item) for item in payload["layers"]]
        return cls(
            shard_id=str(payload["shard_id"]),
            index=int(payload["index"]),
            npz_file=str(payload["npz_file"]),
            batch=int(payload["batch"]),
            input_features=int(payload["input_features"]),
            layers=layers,
            expected_output_shape=[int(v) for v in
                                   payload["expected_output_shape"]],
            expected_rel_error_bound=float(
                payload.get("expected_rel_error_bound", 0.35)),
        )


@dataclass(frozen=True)
class Manifest:
    config_version: int
    budget: Budget
    default_tolerance: Dict[str, float]
    shards: List[ShardSpec]
    source_path: str = ""

    def shard_by_id(self, shard_id: str) -> ShardSpec:
        for shard in self.shards:
            if shard.shard_id == shard_id:
                return shard
        raise KeyError(shard_id)


@dataclass(frozen=True)
class ShardEstimate:
    shard_id: str
    index: int
    compute_elements: int
    working_set_bytes: int
    input_elements: int
    weight_elements: int
    max_bitwidth: int


def _validate_bitwidths(shards: List[ShardSpec]) -> None:
    for shard in shards:
        for layer in shard.layers:
            for width in (layer.bitwidth, layer.activation_bitwidth):
                if width not in SUPPORTED_BITWIDTHS:
                    raise InvalidBitwidthError(
                        f"layer {layer.name} of shard {shard.shard_id} "
                        f"declares illegal bitwidth {width}",
                        details={"shard_id": shard.shard_id,
                                 "layer": layer.name, "bitwidth": width,
                                 "supported": list(SUPPORTED_BITWIDTHS)},
                    )


def load_manifest(path: str | Path) -> Manifest:
    path = Path(path)
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    manifest = manifest_from_dict(payload, source_path=str(path))
    _validate_bitwidths(manifest.shards)
    return manifest


def manifest_from_dict(payload: Dict[str, Any],
                       source_path: str = "") -> Manifest:
    shards = [ShardSpec.from_dict(item) for item in payload["shards"]]
    shards.sort(key=lambda item: item.index)
    manifest = Manifest(
        config_version=int(payload.get("config_version", 1)),
        budget=Budget.from_dict(payload["budget"]),
        default_tolerance={
            "rel_tol": float(payload.get("default_tolerance", {})
                             .get("rel_tol", 0.35)),
            "abs_tol": float(payload.get("default_tolerance", {})
                             .get("abs_tol", 0.5)),
        },
        shards=shards,
        source_path=source_path,
    )
    _validate_bitwidths(manifest.shards)
    return manifest


def manifest_config_fingerprint(manifest: Manifest) -> str:
    """Fingerprint of every version-relevant manifest field."""

    payload = {
        "config_version": manifest.config_version,
        "budget": manifest.budget.to_dict(),
        "default_tolerance": manifest.default_tolerance,
        "shards": [
            {
                "shard_id": shard.shard_id,
                "index": shard.index,
                "npz_file": shard.npz_file,
                "batch": shard.batch,
                "input_features": shard.input_features,
                "expected_output_shape": shard.expected_output_shape,
                "expected_rel_error_bound": shard.expected_rel_error_bound,
                "layers": [layer.to_dict() for layer in shard.layers],
            }
            for shard in manifest.shards
        ],
    }
    return sha256_hex(canonical_json_bytes(payload))


def estimate_shard(shard: ShardSpec) -> ShardEstimate:
    """Estimate element cost and working set from declared shapes."""

    compute_elements = 0
    weight_elements = 0
    max_bitwidth = 0
    rows = shard.batch
    for layer in shard.layers:
        # One multiply-accumulate per (row, output, input) triple.
        compute_elements += rows * layer.out_features * layer.in_features
        weight_elements += layer.in_features * layer.out_features
        max_bitwidth = max(max_bitwidth, layer.bitwidth,
                           layer.activation_bitwidth)
        rows = layer.out_features

    input_elements = shard.batch * shard.input_features
    # Float64 working set: activations, reference copy, weights, integer
    # temporaries and per-layer outputs, with a conservative x3 multiplier for
    # quantize temporaries and output buffers.
    activation_bytes = (input_elements + compute_elements // max(
        shard.batch, 1)) * 8
    weight_bytes = weight_elements * 8
    raw_bytes = (activation_bytes * 2 + weight_bytes) * 3
    working_set_bytes = int(raw_bytes + 4096)
    return ShardEstimate(
        shard_id=shard.shard_id,
        index=shard.index,
        compute_elements=int(compute_elements),
        working_set_bytes=working_set_bytes,
        input_elements=input_elements,
        weight_elements=weight_elements,
        max_bitwidth=max_bitwidth,
    )
