from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np

from .canonical import canonical_json_bytes, sha256_bytes
from .config import Config
from .errors import E_NUMERIC, business_error
from .numerics import BatchDensity, curve_digest, state_payload_digest


@dataclass
class DensityState:
    version: int
    batch_count: int
    last_batch_id: int
    total_samples: int
    density_units: np.ndarray
    grid_weights: np.ndarray
    scale: int
    chain_digest: str
    source_batches: tuple[int, ...]

    @classmethod
    def empty(cls, config: Config) -> DensityState:
        grid_size = config.grid.nx * config.grid.ny
        return cls(
            version=0,
            batch_count=0,
            last_batch_id=-1,
            total_samples=0,
            density_units=np.zeros(grid_size, dtype=np.int64),
            grid_weights=np.zeros(grid_size, dtype=np.int64),
            scale=config.density.density_scale,
            chain_digest=sha256_bytes(b"density-trace-empty-state-v1"),
            source_batches=(),
        )

    def curve(self) -> np.ndarray:
        if self.total_samples == 0:
            return np.zeros_like(self.density_units, dtype=np.float64)
        denominator = float(self.scale) * float(self.total_samples)
        return self.density_units.astype(np.float64) / denominator

    def curve_sha256(self) -> str:
        return curve_digest(self.density_units, self.total_samples, self.scale)

    def payload_sha256(self) -> str:
        return state_payload_digest(self.density_units, self.grid_weights)

    def version_fingerprint(self, config_fingerprint: str) -> str:
        payload = {
            "state_version": self.version,
            "batch_count": self.batch_count,
            "last_batch_id": self.last_batch_id,
            "total_samples": self.total_samples,
            "scale": self.scale,
            "payload_sha256": self.payload_sha256(),
            "chain_digest": self.chain_digest,
            "config_fingerprint": config_fingerprint,
        }
        return sha256_bytes(canonical_json_bytes(payload))

    def summary(self, config_fingerprint: str) -> dict[str, Any]:
        return {
            "state_version": self.version,
            "version_fingerprint": self.version_fingerprint(config_fingerprint),
            "covered_batches": self.batch_count,
            "last_batch_id": self.last_batch_id,
            "effective_samples": self.total_samples,
            "scale": self.scale,
            "curve_sha256": self.curve_sha256(),
            "payload_sha256": self.payload_sha256(),
            "chain_digest": self.chain_digest,
            "curve_max": float(np.max(self.curve()))
            if self.density_units.size
            else 0.0,
        }

    def to_record(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "batch_count": self.batch_count,
            "last_batch_id": self.last_batch_id,
            "total_samples": self.total_samples,
            "density_units": self.density_units.astype(int).tolist(),
            "grid_weights": self.grid_weights.astype(int).tolist(),
            "scale": self.scale,
            "chain_digest": self.chain_digest,
            "source_batches": list(self.source_batches),
        }

    @classmethod
    def from_record(cls, value: dict[str, Any]) -> DensityState:
        try:
            density_units = np.asarray(value["density_units"], dtype=np.int64)
            grid_weights = np.asarray(value["grid_weights"], dtype=np.int64)
            state = cls(
                version=int(value["version"]),
                batch_count=int(value["batch_count"]),
                last_batch_id=int(value["last_batch_id"]),
                total_samples=int(value["total_samples"]),
                density_units=density_units,
                grid_weights=grid_weights,
                scale=int(value["scale"]),
                chain_digest=str(value["chain_digest"]),
                source_batches=tuple(int(item) for item in value["source_batches"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise business_error(
                "E_CHECKPOINT_CORRUPT", f"invalid serialized density state: {exc}"
            ) from exc
        if density_units.ndim != 1 or grid_weights.shape != density_units.shape:
            raise business_error(
                "E_CHECKPOINT_CORRUPT",
                "serialized state arrays have inconsistent shapes",
            )
        if state.scale <= 0 or state.total_samples < 0:
            raise business_error(
                E_NUMERIC, "serialized state has invalid scale or sample count"
            )
        return state


def merge_batch_state(
    parent: DensityState,
    batch_density: BatchDensity,
    config: Config,
    batch_id: int,
    config_fingerprint: str,
) -> DensityState:
    if parent.density_units.size != batch_density.density_units.size:
        raise business_error(
            E_NUMERIC, "state and batch density grid sizes differ", batch_id=batch_id
        )
    if parent.scale != config.density.density_scale:
        raise business_error(
            E_NUMERIC, "state and config density scales differ", batch_id=batch_id
        )
    if batch_id != parent.last_batch_id + 1:
        raise business_error(
            "E_INVALID_STAGE",
            "batch cannot be merged out of order",
            batch_id=batch_id,
            expected_batch_id=parent.last_batch_id + 1,
        )
    count = batch_density.effective_samples
    int64_max = np.iinfo(np.int64).max
    max_batch_unit = int(np.max(np.abs(batch_density.density_units)))
    max_parent_unit = int(np.max(np.abs(parent.density_units)))
    if max_batch_unit * int(count) > int64_max - max_parent_unit:
        raise business_error(
            E_NUMERIC,
            "integer density contribution overflowed",
            batch_id=batch_id,
            grid_interval=[0, batch_density.density_units.size - 1],
            anomaly="int64 overflow",
        )
    product = batch_density.density_units.astype(np.int64) * np.int64(count)
    new_density = parent.density_units + product
    new_weights = parent.grid_weights + np.int64(count)
    if np.any(new_density < parent.density_units) or np.any(
        new_weights < parent.grid_weights
    ):
        raise business_error(
            E_NUMERIC,
            "integer density state overflowed",
            batch_id=batch_id,
            grid_interval=[0, product.size - 1],
            anomaly="int64 overflow",
        )
    chain_payload = {
        "parent": parent.chain_digest,
        "batch_id": batch_id,
        "batch_curve_sha256": batch_density.local_curve_sha256,
        "effective_samples": count,
        "config_fingerprint": config_fingerprint,
    }
    return DensityState(
        version=parent.version + 1,
        batch_count=parent.batch_count + 1,
        last_batch_id=batch_id,
        total_samples=parent.total_samples + count,
        density_units=new_density,
        grid_weights=new_weights,
        scale=parent.scale,
        chain_digest=sha256_bytes(canonical_json_bytes(chain_payload)),
        source_batches=parent.source_batches + (batch_id,),
    )


def merge_many_batch_states(
    batch_densities: list[BatchDensity],
    config: Config,
    config_fingerprint: str,
) -> DensityState:
    state = DensityState.empty(config)
    for batch_density in batch_densities:
        state = merge_batch_state(
            state,
            batch_density,
            config,
            batch_density.batch_id,
            config_fingerprint,
        )
    return state
