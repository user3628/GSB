from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np

from .canonical import canonical_json_bytes, sha256_bytes
from .errors import E_CONFIG_INVALID, business_error


@dataclass(frozen=True)
class GridConfig:
    x_min: float
    x_max: float
    y_min: float
    y_max: float
    nx: int
    ny: int

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> GridConfig:
        return cls(
            x_min=float(value["x_min"]),
            x_max=float(value["x_max"]),
            y_min=float(value["y_min"]),
            y_max=float(value["y_max"]),
            nx=int(value["nx"]),
            ny=int(value["ny"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "x_min": self.x_min,
            "x_max": self.x_max,
            "y_min": self.y_min,
            "y_max": self.y_max,
            "nx": self.nx,
            "ny": self.ny,
        }

    def validate(self) -> None:
        values = [self.x_min, self.x_max, self.y_min, self.y_max]
        if not all(np.isfinite(value) for value in values):
            raise business_error(E_CONFIG_INVALID, "grid bounds must be finite")
        if self.x_min >= self.x_max or self.y_min >= self.y_max:
            raise business_error(
                E_CONFIG_INVALID, "grid bounds must be strictly increasing"
            )
        if self.nx < 2 or self.ny < 2:
            raise business_error(E_CONFIG_INVALID, "grid dimensions must be at least 2")

    def points(self) -> np.ndarray:
        xs = np.linspace(self.x_min, self.x_max, self.nx, dtype=np.float64)
        ys = np.linspace(self.y_min, self.y_max, self.ny, dtype=np.float64)
        gx, gy = np.meshgrid(xs, ys, indexing="xy")
        return np.column_stack((gx.ravel(order="C"), gy.ravel(order="C")))


@dataclass(frozen=True)
class BandwidthConfig:
    method: str = "mad_iqr_fallback"
    mad_factor: float = 1.4826
    iqr_factor: float = 1.349
    floor: float = 0.12

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> BandwidthConfig:
        if not value:
            return cls()
        return cls(
            method=str(value.get("method", "mad_iqr_fallback")),
            mad_factor=float(value.get("mad_factor", 1.4826)),
            iqr_factor=float(value.get("iqr_factor", 1.349)),
            floor=float(value.get("floor", 0.12)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "method": self.method,
            "mad_factor": self.mad_factor,
            "iqr_factor": self.iqr_factor,
            "floor": self.floor,
        }

    def validate(self) -> None:
        if self.method != "mad_iqr_fallback":
            raise business_error(
                E_CONFIG_INVALID, f"unsupported bandwidth method: {self.method}"
            )
        if not np.isfinite(self.mad_factor) or self.mad_factor <= 0:
            raise business_error(
                E_CONFIG_INVALID, "mad_factor must be positive and finite"
            )
        if not np.isfinite(self.iqr_factor) or self.iqr_factor <= 0:
            raise business_error(
                E_CONFIG_INVALID, "iqr_factor must be positive and finite"
            )
        if not np.isfinite(self.floor) or self.floor <= 0:
            raise business_error(
                E_CONFIG_INVALID, "bandwidth floor must be positive and finite"
            )


@dataclass(frozen=True)
class DensityConfig:
    chunk_size: int = 32
    sum_strategy: str = "neumaier"
    density_scale: int = 1_000_000_000_000
    bandwidth: BandwidthConfig = field(default_factory=BandwidthConfig)
    curve_abs_tolerance: float = 1e-9

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> DensityConfig:
        if not value:
            return cls()
        return cls(
            chunk_size=int(value.get("chunk_size", 32)),
            sum_strategy=str(value.get("sum_strategy", "neumaier")),
            density_scale=int(value.get("density_scale", 1_000_000_000_000)),
            bandwidth=BandwidthConfig.from_dict(value.get("bandwidth")),
            curve_abs_tolerance=float(value.get("curve_abs_tolerance", 1e-9)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "chunk_size": self.chunk_size,
            "sum_strategy": self.sum_strategy,
            "density_scale": self.density_scale,
            "bandwidth": self.bandwidth.to_dict(),
            "curve_abs_tolerance": self.curve_abs_tolerance,
        }

    def validate(self) -> None:
        if self.chunk_size <= 0:
            raise business_error(E_CONFIG_INVALID, "chunk_size must be positive")
        if self.sum_strategy not in {"kahan", "neumaier"}:
            raise business_error(
                E_CONFIG_INVALID, f"unsupported sum strategy: {self.sum_strategy}"
            )
        if not np.isfinite(self.curve_abs_tolerance) or self.curve_abs_tolerance < 0:
            raise business_error(
                E_CONFIG_INVALID, "curve_abs_tolerance must be finite and nonnegative"
            )
        self.bandwidth.validate()


@dataclass(frozen=True)
class AdjudicationConfig:
    warning_threshold: float = 0.02
    drift_threshold: float = 0.06
    warning_confidence: float = 0.90
    drift_confidence: float = 0.99
    quantile_index_start: int = 1
    quantile_index_end: int = 17

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> AdjudicationConfig:
        if not value:
            return cls()
        return cls(
            warning_threshold=float(value.get("warning_threshold", 0.02)),
            drift_threshold=float(value.get("drift_threshold", 0.06)),
            warning_confidence=float(value.get("warning_confidence", 0.90)),
            drift_confidence=float(value.get("drift_confidence", 0.99)),
            quantile_index_start=int(value.get("quantile_index_start", 1)),
            quantile_index_end=int(value.get("quantile_index_end", 17)),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "warning_threshold": self.warning_threshold,
            "drift_threshold": self.drift_threshold,
            "warning_confidence": self.warning_confidence,
            "drift_confidence": self.drift_confidence,
            "quantile_index_start": self.quantile_index_start,
            "quantile_index_end": self.quantile_index_end,
        }

    def validate(self) -> None:
        values = [
            self.warning_threshold,
            self.drift_threshold,
            self.warning_confidence,
            self.drift_confidence,
        ]
        if not all(np.isfinite(value) for value in values):
            raise business_error(
                E_CONFIG_INVALID, "adjudication settings must be finite"
            )
        if self.warning_threshold < 0 or self.drift_threshold <= self.warning_threshold:
            raise business_error(
                E_CONFIG_INVALID, "drift_threshold must exceed warning_threshold"
            )
        if not (0 < self.warning_confidence < self.drift_confidence <= 1):
            raise business_error(
                E_CONFIG_INVALID, "confidence levels must be ordered within (0, 1]"
            )
        if (
            self.quantile_index_start < 0
            or self.quantile_index_end < self.quantile_index_start
        ):
            raise business_error(E_CONFIG_INVALID, "invalid quantile index range")


@dataclass(frozen=True)
class Config:
    config_version: str
    grid: GridConfig
    density: DensityConfig
    adjudication: AdjudicationConfig
    schema_version: int = 1

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> Config:
        try:
            config = cls(
                config_version=str(value["config_version"]),
                grid=GridConfig.from_dict(value["grid"]),
                density=DensityConfig.from_dict(value.get("density")),
                adjudication=AdjudicationConfig.from_dict(value.get("adjudication")),
                schema_version=int(value.get("schema_version", 1)),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise business_error(E_CONFIG_INVALID, f"invalid config: {exc}") from exc
        config.validate()
        return config

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "config_version": self.config_version,
            "grid": self.grid.to_dict(),
            "density": self.density.to_dict(),
            "adjudication": self.adjudication.to_dict(),
        }

    def validate(self) -> None:
        if not self.config_version:
            raise business_error(E_CONFIG_INVALID, "config_version is required")
        self.grid.validate()
        self.density.validate()
        self.adjudication.validate()

    def density_stage_fingerprint(self) -> str:
        payload = {
            "stage": "density",
            "grid": self.grid.to_dict(),
            "density": self.density.to_dict(),
        }
        return sha256_bytes(canonical_json_bytes(payload))

    def adjudication_stage_fingerprint(self, reference_digest: str) -> str:
        payload = {
            "stage": "adjudication",
            "adjudication": self.adjudication.to_dict(),
            "reference_digest": reference_digest,
        }
        return sha256_bytes(canonical_json_bytes(payload))

    def config_fingerprint(self, reference_digest: str) -> str:
        payload = {
            "config": self.to_dict(),
            "reference_digest": reference_digest,
            "density_stage": self.density_stage_fingerprint(),
            "adjudication_stage": self.adjudication_stage_fingerprint(reference_digest),
        }
        return sha256_bytes(canonical_json_bytes(payload))
