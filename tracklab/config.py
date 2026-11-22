"""Configuration loading and workspace path policy."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .canonical import digest
from .errors import E_CONFIG, E_IO, raise_error


DEFAULT_CONFIG = Path("config/workbench.yaml")


@dataclass(frozen=True)
class Settings:
    root: Path
    config_path: Path
    raw: dict[str, Any]

    @property
    def workspace(self) -> dict[str, Any]:
        return self.raw["workspace"]

    @property
    def appearance(self) -> dict[str, Any]:
        return self.raw["appearance"]

    @property
    def tracking(self) -> dict[str, Any]:
        return self.raw["tracking"]

    @property
    def generator(self) -> dict[str, Any]:
        return self.raw["generator"]

    @property
    def server(self) -> dict[str, Any]:
        return self.raw["server"]

    def path(self, key: str) -> Path:
        value = self.workspace.get(key)
        if not isinstance(value, str) or not value:
            raise_error(E_CONFIG, f"workspace.{key} must be a non-empty relative path")
        candidate = Path(value)
        if candidate.is_absolute():
            raise_error(E_CONFIG, f"workspace.{key} must be relative to the workspace root")
        if ".." in candidate.parts:
            raise_error(E_CONFIG, f"workspace.{key} must not escape the workspace root")
        return (self.root / candidate).resolve()

    @property
    def data_dir(self) -> Path:
        return self.path("data_dir")

    @property
    def manifest_path(self) -> Path:
        return self.path("manifest_path")

    @property
    def reference_summary_path(self) -> Path:
        return self.path("reference_summary_path")

    @property
    def checkpoints_dir(self) -> Path:
        return self.path("checkpoints_dir")

    @property
    def snapshots_dir(self) -> Path:
        return self.path("snapshots_dir")

    @property
    def artifacts_dir(self) -> Path:
        return self.path("artifacts_dir")

    @property
    def invalidated_dir(self) -> Path:
        return self.path("invalidated_dir")

    @property
    def reports_dir(self) -> Path:
        return self.path("reports_dir")

    @property
    def runtime_dir(self) -> Path:
        return self.path("runtime_dir")

    @property
    def locks_dir(self) -> Path:
        return self.path("locks_dir")

    @property
    def stop_dir(self) -> Path:
        return self.path("stop_dir")

    @property
    def diagnostics_dir(self) -> Path:
        return self.path("diagnostics_dir")

    @property
    def config_fingerprint(self) -> str:
        return digest(
            {
                "schema_version": self.raw.get("schema_version"),
                "appearance": self.appearance,
                "tracking": self.tracking,
            }
        )

    def ensure_directories(self) -> None:
        for path in (
            self.data_dir,
            self.checkpoints_dir,
            self.snapshots_dir,
            self.artifacts_dir,
            self.invalidated_dir,
            self.reports_dir,
            self.runtime_dir,
            self.locks_dir,
            self.stop_dir,
            self.diagnostics_dir,
        ):
            try:
                path.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                raise_error(E_IO, "cannot create workspace directory", context={"path": str(path), "detail": str(exc)})

    def stop_request_path(self, sequence_id: str) -> Path:
        return self.stop_dir / f"{sequence_id}.request.json"

    def sequence_checkpoint_dir(self, sequence_id: str) -> Path:
        return self.checkpoints_dir / sequence_id

    def sequence_artifact_dir(self, sequence_id: str) -> Path:
        return self.artifacts_dir / sequence_id

    def sequence_snapshot_dir(self, sequence_id: str) -> Path:
        return self.snapshots_dir / sequence_id


def _merge(defaults: dict[str, Any], loaded: dict[str, Any]) -> dict[str, Any]:
    result = dict(defaults)
    for key, value in loaded.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _merge(result[key], value)
        else:
            result[key] = value
    return result


DEFAULTS: dict[str, Any] = {
    "schema_version": 1,
    "workspace": {
        "data_dir": "data",
        "manifest_path": "data/tracking_manifest.json",
        "reference_summary_path": "data/reference_summary.json",
        "checkpoints_dir": "runs/checkpoints",
        "snapshots_dir": "runs/snapshots",
        "artifacts_dir": "runs/artifacts",
        "invalidated_dir": "runs/invalidated",
        "reports_dir": "reports",
        "runtime_dir": "runtime",
        "locks_dir": "runtime/locks",
        "stop_dir": "runtime/stop",
        "diagnostics_dir": "runtime/diagnostics",
    },
    "server": {"host": "127.0.0.1", "port": 8765},
    "generator": {
        "seed": 20261123,
        "image_height": 72,
        "image_width": 96,
        "frame_rate": 24,
        "frames_per_sequence": 40,
        "sample_count_upper_bound": 128,
        "output_manifest": "data/tracking_manifest.json",
        "output_summary": "data/reference_summary.json",
    },
    "appearance": {
        "gray_histogram_bins": 16,
        "gradient_histogram_bins": 8,
        "normalization": "l1-v1",
        "gradient_epsilon": 1.0e-12,
        "spatial_grid_size": 8,
        "spatial_epsilon": 1.0e-9,
    },
    "tracking": {
        "grid_step_ratio": 0.05,
        "size_deltas_pixels": [-1, 0, 1],
        "top_k": 6,
        "gray_similarity_weight": 0.40,
        "gradient_similarity_weight": 0.25,
        "spatial_similarity_weight": 0.35,
        "center_prior_weight": 0.30,
        "scale_prior_weight": 0.0,
        "center_prior_sigma_ratio": 0.75,
        "scale_prior_sigma": 0.08,
        "numeric_epsilon": 1.0e-9,
        "velocity_smoothing": 0.50,
        "max_velocity_ratio": 0.08,
        "max_size_velocity_ratio": 0.05,
        "template_update_alpha": 0.15,
        "occlusion_patience": 2,
        "recovery_margin": 0.03,
        "recovery_search_expansion_multiplier": 1.0,
    },
}


def _validate(raw: dict[str, Any]) -> None:
    if raw.get("schema_version") != 1:
        raise_error(E_CONFIG, "unsupported configuration schema_version", context={"schema_version": raw.get("schema_version")})
    appearance = raw["appearance"]
    tracking = raw["tracking"]
    if int(appearance["gray_histogram_bins"]) < 2:
        raise_error(E_CONFIG, "gray_histogram_bins must be >= 2")
    if int(appearance["gradient_histogram_bins"]) < 2:
        raise_error(E_CONFIG, "gradient_histogram_bins must be >= 2")
    if appearance["normalization"] != "l1-v1":
        raise_error(E_CONFIG, "only the l1-v1 appearance normalization is supported")
    if float(tracking["grid_step_ratio"]) <= 0:
        raise_error(E_CONFIG, "grid_step_ratio must be positive")
    if float(tracking["grid_step_ratio"]) > 1:
        raise_error(E_CONFIG, "grid_step_ratio must be <= 1")
    deltas = tracking["size_deltas_pixels"]
    if not isinstance(deltas, list) or not deltas:
        raise_error(E_CONFIG, "size_deltas_pixels must be a non-empty list")
    if any(not isinstance(v, int) for v in deltas):
        raise_error(E_CONFIG, "size_deltas_pixels must contain integers")
    if float(tracking["center_prior_sigma_ratio"]) <= 0:
        raise_error(E_CONFIG, "center_prior_sigma_ratio must be positive")
    if int(tracking["occlusion_patience"]) < 1:
        raise_error(E_CONFIG, "occlusion_patience must be >= 1")
    alpha = float(tracking["template_update_alpha"])
    if not (0.0 < alpha <= 1.0):
        raise_error(E_CONFIG, "template_update_alpha must be in (0, 1]")
    if int(tracking["top_k"]) < 1:
        raise_error(E_CONFIG, "top_k must be >= 1")
    port = int(raw["server"].get("port", 0))
    if not (0 <= port <= 65535):
        raise_error(E_CONFIG, "server.port is invalid")


def load_settings(root: str | Path | None = None, config_path: str | Path | None = None) -> Settings:
    root_path = Path(root or ".").resolve()
    if config_path is None:
        candidate = root_path / DEFAULT_CONFIG
    else:
        candidate = Path(config_path)
        if not candidate.is_absolute():
            candidate = (root_path / candidate).resolve()
    if not candidate.exists():
        raise_error(E_CONFIG, "configuration file does not exist", context={"path": str(candidate)})
    try:
        loaded = yaml.safe_load(candidate.read_text(encoding="utf-8")) or {}
    except (OSError, yaml.YAMLError) as exc:
        raise_error(E_CONFIG, "cannot load YAML configuration", context={"path": str(candidate), "detail": str(exc)})
    if not isinstance(loaded, dict):
        raise_error(E_CONFIG, "configuration root must be a mapping", context={"path": str(candidate)})
    raw = _merge(DEFAULTS, loaded)
    _validate(raw)
    settings = Settings(root=root_path, config_path=candidate.resolve(), raw=raw)
    settings.ensure_directories()
    return settings
