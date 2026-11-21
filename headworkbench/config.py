from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .canonical import digest_file, digest_object, read_json, write_json_file
from .errors import WorkbenchError


@dataclass(frozen=True)
class WorkbenchConfig:
    version: str
    seed: int
    candidate_bitwidths: tuple[int, ...]
    sampling_windows: dict[str, int]
    sample_cap_per_head: int
    min_samples_per_head: int
    sensitivity_threshold: float
    global_error_budget: float
    per_head_error_tolerance: float
    activation_clip_abs: float
    latency_budget_ms: float
    config_path: Path
    config_digest: str

    def window_limit(self, window_name: str) -> int:
        try:
            raw = self.sampling_windows[window_name]
        except KeyError as exc:
            raise WorkbenchError(
                "E_CONFIG",
                f"unknown sampling window {window_name!r}",
                details={"available_windows": sorted(self.sampling_windows)},
            ) from exc
        return min(int(raw), self.sample_cap_per_head)

    def window_descriptor(self, window_name: str) -> dict[str, Any]:
        limit = self.window_limit(window_name)
        descriptor = {
            "name": window_name,
            "requested_samples": int(self.sampling_windows[window_name]),
            "sample_cap_per_head": self.sample_cap_per_head,
            "effective_sample_limit": limit,
        }
        descriptor["window_digest"] = digest_object(descriptor)
        return descriptor


def _require(mapping: dict[str, Any], key: str, kind: type, section: str) -> Any:
    if key not in mapping:
        raise WorkbenchError("E_CONFIG", f"missing {section}.{key}")
    value = mapping[key]
    if kind is float and isinstance(value, int):
        return float(value)
    if not isinstance(value, kind):
        raise WorkbenchError(
            "E_CONFIG",
            f"{section}.{key} must be {kind.__name__}",
            details={"value": value},
        )
    return value


def load_config(path: Path) -> WorkbenchConfig:
    path = path.resolve()
    try:
        loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise WorkbenchError("E_CONFIG", f"config not found: {path}") from exc
    except yaml.YAMLError as exc:
        raise WorkbenchError("E_CONFIG", f"invalid YAML: {exc}") from exc
    if not isinstance(loaded, dict):
        raise WorkbenchError("E_CONFIG", "config root must be a mapping")
    try:
        candidate_bitwidths = tuple(int(x) for x in loaded["candidate_bitwidths"])
        windows = {str(k): int(v) for k, v in loaded["sampling_windows"].items()}
        config = WorkbenchConfig(
            version=str(loaded["version"]),
            seed=int(loaded["seed"]),
            candidate_bitwidths=candidate_bitwidths,
            sampling_windows=windows,
            sample_cap_per_head=int(loaded["sample_cap_per_head"]),
            min_samples_per_head=int(loaded["min_samples_per_head"]),
            sensitivity_threshold=float(loaded["sensitivity_threshold"]),
            global_error_budget=float(loaded["global_error_budget"]),
            per_head_error_tolerance=float(loaded["per_head_error_tolerance"]),
            activation_clip_abs=float(loaded.get("activation_clip_abs", 4.0)),
            latency_budget_ms=float(loaded.get("latency_budget_ms", 250)),
            config_path=path,
            config_digest=digest_file(path),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise WorkbenchError("E_CONFIG", f"invalid config: {exc}") from exc

    if not candidate_bitwidths or len(set(candidate_bitwidths)) != len(candidate_bitwidths):
        raise WorkbenchError("E_CONFIG", "candidate_bitwidths must be non-empty and unique")
    if any(bits < 2 or bits > 32 for bits in candidate_bitwidths):
        raise WorkbenchError("E_CONFIG", "candidate bit widths must be in [2, 32]")
    if not windows:
        raise WorkbenchError("E_CONFIG", "sampling_windows must not be empty")
    if config.sample_cap_per_head <= 0 or config.min_samples_per_head < 0:
        raise WorkbenchError("E_CONFIG", "sample limits must be non-negative/positive")
    if not 0.0 <= config.sensitivity_threshold <= 1.0:
        raise WorkbenchError("E_CONFIG", "sensitivity_threshold must be in [0, 1]")
    if config.global_error_budget <= 0 or config.per_head_error_tolerance <= 0:
        raise WorkbenchError("E_CONFIG", "error budgets must be positive")
    if config.activation_clip_abs <= 0:
        raise WorkbenchError("E_CONFIG", "activation_clip_abs must be positive")
    return config


def current_config_path(root: Path) -> Path:
    pointer = root / "config" / "current.json"
    if not pointer.exists():
        return root / "config" / "adjudication.yaml"
    payload = read_json(pointer)
    relative = payload.get("config_path")
    if not isinstance(relative, str):
        raise WorkbenchError("E_CONFIG", "config/current.json has no config_path")
    path = (root / relative).resolve()
    _ensure_within(root, path)
    return path


def install_config(root: Path, source: Path) -> dict[str, Any]:
    source = source.resolve()
    if not source.is_file():
        raise WorkbenchError("E_CONFIG", f"config not found: {source}")
    _ensure_within(root, source)
    loaded = load_config(source)
    digest = digest_file(source)
    target_dir = root / "config" / "imports"
    target = target_dir / f"adjudication-{digest[:16]}.yaml"
    target_dir.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        target.write_bytes(source.read_bytes())
    pointer = root / "config" / "current.json"
    write_json_file(
        pointer,
        {
            "schema": "headworkbench.config-pointer/v1",
            "config_path": str(target.relative_to(root)),
            "config_digest": digest,
            "config_version": loaded.version,
        },
    )
    return {
        "config_path": str(target.relative_to(root)),
        "config_digest": digest,
        "config_version": loaded.version,
        "sampling_windows": loaded.sampling_windows,
    }


def _ensure_within(root: Path, path: Path) -> None:
    try:
        path.relative_to(root.resolve())
    except ValueError as exc:
        raise WorkbenchError(
            "E_PATH",
            "all inputs must reside inside the workspace",
            details={"path": str(path), "workspace": str(root)},
        ) from exc

