from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .util import WorkbenchError, atomic_write_json, digest, read_json, resolve_workspace_path


DEFAULT_CONFIG: dict[str, Any] = {
    "schema_version": 1,
    "dataset": {
        "manifest": "samples/manifest.json",
        "injection": "configs/injection.json",
    },
    "preprocess": {
        "block_size": 8,
        "normalization": {
            "rule": "symmetric",
            "center": 0.5,
            "scale": 0.5,
        },
    },
    "scoring": {
        "default_threshold": 0.5,
        "exclusive_pairs": [
            ["cat", "dog"],
            ["indoor", "outdoor"],
            ["day", "night"],
        ],
    },
    "metrics": {
        "ap_method": "step_sum_precision_at_positive",
    },
    "report": {
        "include_sample_records": True,
    },
}


STAGE_NAMES = ("preprocess", "score", "metrics", "report")
STAGE_INDEX = {name: index for index, name in enumerate(STAGE_NAMES, start=1)}


@dataclass(frozen=True)
class WorkbenchConfig:
    root: Path
    path: Path
    data: dict[str, Any]
    digest: str

    @property
    def manifest_path(self) -> Path:
        return resolve_workspace_path(self.root, self.data["dataset"]["manifest"])

    @property
    def injection_path(self) -> Path:
        return resolve_workspace_path(self.root, self.data["dataset"]["injection"])

    @property
    def config_version(self) -> str:
        return f"sha256:{self.digest}"

    def stage_data(self, stage: str) -> dict[str, Any]:
        if stage == "preprocess":
            return {"preprocess": self.data["preprocess"]}
        if stage == "score":
            return {"scoring": self.data["scoring"]}
        if stage == "metrics":
            return {"metrics": self.data["metrics"]}
        if stage == "report":
            return {"report": self.data["report"]}
        raise KeyError(stage)

    def stage_digest(self, stage: str) -> str:
        return digest(self.stage_data(stage))


def write_default_config(root: Path, path: Path, *, overwrite: bool = False) -> WorkbenchConfig:
    if overwrite or not path.exists():
        atomic_write_json(path, DEFAULT_CONFIG, pretty=True)
    return load_config(root, path)


def load_config(root: Path, path: Path | str) -> WorkbenchConfig:
    config_path = resolve_workspace_path(root, path, must_exist=True)
    try:
        data = read_json(config_path)
    except Exception as exc:
        raise WorkbenchError(
            "E_CONFIG_INVALID",
            "configuration is not valid JSON",
            {"path": config_path.as_posix(), "error": str(exc)},
        ) from exc
    if not isinstance(data, dict):
        raise WorkbenchError("E_CONFIG_INVALID", "configuration root must be an object")
    merged = copy.deepcopy(DEFAULT_CONFIG)
    _deep_merge(merged, data)
    validate_config(merged)
    return WorkbenchConfig(root=root.resolve(), path=config_path, data=merged, digest=digest(merged))


def _deep_merge(target: dict[str, Any], source: dict[str, Any]) -> None:
    for key, value in source.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _deep_merge(target[key], value)
        else:
            target[key] = value


def validate_config(data: dict[str, Any]) -> None:
    try:
        dataset = data["dataset"]
        preprocess = data["preprocess"]
        scoring = data["scoring"]
        metrics = data["metrics"]
        report = data["report"]
        if not isinstance(dataset.get("manifest"), str):
            raise ValueError("dataset.manifest must be a string")
        if not isinstance(dataset.get("injection"), str):
            raise ValueError("dataset.injection must be a string")
        block_size = int(preprocess["block_size"])
        if block_size <= 0 or block_size > 512:
            raise ValueError("preprocess.block_size must be in [1, 512]")
        norm = preprocess["normalization"]
        if norm.get("rule") != "symmetric":
            raise ValueError("only the fixed symmetric normalization rule is supported")
        if float(norm.get("scale", 0)) == 0.0:
            raise ValueError("preprocess.normalization.scale cannot be zero")
        threshold = float(scoring["default_threshold"])
        if not 0.0 <= threshold <= 1.0:
            raise ValueError("scoring.default_threshold must be in [0, 1]")
        pairs = scoring["exclusive_pairs"]
        if not isinstance(pairs, list):
            raise ValueError("scoring.exclusive_pairs must be a list")
        for pair in pairs:
            if not isinstance(pair, list) or len(pair) != 2 or pair[0] == pair[1]:
                raise ValueError("each exclusive pair must contain two distinct labels")
        if metrics.get("ap_method") != "step_sum_precision_at_positive":
            raise ValueError("unsupported metrics.ap_method")
        if not isinstance(report.get("include_sample_records"), bool):
            raise ValueError("report.include_sample_records must be boolean")
    except (KeyError, TypeError, ValueError) as exc:
        raise WorkbenchError("E_CONFIG_INVALID", "configuration validation failed", {"error": str(exc)}) from exc


__all__ = [
    "DEFAULT_CONFIG",
    "STAGE_INDEX",
    "STAGE_NAMES",
    "WorkbenchConfig",
    "load_config",
    "validate_config",
    "write_default_config",
]
