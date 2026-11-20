from __future__ import annotations

import copy
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .util import (
    WorkbenchError,
    digest,
    file_sha256,
    read_json,
    relpath,
    resolve_workspace_path,
    stable_round,
    strict_float,
)


REQUIRED_SAMPLE_FIELDS = ("image_id", "path", "width", "height", "truth", "scores")
REQUIRED_ROOT_FIELDS = ("classes", "samples")


@dataclass
class ManifestSample:
    key: str
    image_id: str | None
    raw: dict[str, Any]
    source_index: int
    manifest_path: Path
    image_path: Path | None
    relative_image_path: str | None

    @property
    def width(self) -> Any:
        return self.raw.get("width")

    @property
    def height(self) -> Any:
        return self.raw.get("height")

    @property
    def truth(self) -> Any:
        return self.raw.get("truth")

    @property
    def scores(self) -> Any:
        return self.raw.get("scores")

    def missing_fields(self) -> list[str]:
        return [field for field in REQUIRED_SAMPLE_FIELDS if field not in self.raw]

    def source_summary(self, root: Path) -> dict[str, Any]:
        return {
            "sample_key": self.key,
            "image_id": self.image_id,
            "manifest_path": relpath(root, self.manifest_path),
            "manifest_record_index": self.source_index,
            "image_path": self.relative_image_path,
        }


@dataclass
class Dataset:
    root: Path
    manifest_path: Path
    manifest_sha256: str
    raw: dict[str, Any]
    classes: list[str]
    samples: list[ManifestSample]
    injection_path: Path
    injection_sha256: str
    injections: list[dict[str, Any]]

    @property
    def dataset_id(self) -> str:
        value = self.raw.get("dataset_id")
        return value if isinstance(value, str) else "local-dataset"

    @property
    def seed(self) -> Any:
        return self.raw.get("seed")

    def source_summary(self) -> dict[str, Any]:
        return {
            "dataset_id": self.dataset_id,
            "seed": self.seed,
            "manifest": {
                "path": relpath(self.root, self.manifest_path),
                "sha256": self.manifest_sha256,
            },
            "injection": {
                "path": relpath(self.root, self.injection_path),
                "sha256": self.injection_sha256,
                "operations": copy.deepcopy(self.injections),
            },
        }


def load_dataset(root: Path, config: Any) -> Dataset:
    root = root.resolve()
    manifest_path = config.manifest_path
    if not manifest_path.exists():
        raise WorkbenchError(
            "E_NOT_FOUND",
            "dataset manifest does not exist",
            {"path": relpath(root, manifest_path)},
        )
    try:
        raw = read_json(manifest_path)
    except Exception as exc:
        raise WorkbenchError(
            "E_MANIFEST_INVALID",
            "dataset manifest is not valid JSON",
            {"path": relpath(root, manifest_path), "error": str(exc)},
        ) from exc
    if not isinstance(raw, dict):
        raise WorkbenchError("E_MANIFEST_INVALID", "manifest root must be an object")
    missing_root = [field for field in REQUIRED_ROOT_FIELDS if field not in raw]
    if missing_root:
        raise WorkbenchError(
            "E_MISSING_FIELD",
            "manifest is missing required root fields",
            {"path": relpath(root, manifest_path), "fields": missing_root},
        )
    classes = raw.get("classes")
    if not isinstance(classes, list) or not classes or not all(isinstance(item, str) for item in classes):
        raise WorkbenchError("E_MANIFEST_INVALID", "manifest.classes must be a non-empty string list")
    if len(set(classes)) != len(classes):
        raise WorkbenchError("E_MANIFEST_INVALID", "manifest.classes contains duplicates")
    raw_samples = raw.get("samples")
    if not isinstance(raw_samples, list):
        raise WorkbenchError("E_MANIFEST_INVALID", "manifest.samples must be a list")

    injection_path = config.injection_path
    injections = load_injection(injection_path)
    injection_sha = file_sha256(injection_path) if injection_path.exists() else digest({"operations": []})
    processed = copy.deepcopy(raw_samples)
    _apply_injections(processed, injections)

    samples: list[ManifestSample] = []
    seen_keys: set[str] = set()
    for index, record in enumerate(processed):
        if not isinstance(record, dict):
            raise WorkbenchError(
                "E_MANIFEST_INVALID",
                "each sample record must be an object",
                {"manifest_record_index": index},
            )
        image_id = record.get("image_id") if isinstance(record.get("image_id"), str) else None
        path_value = record.get("path") if isinstance(record.get("path"), str) else None
        if image_id:
            key = image_id
        else:
            key = f"missing-image-id:{index}:{path_value or '<no-path>'}"
        if key in seen_keys:
            raise WorkbenchError(
                "E_MANIFEST_INVALID",
                "duplicate sample key/image_id",
                {"sample_key": key, "manifest_record_index": index},
            )
        seen_keys.add(key)
        image_path: Path | None = None
        relative_image_path: str | None = None
        if path_value:
            candidate = (manifest_path.parent / path_value).resolve()
            try:
                candidate.relative_to(root)
            except ValueError as exc:
                raise WorkbenchError(
                    "E_OUTSIDE_WORKSPACE",
                    "sample image path must stay inside the workspace",
                    {"sample_key": key, "path": path_value},
                ) from exc
            image_path = candidate
            relative_image_path = relpath(root, candidate)
        samples.append(
            ManifestSample(
                key=key,
                image_id=image_id,
                raw=record,
                source_index=index,
                manifest_path=manifest_path,
                image_path=image_path,
                relative_image_path=relative_image_path,
            )
        )
    return Dataset(
        root=root,
        manifest_path=manifest_path,
        manifest_sha256=file_sha256(manifest_path),
        raw=raw,
        classes=list(classes),
        samples=samples,
        injection_path=injection_path,
        injection_sha256=injection_sha,
        injections=injections,
    )


def load_injection(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        data = read_json(path)
    except Exception as exc:
        raise WorkbenchError(
            "E_INJECTION_INVALID",
            "injection file is not valid JSON",
            {"path": path.as_posix(), "error": str(exc)},
        ) from exc
    if isinstance(data, list):
        operations = data
    elif isinstance(data, dict):
        operations = data.get("operations", [])
    else:
        raise WorkbenchError("E_INJECTION_INVALID", "injection root must be an object or list")
    if not isinstance(operations, list) or not all(isinstance(item, dict) for item in operations):
        raise WorkbenchError("E_INJECTION_INVALID", "injection operations must be a list of objects")
    for operation in operations:
        validate_injection_operation(operation)
    return copy.deepcopy(operations)


def validate_injection_operation(operation: dict[str, Any]) -> None:
    kind = operation.get("type")
    target = operation.get("sample")
    allowed = {
        "dim_mismatch": {"type", "sample", "width", "height"},
        "unknown_label": {"type", "sample", "label"},
        "numeric": {"type", "sample", "label", "value"},
        "missing_field": {"type", "sample", "field"},
        "score_length": {"type", "sample", "label"},
    }
    if kind not in allowed:
        raise WorkbenchError("E_INJECTION_INVALID", "unsupported injection type", {"type": kind})
    if not isinstance(target, str) or not target:
        raise WorkbenchError("E_INJECTION_INVALID", "injection sample must be a non-empty image_id")
    missing = [key for key in allowed[kind] - set(operation) if key != "sample"]
    if missing:
        raise WorkbenchError(
            "E_INJECTION_INVALID",
            "injection operation is missing fields",
            {"type": kind, "fields": missing},
        )
    if kind == "dim_mismatch":
        if not isinstance(operation["width"], int) or not isinstance(operation["height"], int):
            raise WorkbenchError("E_INJECTION_INVALID", "injected dimensions must be integers")
    if kind == "missing_field" and operation["field"] not in REQUIRED_SAMPLE_FIELDS:
        raise WorkbenchError(
            "E_INJECTION_INVALID",
            "missing_field injection targets an unsupported field",
            {"field": operation["field"]},
        )
    if kind in {"unknown_label", "numeric", "score_length"} and not isinstance(operation["label"], str):
        raise WorkbenchError("E_INJECTION_INVALID", "injected label must be a string")


def _apply_injections(records: list[Any], operations: list[dict[str, Any]]) -> None:
    by_id = {
        record.get("image_id"): record
        for record in records
        if isinstance(record, dict) and isinstance(record.get("image_id"), str)
    }
    for operation in operations:
        target = operation["sample"]
        record = by_id.get(target)
        if record is None:
            raise WorkbenchError(
                "E_INJECTION_INVALID",
                "injection target image_id was not found",
                {"sample": target, "type": operation["type"]},
            )
        kind = operation["type"]
        if kind == "dim_mismatch":
            record["width"] = operation["width"]
            record["height"] = operation["height"]
        elif kind == "unknown_label":
            truth = record.get("truth")
            if not isinstance(truth, list):
                record["truth"] = [
                    {"label": operation["label"], "lower_bound": float(operation.get("lower_bound", 0.5))}
                ]
            else:
                truth.append(
                    {
                        "label": operation["label"],
                        "lower_bound": float(operation.get("lower_bound", 0.5)),
                    }
                )
        elif kind == "numeric":
            scores = record.get("scores")
            if not isinstance(scores, dict):
                record["scores"] = {operation["label"]: operation["value"]}
            else:
                scores[operation["label"]] = operation["value"]
        elif kind == "missing_field":
            record.pop(operation["field"], None)
        elif kind == "score_length":
            scores = record.get("scores")
            if isinstance(scores, dict):
                scores.pop(operation["label"], None)
    # A missing image_id changes the identity key; later operations still target the
    # original id because operations are applied in order against the exact object map.


def truth_rows(sample: ManifestSample) -> list[dict[str, Any]] | None:
    truth = sample.truth
    if not isinstance(truth, list):
        return None
    rows: list[dict[str, Any]] = []
    for item in truth:
        if not isinstance(item, dict) or not isinstance(item.get("label"), str):
            return None
        try:
            lower = strict_float(item.get("lower_bound"))
        except (TypeError, ValueError):
            return None
        if not math.isfinite(lower):
            return None
        rows.append({"label": item["label"], "lower_bound": stable_round(lower, 12)})
    return rows


def truth_digest(sample: ManifestSample) -> str:
    rows = truth_rows(sample)
    return digest({"truth": rows if rows is not None else sample.raw.get("truth")})


def scores_for_digest(sample: ManifestSample) -> Any:
    scores = sample.scores
    if not isinstance(scores, dict):
        return scores
    normalized: dict[str, Any] = {}
    for label, value in scores.items():
        try:
            number = strict_float(value)
            if math.isfinite(number):
                normalized[str(label)] = stable_round(number, 12)
            elif math.isnan(number):
                normalized[str(label)] = "NaN"
            elif number > 0:
                normalized[str(label)] = "Inf"
            else:
                normalized[str(label)] = "-Inf"
        except (TypeError, ValueError):
            normalized[str(label)] = {"invalid_score": str(value)}
    return normalized


def scores_digest(sample: ManifestSample) -> str:
    return digest({"scores": scores_for_digest(sample)})


__all__ = [
    "Dataset",
    "ManifestSample",
    "REQUIRED_SAMPLE_FIELDS",
    "load_dataset",
    "load_injection",
    "scores_digest",
    "scores_for_digest",
    "truth_digest",
    "truth_rows",
    "validate_injection_operation",
]
