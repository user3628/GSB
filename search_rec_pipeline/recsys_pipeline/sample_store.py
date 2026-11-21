"""Sample declaration parsing, validation, and import manifests."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import SCALES, Config
from .errors import E_INVALID_FEATURE, E_INVALID_STAGE, PipelineError
from .util import locked_file, read_jsonl, sha256_bytes, sha256_file, write_json_atomic


@dataclass
class ScaleData:
    scale: str
    declaration: dict[str, Any]
    declaration_path: Path
    declaration_bytes: bytes
    sample_path: Path
    sample_bytes: bytes
    sample_input_fingerprint: str
    records: list[dict[str, Any]]


class SampleRepository:
    def __init__(self, config: Config) -> None:
        self.config = config

    def load_scale(self, scale: str) -> ScaleData:
        if scale not in SCALES:
            raise PipelineError(
                E_INVALID_STAGE,
                "unknown scale",
                scale=scale,
                valid_scales=list(SCALES),
            )
        declaration_path = self.config.scale_declaration_path(scale)
        try:
            declaration_bytes = declaration_path.read_bytes()
        except FileNotFoundError as exc:
            raise PipelineError(
                E_INVALID_STAGE,
                "scale declaration file is missing",
                scale=scale,
                path=str(declaration_path),
            ) from exc
        try:
            declaration = json.loads(declaration_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PipelineError(
                E_INVALID_STAGE,
                "scale declaration is not valid UTF-8 JSON",
                scale=scale,
                path=str(declaration_path),
                error=str(exc),
            ) from exc
        if not isinstance(declaration, dict):
            raise PipelineError(
                E_INVALID_STAGE,
                "scale declaration must be an object",
                scale=scale,
                path=str(declaration_path),
            )
        self._validate_declaration_shape(scale, declaration, declaration_path)

        sample_relative = declaration.get("sample_file", f"samples/{scale}.jsonl")
        sample_path = (self.config.project_root / str(sample_relative)).resolve()
        try:
            sample_bytes = sample_path.read_bytes()
        except FileNotFoundError as exc:
            raise PipelineError(
                E_INVALID_STAGE,
                "sample file is missing",
                scale=scale,
                path=str(sample_path),
            ) from exc
        try:
            records = read_jsonl(sample_path)
        except (UnicodeDecodeError, ValueError) as exc:
            raise PipelineError(
                E_INVALID_FEATURE,
                "sample JSONL cannot be parsed",
                scale=scale,
                path=str(sample_path),
                error=str(exc),
            ) from exc
        declared_count = int(declaration["sample_count"])
        if len(records) != declared_count:
            raise PipelineError(
                E_INVALID_STAGE,
                "declared sample_count does not match the actual JSONL record count",
                scale=scale,
                declared_sample_count=declared_count,
                actual_sample_count=len(records),
                sample_file=str(sample_path),
            )
        seen: set[str] = set()
        for index, record in enumerate(records):
            sample_id = record.get("sample_id")
            if not isinstance(sample_id, str) or not sample_id.strip():
                raise PipelineError(
                    E_INVALID_FEATURE,
                    "sample_id must be a non-empty string",
                    sample_id=sample_id,
                    field="sample_id",
                    scale=scale,
                    line=index + 1,
                )
            if sample_id in seen:
                raise PipelineError(
                    E_INVALID_FEATURE,
                    "sample_id must be unique within a scale",
                    sample_id=sample_id,
                    field="sample_id",
                    scale=scale,
                    line=index + 1,
                )
            seen.add(sample_id)
        return ScaleData(
            scale=scale,
            declaration=declaration,
            declaration_path=declaration_path,
            declaration_bytes=declaration_bytes,
            sample_path=sample_path,
            sample_bytes=sample_bytes,
            sample_input_fingerprint=sha256_bytes(sample_bytes),
            records=records,
        )

    @staticmethod
    def _validate_declaration_shape(
        scale: str, declaration: dict[str, Any], declaration_path: Path
    ) -> None:
        required = ("sample_count", "feature_cardinality", "cross_limit", "budget_units")
        missing = [key for key in required if key not in declaration]
        if missing:
            raise PipelineError(
                E_INVALID_STAGE,
                "scale declaration is missing required observations",
                scale=scale,
                path=str(declaration_path),
                missing=missing,
            )
        if not isinstance(declaration["feature_cardinality"], dict):
            raise PipelineError(
                E_INVALID_STAGE,
                "feature_cardinality must be an object",
                scale=scale,
                path=str(declaration_path),
            )
        for key in ("sample_count", "cross_limit", "budget_units"):
            value = declaration[key]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise PipelineError(
                    E_INVALID_STAGE,
                    f"declaration.{key} must be a non-negative integer",
                    scale=scale,
                    path=str(declaration_path),
                    value=value,
                )
        if declaration["budget_units"] <= 0:
            raise PipelineError(
                E_INVALID_STAGE,
                "declaration.budget_units must be positive",
                scale=scale,
                path=str(declaration_path),
            )

    def import_all(self) -> dict[str, Any]:
        manifest_path = self.config.workdir / "import_manifest.json"
        lock_path = self.config.workdir / "locks" / "import.lock"
        with locked_file(lock_path):
            scale_entries: dict[str, Any] = {}
            for scale in SCALES:
                data = self.load_scale(scale)
                scale_entries[scale] = {
                    "sample_file": str(data.sample_path.relative_to(self.config.project_root)),
                    "sample_sha256": data.sample_input_fingerprint,
                    "sample_bytes": len(data.sample_bytes),
                    "declaration_file": str(
                        data.declaration_path.relative_to(self.config.project_root)
                    ),
                    "declaration_sha256": sha256_bytes(data.declaration_bytes),
                    "declared_sample_count": data.declaration["sample_count"],
                    "actual_sample_count": len(data.records),
                    "feature_cardinality": data.declaration["feature_cardinality"],
                    "cross_limit": data.declaration["cross_limit"],
                    "budget_units": data.declaration["budget_units"],
                }
            manifest = {
                "schema_version": 1,
                "config_version": self.config.config_version,
                "config_source": self.config.source_label,
                "scales": scale_entries,
            }
            write_json_atomic(manifest_path, manifest, pretty=True)
            return manifest

    def load_import_manifest(self) -> dict[str, Any]:
        path = self.config.workdir / "import_manifest.json"
        if not path.exists():
            return self.import_all()
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return self.import_all()
        if not isinstance(value, dict) or value.get("config_version") != self.config.config_version:
            return self.import_all()
        entries = value.get("scales")
        if isinstance(entries, dict):
            for scale, entry in entries.items():
                if not isinstance(entry, dict):
                    return self.import_all()
                sample_file = entry.get("sample_file")
                declaration_file = entry.get("declaration_file")
                if not isinstance(sample_file, str) or not isinstance(declaration_file, str):
                    return self.import_all()
                sample_path = (self.config.project_root / sample_file).resolve()
                declaration_path = (self.config.project_root / declaration_file).resolve()
                if not sample_path.is_file() or not declaration_path.is_file():
                    return self.import_all()
                if sha256_file(sample_path) != entry.get("sample_sha256"):
                    return self.import_all()
                if sha256_file(declaration_path) != entry.get("declaration_sha256"):
                    return self.import_all()
        return value
