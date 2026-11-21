"""JSONL shard loading, manifest observation and row-level validation."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .errors import ErrorCode, PipelineError
from .util import (
    atomic_write_json,
    canonical_hash,
    resolve_under,
    sha256_bytes,
)


REQUIRED_SAMPLE_FIELDS = (
    "impression_id",
    "user_features",
    "item_features",
    "context_features",
    "ctr_label",
    "source",
    "lang",
)


@dataclass(frozen=True)
class ManifestShard:
    shard_id: str
    index: int
    relative_path: str
    absolute_path: Path
    rows: int
    ctr_positive_count: int | None
    ctr_positive_ratio: float | None
    cvr_positive_count: int | None
    cvr_positive_ratio: float | None
    conversion_rate_on_exposure: float | None
    missing_fields: dict[str, int]
    declared_sha256: str | None


@dataclass
class ManifestBundle:
    path: Path
    digest: str
    payload: dict[str, Any]
    shards: list[ManifestShard]


@dataclass
class LoadedShard:
    info: ManifestShard
    rows: list[dict[str, Any]]
    raw_bytes: bytes
    content_digest: str


@dataclass
class DataBundle:
    manifest: ManifestBundle
    shards: list[LoadedShard]
    data_content_fingerprint: str

    def by_id(self) -> dict[str, LoadedShard]:
        return {shard.info.shard_id: shard for shard in self.shards}


@dataclass
class ValidationReport:
    label_errors: list[dict[str, Any]] = field(default_factory=list)
    funnel_violations: list[dict[str, Any]] = field(default_factory=list)
    actual_counts: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return not self.label_errors and not self.funnel_violations


def _as_count(value: Any, field_name: str, shard_id: str, row_number: int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise PipelineError(
            ErrorCode.E_MANIFEST_MISMATCH,
            f"manifest field {field_name} must be a non-negative integer",
            stage="manifest",
            shard_id=shard_id,
            row_number=row_number,
            details={"field": field_name, "value": value},
        )
    return value


def _as_ratio(value: Any, field_name: str, shard_id: str, row_number: int | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PipelineError(
            ErrorCode.E_MANIFEST_MISMATCH,
            f"manifest field {field_name} must be numeric",
            stage="manifest",
            shard_id=shard_id,
            row_number=row_number,
            details={"field": field_name, "value": value},
        )
    number = float(value)
    if not math.isfinite(number) or number < 0.0 or number > 1.0:
        raise PipelineError(
            ErrorCode.E_MANIFEST_MISMATCH,
            f"manifest field {field_name} must be in [0, 1]",
            stage="manifest",
            shard_id=shard_id,
            row_number=row_number,
            details={"field": field_name, "value": value},
        )
    return number


def load_manifest(manifest_path: Path) -> ManifestBundle:
    manifest_path = Path(manifest_path).resolve()
    try:
        raw = manifest_path.read_bytes()
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PipelineError(
            ErrorCode.E_MANIFEST_MISMATCH,
            "cannot read shard manifest",
            stage="manifest",
            details={"path": str(manifest_path), "reason": str(exc)},
        ) from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("shards"), list):
        raise PipelineError(
            ErrorCode.E_MANIFEST_MISMATCH,
            "manifest must contain a shards list",
            stage="manifest",
            details={"path": str(manifest_path)},
        )

    shards: list[ManifestShard] = []
    manifest_dir = manifest_path.parent
    for position, item in enumerate(payload["shards"]):
        row_number = position + 1
        if not isinstance(item, dict):
            raise PipelineError(
                ErrorCode.E_MANIFEST_MISMATCH,
                "manifest shard entry must be an object",
                stage="manifest",
                row_number=row_number,
            )
        shard_id = item.get("shard_id")
        index = item.get("index")
        relative = item.get("path")
        rows = item.get("rows")
        if not isinstance(shard_id, str) or not shard_id:
            raise PipelineError(
                ErrorCode.E_MANIFEST_MISMATCH,
                "manifest shard_id must be a non-empty string",
                stage="manifest",
                row_number=row_number,
            )
        if not isinstance(index, int) or isinstance(index, bool) or index < 0:
            raise PipelineError(
                ErrorCode.E_MANIFEST_MISMATCH,
                "manifest shard index must be a non-negative integer",
                stage="manifest",
                shard_id=shard_id,
                row_number=row_number,
            )
        if not isinstance(relative, str) or not relative:
            raise PipelineError(
                ErrorCode.E_MANIFEST_MISMATCH,
                "manifest shard path must be a non-empty relative string",
                stage="manifest",
                shard_id=shard_id,
                row_number=row_number,
            )
        if not isinstance(rows, int) or isinstance(rows, bool) or rows < 0:
            raise PipelineError(
                ErrorCode.E_MANIFEST_MISMATCH,
                "manifest rows must be a non-negative integer",
                stage="manifest",
                shard_id=shard_id,
                row_number=row_number,
            )
        try:
            shard_path = resolve_under(manifest_dir, relative, f"shards[{position}].path")
        except PipelineError as exc:
            raise PipelineError(
                ErrorCode.E_MANIFEST_MISMATCH,
                exc.message,
                stage="manifest",
                shard_id=shard_id,
                row_number=row_number,
                details=exc.details,
            ) from exc

        missing = item.get("missing_fields", {})
        if not isinstance(missing, dict) or any(
            not isinstance(key, str) or not isinstance(value, int) or isinstance(value, bool) or value < 0
            for key, value in missing.items()
        ):
            raise PipelineError(
                ErrorCode.E_MANIFEST_MISMATCH,
                "manifest missing_fields must map field paths to non-negative counts",
                stage="manifest",
                shard_id=shard_id,
                row_number=row_number,
            )
        declared_sha = item.get("sha256")
        if declared_sha is not None and not isinstance(declared_sha, str):
            raise PipelineError(
                ErrorCode.E_MANIFEST_MISMATCH,
                "manifest sha256 must be a string when present",
                stage="manifest",
                shard_id=shard_id,
                row_number=row_number,
            )

        shards.append(
            ManifestShard(
                shard_id=shard_id,
                index=index,
                relative_path=relative,
                absolute_path=shard_path,
                rows=rows,
                ctr_positive_count=_as_count(item.get("ctr_positive_count"), "ctr_positive_count", shard_id, row_number),
                ctr_positive_ratio=_as_ratio(item.get("ctr_positive_ratio"), "ctr_positive_ratio", shard_id, row_number),
                cvr_positive_count=_as_count(item.get("cvr_positive_count"), "cvr_positive_count", shard_id, row_number),
                cvr_positive_ratio=_as_ratio(item.get("cvr_positive_ratio"), "cvr_positive_ratio", shard_id, row_number),
                conversion_rate_on_exposure=_as_ratio(
                    item.get("conversion_rate_on_exposure"), "conversion_rate_on_exposure", shard_id, row_number
                ),
                missing_fields={str(key): int(value) for key, value in missing.items()},
                declared_sha256=declared_sha,
            )
        )

    shards.sort(key=lambda shard: shard.index)
    indices = [shard.index for shard in shards]
    ids = [shard.shard_id for shard in shards]
    if len(set(indices)) != len(indices):
        raise PipelineError(ErrorCode.E_MANIFEST_MISMATCH, "manifest contains duplicate shard indices", stage="manifest")
    if len(set(ids)) != len(ids):
        raise PipelineError(ErrorCode.E_MANIFEST_MISMATCH, "manifest contains duplicate shard ids", stage="manifest")
    return ManifestBundle(
        path=manifest_path,
        digest=sha256_bytes(raw),
        payload=payload,
        shards=shards,
    )


def register_manifest_observation(manifest: ManifestBundle, state_root: Path) -> Path:
    """Persist the manifest declaration as a read-only observation of truth."""

    observation = {
        "format": "ctr-cvr-manifest-observation-v1",
        "manifest_path": manifest.path.name,
        "manifest_sha256": manifest.digest,
        "read_only": True,
        "shards": [
            {
                "shard_id": shard.shard_id,
                "index": shard.index,
                "path": shard.relative_path,
                "rows": shard.rows,
                "ctr_positive_count": shard.ctr_positive_count,
                "ctr_positive_ratio": shard.ctr_positive_ratio,
                "cvr_positive_count": shard.cvr_positive_count,
                "cvr_positive_ratio": shard.cvr_positive_ratio,
                "conversion_rate_on_exposure": shard.conversion_rate_on_exposure,
                "missing_fields": shard.missing_fields,
            }
            for shard in manifest.shards
        ],
    }
    path = state_root / "observations" / f"manifest-{manifest.digest[:16]}.json"
    if path.exists():
        return path
    atomic_write_json(path, observation, mode=0o444)
    return path


def load_rows(manifest: ManifestBundle) -> list[LoadedShard]:
    loaded: list[LoadedShard] = []
    for shard in manifest.shards:
        try:
            raw = shard.absolute_path.read_bytes()
        except OSError as exc:
            raise PipelineError(
                ErrorCode.E_IO,
                "cannot read shard file",
                stage="feature",
                shard_id=shard.shard_id,
                details={"path": shard.relative_path, "reason": str(exc)},
            ) from exc
        content_digest = sha256_bytes(raw)
        rows: list[dict[str, Any]] = []
        for line_number, line in enumerate(raw.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise PipelineError(
                    ErrorCode.E_DATA_INVALID,
                    "JSONL row is not valid UTF-8 JSON",
                    stage="feature",
                    shard_id=shard.shard_id,
                    row_number=line_number,
                    details={"reason": str(exc)},
                ) from exc
            if not isinstance(record, dict):
                raise PipelineError(
                    ErrorCode.E_DATA_INVALID,
                    "JSONL row must be an object",
                    stage="feature",
                    shard_id=shard.shard_id,
                    row_number=line_number,
                )
            record["_row_number"] = line_number
            rows.append(record)
        loaded.append(
            LoadedShard(
                info=shard,
                rows=rows,
                raw_bytes=raw,
                content_digest=content_digest,
            )
        )
    loaded.sort(key=lambda item: item.info.index)
    return loaded


def data_content_fingerprint(shards: list[LoadedShard], manifest_digest: str) -> str:
    return canonical_hash(
        {
            "manifest_sha256": manifest_digest,
            "shards": [
                {
                    "shard_id": shard.info.shard_id,
                    "index": shard.info.index,
                    "sha256": shard.content_digest,
                    "rows": len(shard.rows),
                }
                for shard in sorted(shards, key=lambda item: item.info.index)
            ],
        }
    )


def make_data_bundle(manifest: ManifestBundle, shards: list[LoadedShard]) -> DataBundle:
    return DataBundle(
        manifest=manifest,
        shards=shards,
        data_content_fingerprint=data_content_fingerprint(shards, manifest.digest),
    )


def _label_error(
    shard: LoadedShard,
    row: dict[str, Any],
    field_name: str,
    value: Any,
    reason: str,
) -> dict[str, Any]:
    return {
        "code": ErrorCode.E_LABEL_INVALID,
        "message": f"{field_name} must be integer 0 or 1",
        "field": field_name,
        "value": value,
        "reason": reason,
        "shard_id": shard.info.shard_id,
        "row_number": int(row["_row_number"]),
        "impression_id": row.get("impression_id"),
    }


def apply_data_injection(shards: list[LoadedShard], injection: str | None) -> None:
    if injection not in {"invalid_ctr_label", "cvr_without_ctr"}:
        return
    target = next((shard for shard in shards if shard.rows), None)
    if target is None:
        return
    row = target.rows[0]
    if injection == "invalid_ctr_label":
        row["ctr_label"] = 2
    elif injection == "cvr_without_ctr":
        row["ctr_label"] = 0
        row["cvr_label"] = 1


def validate_rows(shards: list[LoadedShard]) -> ValidationReport:
    report = ValidationReport()
    for shard in sorted(shards, key=lambda item: item.info.index):
        counts = {
            "rows": len(shard.rows),
            "ctr_positive_count": 0,
            "cvr_positive_count": 0,
            "cvr_observed_count": 0,
        }
        for row in shard.rows:
            row_number = int(row["_row_number"])
            for required in REQUIRED_SAMPLE_FIELDS:
                if required not in row:
                    raise PipelineError(
                        ErrorCode.E_DATA_INVALID,
                        f"required sample field is missing: {required}",
                        stage="feature",
                        shard_id=shard.info.shard_id,
                        row_number=row_number,
                        impression_id=row.get("impression_id"),
                    )
            impression_id = row.get("impression_id")
            if not isinstance(impression_id, str) or not impression_id:
                raise PipelineError(
                    ErrorCode.E_DATA_INVALID,
                    "impression_id must be a non-empty string",
                    stage="feature",
                    shard_id=shard.info.shard_id,
                    row_number=row_number,
                )

            ctr_value = row.get("ctr_label")
            ctr_valid = isinstance(ctr_value, int) and not isinstance(ctr_value, bool) and ctr_value in (0, 1)
            if not ctr_valid:
                report.label_errors.append(
                    _label_error(shard, row, "ctr_label", ctr_value, "expected integer 0 or 1")
                )
                continue
            counts["ctr_positive_count"] += ctr_value

            cvr_present = "cvr_label" in row
            cvr_value = row.get("cvr_label")
            cvr_valid = (
                not cvr_present
                or cvr_value is None
                or (isinstance(cvr_value, int) and not isinstance(cvr_value, bool) and cvr_value in (0, 1))
            )
            if not cvr_valid:
                report.label_errors.append(
                    _label_error(shard, row, "cvr_label", cvr_value, "expected integer 0 or 1 when present")
                )
                continue
            if cvr_present and cvr_value is not None:
                counts["cvr_observed_count"] += 1
                if cvr_value == 1:
                    counts["cvr_positive_count"] += 1
                if ctr_value == 0:
                    report.funnel_violations.append(
                        {
                            "code": ErrorCode.E_FUNNEL_VIOLATION,
                            "check": "cvr_requires_ctr_positive",
                            "message": "cvr_label must be absent when ctr_label=0",
                            "violated_value": {"ctr_label": 0, "cvr_label": cvr_value},
                            "shard_id": shard.info.shard_id,
                            "row_number": row_number,
                            "impression_id": impression_id,
                        }
                    )
        report.actual_counts[shard.info.shard_id] = counts
    return report


def _ratio_close(actual: float | None, expected: float | None) -> bool:
    if actual is None or expected is None:
        return True
    return math.isclose(actual, expected, rel_tol=0.0, abs_tol=1e-12)


def validate_manifest_truth(
    shards: list[LoadedShard],
    report: ValidationReport,
    *,
    injection: str | None = None,
) -> None:
    for shard in sorted(shards, key=lambda item: item.info.index):
        expected_rows = shard.info.rows + (1 if injection == "manifest_mismatch" and shard.info.index == shards[0].info.index else 0)
        actual = report.actual_counts[shard.info.shard_id]
        actual_rows = int(actual["rows"])
        if actual_rows != expected_rows:
            raise PipelineError(
                ErrorCode.E_MANIFEST_MISMATCH,
                "manifest row count does not match actual JSONL rows",
                stage="manifest",
                shard_id=shard.info.shard_id,
                row_number=min(actual_rows, expected_rows) + 1,
                details={
                    "expected_rows": expected_rows,
                    "actual_rows": actual_rows,
                    "shard_path": shard.info.relative_path,
                },
            )
        if shard.info.declared_sha256 is not None and shard.info.declared_sha256 != shard.content_digest:
            raise PipelineError(
                ErrorCode.E_MANIFEST_MISMATCH,
                "manifest shard sha256 does not match shard bytes",
                stage="manifest",
                shard_id=shard.info.shard_id,
                row_number=1,
                details={
                    "expected_sha256": shard.info.declared_sha256,
                    "actual_sha256": shard.content_digest,
                },
            )
        if shard.info.ctr_positive_ratio is not None:
            actual_ratio = actual["ctr_positive_count"] / actual_rows if actual_rows else 0.0
            if not _ratio_close(actual_ratio, shard.info.ctr_positive_ratio):
                raise PipelineError(
                    ErrorCode.E_MANIFEST_MISMATCH,
                    "manifest CTR positive ratio does not match observed rows",
                    stage="manifest",
                    shard_id=shard.info.shard_id,
                    row_number=1,
                    details={"expected": shard.info.ctr_positive_ratio, "actual": actual_ratio},
                )
        if shard.info.ctr_positive_count is not None and actual["ctr_positive_count"] != shard.info.ctr_positive_count:
            raise PipelineError(
                ErrorCode.E_MANIFEST_MISMATCH,
                "manifest CTR positive count does not match observed rows",
                stage="manifest",
                shard_id=shard.info.shard_id,
                row_number=1,
                details={"expected": shard.info.ctr_positive_count, "actual": actual["ctr_positive_count"]},
            )
        if shard.info.cvr_positive_ratio is not None:
            actual_ratio = actual["cvr_positive_count"] / actual_rows if actual_rows else 0.0
            if not _ratio_close(actual_ratio, shard.info.cvr_positive_ratio):
                raise PipelineError(
                    ErrorCode.E_MANIFEST_MISMATCH,
                    "manifest CVR positive ratio does not match observed rows",
                    stage="manifest",
                    shard_id=shard.info.shard_id,
                    row_number=1,
                    details={"expected": shard.info.cvr_positive_ratio, "actual": actual_ratio},
                )
        if shard.info.cvr_positive_count is not None and actual["cvr_positive_count"] != shard.info.cvr_positive_count:
            raise PipelineError(
                ErrorCode.E_MANIFEST_MISMATCH,
                "manifest CVR positive count does not match observed rows",
                stage="manifest",
                shard_id=shard.info.shard_id,
                row_number=1,
                details={"expected": shard.info.cvr_positive_count, "actual": actual["cvr_positive_count"]},
            )
        if shard.info.conversion_rate_on_exposure is not None:
            denominator = int(actual["ctr_positive_count"])
            actual_rate = actual["cvr_positive_count"] / denominator if denominator else 0.0
            if not _ratio_close(actual_rate, shard.info.conversion_rate_on_exposure):
                raise PipelineError(
                    ErrorCode.E_MANIFEST_MISMATCH,
                    "manifest conversion rate does not match observed rows",
                    stage="manifest",
                    shard_id=shard.info.shard_id,
                    row_number=1,
                    details={"expected": shard.info.conversion_rate_on_exposure, "actual": actual_rate},
                )
