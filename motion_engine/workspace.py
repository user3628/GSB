"""Persisted workspace boundary used by CLI and HTTP clients."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .checkpoints import CheckpointStore
from .errors import invalid_state
from .inputs import SeriesData, load_series_data
from .manifest import load_manifest
from .runner import (
    PrefixAssessment,
    assess_prefix,
    build_report,
    load_invalidation,
    report_bytes,
    run_series,
)
from .util import atomic_write_bytes, pretty_json_bytes, safe_component


class Workspace:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).expanduser().resolve()
        self.metadata_path = self.root / "workspace.json"
        self.store = CheckpointStore(self.root)

    def _write_metadata(self, manifest_path: Path) -> None:
        atomic_write_bytes(
            self.metadata_path,
            pretty_json_bytes(
                {
                    "schema_version": 1,
                    "manifest_path": str(manifest_path.resolve()),
                }
            ),
        )

    def import_manifest(self, manifest_path: str | Path, *, reset: bool = False) -> dict[str, Any]:
        manifest_file = Path(manifest_path).expanduser().resolve()
        manifest = load_manifest(manifest_file)
        loaded = [load_series_data(series) for series in manifest["series"]]
        if reset:
            for series_data in loaded:
                self.store.reset(series_data.series_id)
                invalidation = (
                    self.root
                    / "invalidations"
                    / f"{safe_component(series_data.series_id)}.json"
                )
                if invalidation.exists():
                    invalidation.unlink()
        self._write_metadata(manifest_file)
        return {
            "workspace": str(self.root),
            "manifest_path": str(manifest_file),
            "manifest_file_sha256": manifest["manifest_file_sha256"],
            "series": [
                {
                    "series_id": data.series_id,
                    "shape": list(data.frames.shape),
                    "version_fingerprint": data.version_fingerprint,
                    "frame_file_sha256": data.frame_file_sha256,
                    "frame_content_fingerprint": data.frame_content_fingerprint,
                }
                for data in loaded
            ],
        }

    def manifest_path(self) -> Path:
        if not self.metadata_path.is_file():
            raise invalid_state(
                "workspace is not imported; run the import command first",
                position="workspace.json",
                workspace=str(self.root),
            )
        from .util import load_json_file

        try:
            metadata = load_json_file(self.metadata_path)
        except (OSError, ValueError) as exc:
            raise invalid_state(
                f"cannot read workspace metadata: {exc}",
                position="workspace.json",
                workspace=str(self.root),
            ) from exc
        path = metadata.get("manifest_path") if isinstance(metadata, dict) else None
        if not isinstance(path, str) or not path:
            raise invalid_state(
                "workspace metadata has no manifest_path",
                position="workspace.json.manifest_path",
            )
        return Path(path)

    def load_manifest(self) -> dict[str, Any]:
        return load_manifest(self.manifest_path())

    def series_specs(self) -> list[dict[str, Any]]:
        return list(self.load_manifest()["series"])

    def series_ids(self) -> list[str]:
        return [series["series_id"] for series in self.series_specs()]

    def series_data(self, series_id: str) -> SeriesData:
        manifest = self.load_manifest()
        for series in manifest["series"]:
            if series["series_id"] == series_id:
                return load_series_data(series)
        raise invalid_state(
            "unknown series_id",
            series_id=series_id,
            position="series_id",
            available=[series["series_id"] for series in manifest["series"]],
        )

    def all_series_data(self) -> list[SeriesData]:
        return [load_series_data(series) for series in self.load_manifest()["series"]]

    def run(
        self,
        series_id: str,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return run_series(self.root, self.series_data(series_id), **kwargs)

    def reset_series(self, series_id: str) -> dict[str, Any]:
        data = self.series_data(series_id)
        removed = self.store.reset(series_id)
        invalidation = (
            self.root / "invalidations" / f"{safe_component(series_id)}.json"
        )
        if invalidation.exists():
            invalidation.unlink()
        return {"series_id": series_id, "removed": removed}

    def publish_checkpoint(
        self,
        record: dict[str, Any],
        *,
        expected_series_id: str | None = None,
        expected_next_step: int | None = None,
    ) -> None:
        self.store.append(
            record,
            expected_series_id=expected_series_id,
            expected_next_step=expected_next_step,
        )

    def inspect_series(self, series_id: str) -> dict[str, Any]:
        data = self.series_data(series_id)
        scan = self.store.scan(series_id)
        assessment: PrefixAssessment = assess_prefix(data, scan)
        last_record = assessment.valid_records[-1] if assessment.valid_records else None
        report = build_report(data, assessment.valid_records)
        return {
            "series_id": series_id,
            "frame_shape": list(data.frames.shape),
            "prediction_steps": data.prediction_steps,
            "timeline": data.series["timeline"],
            "checkpoint": {
                "count": len(assessment.valid_records),
                "last_sequence": assessment.last_valid_step,
                "next_sequence": assessment.last_valid_step + 1,
                "complete": assessment.last_valid_step == data.prediction_steps,
                "last_state_digest": last_record.get("state_digest") if last_record else None,
                "last_version_fingerprint": last_record.get("version_fingerprint") if last_record else None,
                "ignored_files": scan.ignored_files,
                "duplicate_sequences": scan.duplicate_sequences,
                "issues": scan.issues,
            },
            "version_fingerprints": {
                "current": data.version_fingerprint,
                "checkpoint": last_record.get("version_fingerprint") if last_record else None,
                "stages": data.stage_fingerprints,
                "changes": assessment.reasons,
            },
            "invalidated": assessment.rebuild_from_step is not None,
            "rebuild_from_step": assessment.rebuild_from_step,
            "invalidation_reasons": assessment.reasons,
            "last_persisted_invalidation": load_invalidation(self.root, series_id),
            "partial_report_fingerprint": report["report_fingerprint"],
        }

    def all_status(self) -> dict[str, Any]:
        statuses: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        for series in self.load_manifest()["series"]:
            series_id = series["series_id"]
            try:
                statuses.append(self.inspect_series(series_id))
            except Exception as exc:  # surface per-series validation failures
                from .errors import EngineError

                errors.append(
                    exc.to_dict()
                    if isinstance(exc, EngineError)
                    else {
                        "code": "E_INVALID_STATE",
                        "message": str(exc),
                        "series_id": series_id,
                    }
                )
        return {
            "workspace": str(self.root),
            "manifest_path": str(self.manifest_path()),
            "series": statuses,
            "errors": errors,
        }

    def report(self, series_id: str, *, canonical: bool = False) -> dict[str, Any] | bytes:
        data = self.series_data(series_id)
        scan = self.store.scan(series_id)
        assessment = assess_prefix(data, scan)
        report = build_report(data, assessment.valid_records)
        return report_bytes(report) if canonical else report
