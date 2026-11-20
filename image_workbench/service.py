from __future__ import annotations

import copy
import json
import threading
from dataclasses import replace
from pathlib import Path
from typing import Any

import numpy as np

from .atomic import atomic_write_bytes, atomic_write_json
from .codecs import (
    decode_jpeg,
    decode_png,
    descriptor_bytes,
    encode_jpeg,
    encode_png,
)
from .control import StopController
from .errors import E_NOT_FOUND, E_VALIDATION, WorkbenchError
from .fingerprints import STAGES, compute_stage_fingerprints
from .hashing import digest_json
from .legacy import LegacyConfig, upgrade_legacy_manifest
from .manifest import DEFAULT_FORMAT_CONFIG, load_manifest, validate_format_config
from .masks import MaskRegion
from .model import FragmentModel
from .numeric import validate_strength
from .pipeline import (
    PipelineCache,
    run_pipeline,
)
from .project_io import load_project, save_project
from .snapshots import SnapshotManager, finalize_snapshot

SESSION_FORMAT = "image-workbench-session"
SESSION_VERSION = 1


class WorkbenchService:
    def __init__(
        self,
        root: str | Path,
        *,
        runtime_dir: str | Path | None = None,
        autoload: bool = True,
    ) -> None:
        self.root = Path(root).resolve()
        self.runtime_dir = Path(runtime_dir).resolve() if runtime_dir is not None else self.root / "runtime"
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = self.runtime_dir / "session.json"
        self.snapshots = SnapshotManager(self.runtime_dir / "snapshots")
        self.control = StopController(self.runtime_dir / "stop.request")
        self._lock = threading.RLock()
        self._run_guard = threading.Lock()
        self.project_name: str | None = None
        self.manifest_path: str | None = None
        self.fragments: dict[str, FragmentModel] = {}
        self.project_metadata: dict[str, Any] = {}
        self.format_config: dict[str, Any] = {}
        self.cache = PipelineCache()
        self.last_run: dict[str, Any] | None = None
        self.last_legacy_upgrade: dict[str, Any] | None = None
        if autoload:
            self.load_state()

    @property
    def has_project(self) -> bool:
        return bool(self.fragments)

    def _fragment_list(self) -> list[FragmentModel]:
        return list(self.fragments.values())

    def import_manifest(
        self,
        manifest_path: str | Path,
        *,
        legacy: bool = False,
        legacy_config: LegacyConfig | dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self._lock:
            if legacy:
                upgrade, v2_document = upgrade_legacy_manifest(manifest_path, legacy_config)
                if isinstance(legacy_config, LegacyConfig):
                    effective_legacy_config = legacy_config
                else:
                    effective_legacy_config = LegacyConfig.from_dict(legacy_config)
                models = upgrade.fragments
                self.project_name = upgrade.project_name
                self.project_metadata = copy.deepcopy(upgrade.metadata)
                self.format_config = copy.deepcopy(upgrade.format_config)
                self.manifest_path = str(upgrade.source_path) if upgrade.source_path else str(manifest_path)
                upgraded_path = self.runtime_dir / "upgraded_legacy_manifest_v2.json"
                atomic_write_json(upgraded_path, v2_document)
                self.last_legacy_upgrade = {
                    "source_schema_version": 1,
                    "target_schema_version": 2,
                    "source_manifest": str(Path(manifest_path).resolve()),
                    "upgraded_manifest": str(upgraded_path),
                    "config": effective_legacy_config.to_dict(),
                    "fragment_ids": [model.fragment_id for model in models],
                }
            else:
                project = load_manifest(manifest_path)
                models = project.fragments
                self.project_name = project.project_name
                self.project_metadata = copy.deepcopy(project.metadata)
                self.format_config = copy.deepcopy(project.format_config)
                self.manifest_path = str(project.source_path) if project.source_path else str(manifest_path)
                self.last_legacy_upgrade = None
            self.fragments = {model.fragment_id: model for model in models}
            self.cache = PipelineCache()
            self.last_run = None
            self._save_state_locked()
            return self.project_summary()

    def project_summary(self) -> dict[str, Any]:
        return {
            "project_name": self.project_name,
            "manifest_path": self.manifest_path,
            "fragment_count": len(self.fragments),
            "fragment_ids": list(self.fragments),
            "format_config": copy.deepcopy(self.format_config),
            "project_metadata": copy.deepcopy(self.project_metadata),
            "legacy_upgrade": copy.deepcopy(self.last_legacy_upgrade),
        }

    def run(self, *, force: bool = False) -> dict[str, Any]:
        if not self.has_project:
            raise WorkbenchError(E_VALIDATION, "No project is loaded")
        if not self._run_guard.acquire(blocking=False):
            from .errors import E_BUSY

            raise WorkbenchError(E_BUSY, "A pipeline run is already in progress")
        try:
            with self._lock:
                models = self._fragment_list()
                previous_cache = None if force else PipelineCache.from_dict(self.cache.to_dict())
                format_config = copy.deepcopy(self.format_config)
            sequence = self.snapshots.next_sequence()
            stage_records: list[dict[str, Any]] = []

            def sink(record: dict[str, Any]) -> None:
                self.snapshots.write_stage_record(sequence, record)
                value = copy.deepcopy(record)
                value["snapshot_sequence"] = sequence
                stage_records.append(value)

            result = run_pipeline(
                models,
                format_config,
                previous_cache=previous_cache,
                stop_requested=self.control.is_requested,
                record_sink=sink,
            )
            summary = result.to_summary()
            payload = {
                "kind": "pipeline-run",
                "project_name": self.project_name,
                "summary": {
                    "summary_digest": summary["summary_digest"],
                    "processed_order": summary["processed_order"],
                    "stopped": summary["stopped"],
                    "stop_after_fragment": summary["stop_after_fragment"],
                },
            }
            snapshot = finalize_snapshot(sequence, stage_records, payload=payload)
            snapshot_receipt = self.snapshots.publish(snapshot)
            with self._lock:
                self.cache = result.cache
                self.last_run = {
                    **summary,
                    "snapshot_sequence": sequence,
                    "snapshot_digest": snapshot["digest"],
                    "stage_records": stage_records,
                    "stage_statuses": {
                        fragment_id: {
                            stage: copy.deepcopy(result.results[fragment_id].cache.statuses[stage])
                            for stage in STAGES
                        }
                        for fragment_id in result.processed_order
                    },
                }
                self._save_state_locked()
            return {
                **summary,
                "snapshot": snapshot_receipt,
                "stage_statuses": self.last_run["stage_statuses"],
            }
        finally:
            self._run_guard.release()

    def request_stop(self) -> dict[str, Any]:
        self.control.request_stop()
        return self.control.snapshot()

    def resume(self) -> dict[str, Any]:
        self.control.resume()
        return self.control.snapshot()

    def status(self) -> dict[str, Any]:
        with self._lock:
            fragments = {
                fragment_id: self.fragment_status(fragment_id, include_preview=False)
                for fragment_id in self.fragments
            }
            return {
                "project": self.project_summary(),
                "control": self.control.snapshot(),
                "snapshot": self.snapshots.current_status(),
                "last_run": copy.deepcopy(self.last_run),
                "fragments": fragments,
            }

    def fragment_status(self, fragment_id: str, *, include_preview: bool = True) -> dict[str, Any]:
        model = self.fragments.get(fragment_id)
        if model is None:
            raise WorkbenchError(E_NOT_FOUND, "Fragment not found", fragment_id=fragment_id)
        entry = self.cache.fragments.get(fragment_id)
        cached = (
            {
                "fingerprints": copy.deepcopy(entry.fingerprints),
                "statuses": copy.deepcopy(entry.statuses),
                "earliest_invalidated_stage": entry.earliest_invalidated_stage,
            }
            if entry is not None
            else None
        )
        response: dict[str, Any] = {
            "fragment_id": fragment_id,
            "pixels": {
                "width": model.width,
                "height": model.height,
                "digest": model.pixel_digest,
            },
            "mask": {
                "rects": model.mask.to_lists(),
                "digest": model.mask_digest,
                "pixel_count": int(model.mask_pixels.sum()),
            },
            "style_reference": model.style_reference,
            "style_strength": model.style_strength,
            "repair_iterations": model.repair_iterations,
            "output_format": model.output_format,
            "metadata": copy.deepcopy(model.metadata),
            "cached": cached,
        }
        if include_preview:
            reference = self.fragments.get(model.style_reference)
            if reference is not None:
                current = compute_stage_fingerprints(model, reference, self.format_config)
                earliest = None
                reason = "not built"
                preview: dict[str, Any] = {}
                for stage in STAGES:
                    cached_fp = entry.fingerprints.get(stage) if entry is not None else None
                    changed = cached_fp is not None and cached_fp != current[stage].fingerprint
                    if earliest is None and changed:
                        earliest = stage
                        if entry is not None:
                            from .fingerprints import explain_invalidation

                            reason = explain_invalidation(
                                stage,
                                entry.dependencies.get(stage, {}),
                                current[stage].dependencies,
                            )
                    preview[stage] = {
                        "fingerprint": current[stage].fingerprint,
                        "input_fingerprint": current[stage].input_fingerprint,
                        "config_fingerprint": current[stage].config_fingerprint,
                        "changed_since_cache": changed,
                    }
                response["fingerprints_preview"] = preview
                response["preview_earliest_invalidated_stage"] = earliest
                response["preview_reason"] = reason if earliest else "all stage fingerprints match cache"
        return response

    def update_fragment(self, fragment_id: str, changes: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            model = self.fragments.get(fragment_id)
            if model is None:
                raise WorkbenchError(E_NOT_FOUND, "Fragment not found", fragment_id=fragment_id)
            allowed = {
                "style_strength",
                "repair_iterations",
                "style_reference",
                "output_format",
                "mask_rects",
                "metadata",
            }
            unknown = set(changes) - allowed
            if unknown:
                raise WorkbenchError(E_VALIDATION, "Unsupported fragment update fields", fragment_id=fragment_id, details={"fields": sorted(unknown)})
            values: dict[str, Any] = {}
            if "style_strength" in changes:
                values["style_strength"] = validate_strength(
                    changes["style_strength"],
                    fragment_id=fragment_id,
                )
            if "repair_iterations" in changes:
                value = changes["repair_iterations"]
                if isinstance(value, bool) or not isinstance(value, int):
                    raise WorkbenchError(E_VALIDATION, "repair_iterations must be an integer", fragment_id=fragment_id)
                values["repair_iterations"] = value
            if "style_reference" in changes:
                reference = str(changes["style_reference"])
                if reference not in self.fragments:
                    raise WorkbenchError(E_NOT_FOUND, "Updated style reference fragment not found", fragment_id=fragment_id)
                values["style_reference"] = reference
            if "output_format" in changes:
                values["output_format"] = str(changes["output_format"])
            if "mask_rects" in changes:
                values["mask"] = MaskRegion.from_dict(changes["mask_rects"])
            if "metadata" in changes:
                values["metadata"] = copy.deepcopy(changes["metadata"])
            updated = replace(model, **values)
            self.fragments[fragment_id] = updated
            self._save_state_locked()
            return self.fragment_status(fragment_id)

    def set_format_config(self, value: dict[str, Any]) -> dict[str, Any]:
        value = validate_format_config(value)
        with self._lock:
            self.format_config = copy.deepcopy(value)
            self._save_state_locked()
            return copy.deepcopy(self.format_config)

    def convert(self, fragment_id: str, target_format: str, output_dir: str | Path | None = None) -> dict[str, Any]:
        target = target_format.lower()
        if target == "jpg":
            target = "jpeg"
        if target not in {"png", "jpeg", "json"}:
            raise WorkbenchError(E_VALIDATION, "target_format must be png, jpeg or json")
        with self._lock:
            model = self.fragments.get(fragment_id)
            if model is None:
                raise WorkbenchError(E_NOT_FOUND, "Fragment not found", fragment_id=fragment_id)
            entry = self.cache.fragments.get(fragment_id)
            if entry is None or "style" not in entry.artifacts:
                raise WorkbenchError(E_VALIDATION, "Run repair/style pipeline before exporting a fragment", fragment_id=fragment_id)
            pixels = np.array(entry.artifacts["style"], dtype=np.uint8, copy=True)
            exported_model = model.with_pixels(pixels, metadata=model.metadata, loss_items=model.loss_items)
            directory = Path(output_dir).resolve() if output_dir is not None else self.runtime_dir / "exports"
            directory.mkdir(parents=True, exist_ok=True)
            files: dict[str, str] = {}
            descriptor: dict[str, Any]
            if target == "png":
                png_config = self.format_config.get("png", {})
                image_bytes, descriptor = encode_png(
                    exported_model,
                    compress_level=int(png_config.get("compress_level", 6)),
                )
                image_path = directory / f"{fragment_id}.png"
                atomic_write_bytes(image_path, image_bytes)
                files["image"] = str(image_path)
            elif target == "jpeg":
                jpeg_config = self.format_config.get("jpeg", {})
                image_bytes, descriptor = encode_jpeg(
                    exported_model,
                    quality=int(jpeg_config.get("quality", 95)),
                    subsampling=int(jpeg_config.get("subsampling", 0)),
                )
                image_path = directory / f"{fragment_id}.jpg"
                atomic_write_bytes(image_path, image_bytes)
                files["image"] = str(image_path)
            else:
                from .codecs import descriptor_for_model

                descriptor = descriptor_for_model(exported_model, "png", loss_items=exported_model.loss_items)
            descriptor_path = directory / f"{fragment_id}.{target}.iwb.json"
            atomic_write_bytes(descriptor_path, descriptor_bytes(descriptor))
            files["descriptor"] = str(descriptor_path)
            return {
                "fragment_id": fragment_id,
                "target_format": target,
                "files": files,
                "loss_items": copy.deepcopy(descriptor.get("loss_items", [])),
                "descriptor_digest": descriptor.get("descriptor_digest"),
                "mask_pixel_count": int(exported_model.mask_pixels.sum()),
            }

    def import_exchange(
        self,
        image_path: str | Path,
        *,
        descriptor_path: str | Path | None = None,
        image_format: str | None = None,
        replace_existing: bool = True,
    ) -> dict[str, Any]:
        path = Path(image_path).resolve()
        image_bytes = path.read_bytes()
        descriptor_value = Path(descriptor_path).read_bytes() if descriptor_path else None
        format_value = (image_format or path.suffix.lstrip(".")).lower()
        if format_value == "jpg":
            format_value = "jpeg"
        if format_value == "png":
            model = decode_png(image_bytes, descriptor_value)
        elif format_value == "jpeg":
            model = decode_jpeg(image_bytes, descriptor_value)
        else:
            raise WorkbenchError(E_VALIDATION, "Only PNG and JPEG exchange images can be imported", details={"image_format": format_value})
        with self._lock:
            if model.fragment_id in self.fragments and not replace_existing:
                raise WorkbenchError(E_VALIDATION, "Fragment already exists", fragment_id=model.fragment_id)
            self.fragments[model.fragment_id] = model
            self._save_state_locked()
            return {
                "fragment_id": model.fragment_id,
                "pixel_digest": model.pixel_digest,
                "mask_digest": model.mask_digest,
                "mask_pixel_count": int(model.mask_pixels.sum()),
                "style_strength": model.style_strength,
                "metadata": copy.deepcopy(model.metadata),
                "loss_items": copy.deepcopy(model.loss_items),
            }

    def export_project(
        self,
        path: str | Path,
        *,
        artifact: str = "style",
        project_name: str | None = None,
    ) -> dict[str, Any]:
        if artifact not in {"source", "repair", "style"}:
            raise WorkbenchError(E_VALIDATION, "artifact must be source, repair or style")
        with self._lock:
            models: list[FragmentModel] = []
            for fragment_id, model in self.fragments.items():
                if artifact == "source":
                    models.append(model)
                    continue
                entry = self.cache.fragments.get(fragment_id)
                if entry is None or artifact not in entry.artifacts:
                    raise WorkbenchError(E_VALIDATION, f"Artifact {artifact} is not built", fragment_id=fragment_id)
                models.append(model.with_pixels(entry.artifacts[artifact]))
            document = save_project(
                path,
                models,
                project_metadata=self.project_metadata,
                project_name=project_name or self.project_name or "untitled",
            )
            return {
                "path": str(Path(path).resolve()),
                "artifact": artifact,
                "fragment_count": len(models),
                "document_digest": digest_json(document),
            }

    def load_project_file(self, path: str | Path) -> dict[str, Any]:
        models, metadata, project_name = load_project(path)
        with self._lock:
            self.fragments = {model.fragment_id: model for model in models}
            self.project_name = project_name
            self.project_metadata = copy.deepcopy(metadata)
            self.format_config = copy.deepcopy(DEFAULT_FORMAT_CONFIG)
            self.cache = PipelineCache()
            self.manifest_path = str(Path(path).resolve())
            self.last_run = None
            self._save_state_locked()
            return self.project_summary()

    def snapshot_status(self) -> dict[str, Any]:
        return self.snapshots.current_status()

    def inspect_snapshot(self, candidate: dict[str, Any]) -> dict[str, Any]:
        return self.snapshots.inspect_candidate(candidate)

    def publish_snapshot(self, candidate: dict[str, Any]) -> dict[str, Any]:
        return self.snapshots.publish(candidate)

    def _save_state_locked(self) -> None:
        document = {
            "format": SESSION_FORMAT,
            "session_version": SESSION_VERSION,
            "project_name": self.project_name,
            "manifest_path": self.manifest_path,
            "project_metadata": copy.deepcopy(self.project_metadata),
            "format_config": copy.deepcopy(self.format_config),
            "fragments": [model.to_internal_dict() for model in self.fragments.values()],
            "cache": self.cache.to_dict(),
            "last_run": copy.deepcopy(self.last_run),
            "last_legacy_upgrade": copy.deepcopy(self.last_legacy_upgrade),
        }
        atomic_write_json(self.state_path, document)

    def save_state(self) -> None:
        with self._lock:
            self._save_state_locked()

    def load_state(self) -> bool:
        if not self.state_path.exists():
            return False
        try:
            document = json.loads(self.state_path.read_text(encoding="utf-8"))
            if document.get("format") != SESSION_FORMAT or int(document.get("session_version", -1)) != SESSION_VERSION:
                raise WorkbenchError(E_VALIDATION, "Session state format/version is unsupported")
            models = [FragmentModel.from_internal_dict(item) for item in document.get("fragments", [])]
            with self._lock:
                self.project_name = document.get("project_name")
                self.manifest_path = document.get("manifest_path")
                self.project_metadata = copy.deepcopy(document.get("project_metadata", {}))
                self.format_config = copy.deepcopy(document.get("format_config", {}))
                self.fragments = {model.fragment_id: model for model in models}
                self.cache = PipelineCache.from_dict(document.get("cache"))
                self.last_run = copy.deepcopy(document.get("last_run"))
                self.last_legacy_upgrade = copy.deepcopy(document.get("last_legacy_upgrade"))
            return True
        except WorkbenchError:
            raise
        except Exception as exc:
            raise WorkbenchError(E_VALIDATION, f"Unable to load persisted session: {exc}") from exc

