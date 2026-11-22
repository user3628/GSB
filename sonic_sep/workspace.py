"""Workspace import, configuration snapshots, overrides, and stop signaling."""

from __future__ import annotations

import copy
import re
import shutil
from pathlib import Path
from typing import Any, Mapping

from .config import SeparationConfig, load_config
from .errors import invalid_config, manifest_error, not_found
from .manifest import AudioManifest, effective_manifest, load_manifest
from .util import atomic_write_bytes, atomic_write_json, canonical_json_bytes, digest_json, read_json


SAFE_SLICE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class Workspace:
    def __init__(self, project_root: str | Path, *, state_root: str | Path | None = None):
        self.project_root = Path(project_root).resolve()
        self.state_root = Path(state_root).resolve() if state_root else self.project_root / "state"
        self.samples_dir = self.project_root / "samples"
        self.configs_dir = self.project_root / "configs"
        self.state_root.mkdir(parents=True, exist_ok=True)
        self.meta_path = self.state_root / "workspace.json"
        self.stop_path = self.state_root / "STOP"
        self.slices_root = self.state_root / "slices"
        self.slices_root.mkdir(parents=True, exist_ok=True)

    def import_dataset(
        self,
        manifest_path: str | Path | None = None,
        config_path: str | Path | None = None,
        *,
        preserve_overrides: bool = False,
    ) -> dict[str, Any]:
        manifest_file = Path(manifest_path).resolve() if manifest_path else (self.samples_dir / "manifest.json").resolve()
        config_file = Path(config_path).resolve() if config_path else (self.configs_dir / "default.yaml").resolve()
        manifest = load_manifest(manifest_file)
        try:
            config = load_config(config_file)
        except Exception as exc:
            if hasattr(exc, "details") and "slice_id" not in exc.details and manifest.slices:
                exc.details["slice_id"] = manifest.slices[0].slice_id
            raise
        if config.sample_rate != manifest.sample_rate:
            raise invalid_config(
                "configuration sample_rate does not match manifest sample_rate",
                param="sample_rate",
                manifest_sample_rate=manifest.sample_rate,
                config_sample_rate=config.sample_rate,
            )
        for item in manifest.slices:
            if item.source_count > config.max_sources:
                raise invalid_config(
                    "configured max_sources is smaller than a slice source count",
                    slice_id=item.slice_id,
                    param="max_sources",
                    source_count=item.source_count,
                    max_sources=config.max_sources,
                )
            if item.sample_rate != config.sample_rate:
                raise invalid_config(
                    "configuration sample_rate does not match a slice",
                    slice_id=item.slice_id,
                    param="sample_rate",
                )
        previous: dict[str, Any] | None = None
        if self.meta_path.exists():
            try:
                previous = read_json(self.meta_path)
            except (OSError, ValueError):
                previous = None
        overrides = previous.get("source_overrides", {}) if previous and preserve_overrides else {}
        meta = {
            "schema_version": 1,
            "project_root": str(self.project_root),
            "manifest_path": str(manifest_file),
            "config_path": str(config_file),
            "manifest_digest": manifest.digest(),
            "config_digest": digest_json(config.to_dict()),
            "config_snapshot": config.to_dict(),
            "source_overrides": overrides,
        }
        atomic_write_json(self.meta_path, meta)
        imported = self.state_root / "imported"
        imported.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(config_file, imported / "config.yaml")
        return {
            "ok": True,
            "manifest_path": str(manifest_file),
            "config_path": str(config_file),
            "slice_count": len(manifest.slices),
            "total_samples": manifest.total_samples,
            "config": config.to_dict(),
            "manifest_digest": meta["manifest_digest"],
            "config_digest": meta["config_digest"],
        }

    def _load_meta(self, *, required: bool = True) -> dict[str, Any]:
        if not self.meta_path.exists():
            if required:
                raise not_found("workspace has not been imported; run import first")
            return {}
        try:
            return read_json(self.meta_path)
        except (OSError, ValueError) as exc:
            raise manifest_error(f"workspace metadata is unreadable: {exc}") from exc

    def meta(self) -> dict[str, Any]:
        return self._load_meta(required=True)

    def config(self) -> SeparationConfig:
        meta = self._load_meta(required=False)
        if not meta:
            default = self.configs_dir / "default.yaml"
            if default.exists():
                return load_config(default)
            raise not_found("workspace has not been imported; run import first")
        config_path = Path(meta.get("config_path", ""))
        if config_path.is_file():
            return load_config(config_path)
        snapshot = meta.get("config_snapshot")
        if isinstance(snapshot, Mapping):
            from .config import validate_config_data

            return validate_config_data(snapshot)
        raise invalid_config("configuration is unavailable", param="config")

    def manifest(self) -> AudioManifest:
        meta = self._load_meta(required=False)
        if not meta:
            default = self.samples_dir / "manifest.json"
            if default.exists():
                return load_manifest(default)
            raise not_found("workspace has not been imported; run import first")
        manifest_path = Path(meta.get("manifest_path", ""))
        if not manifest_path.is_file():
            raise manifest_error("imported manifest no longer exists", file=str(manifest_path))
        return load_manifest(manifest_path)

    def raw_overrides(self) -> dict[str, dict[int, dict[str, float]]]:
        meta = self._load_meta(required=False)
        raw = meta.get("source_overrides", {}) if meta else {}
        result: dict[str, dict[int, dict[str, float]]] = {}
        for slice_id, source_map in raw.items():
            result[str(slice_id)] = {int(source_id): dict(values) for source_id, values in source_map.items()}
        return result

    def effective_manifest(self, overrides: Mapping[str, Mapping[int, Mapping[str, float]]] | None = None) -> AudioManifest:
        selected = self.raw_overrides() if overrides is None else overrides
        return effective_manifest(self.manifest(), selected)

    def save_overrides(self, overrides: Mapping[str, Mapping[int, Mapping[str, float]]]) -> None:
        meta = copy.deepcopy(self.meta())
        normalized = {
            str(slice_id): {
                str(int(source_id)): {str(key): float(value) for key, value in values.items()}
                for source_id, values in source_map.items()
            }
            for slice_id, source_map in overrides.items()
        }
        meta["source_overrides"] = normalized
        meta["overrides_digest"] = digest_json(normalized)
        atomic_write_json(self.meta_path, meta)

    def request_stop(self, reason: str = "api") -> None:
        atomic_write_bytes(self.stop_path, canonical_json_bytes({"reason": reason}) + b"\n")

    def clear_stop(self) -> None:
        try:
            self.stop_path.unlink()
        except FileNotFoundError:
            pass

    def stop_requested(self) -> bool:
        return self.stop_path.exists()

    def slice_dir(self, slice_id: str) -> Path:
        if not SAFE_SLICE_ID.match(slice_id):
            raise invalid_config("slice_id contains unsafe characters", slice_id=slice_id, param="slice_id")
        return self.slices_root / slice_id

    def cache_dir(self, slice_id: str) -> Path:
        path = self.slice_dir(slice_id) / "cache"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def recon_dir(self, slice_id: str) -> Path:
        path = self.slice_dir(slice_id) / "reconstruction"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def output_dir(self, slice_id: str) -> Path:
        path = self.slice_dir(slice_id) / "outputs"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def reset_slice(self, slice_id: str) -> None:
        path = self.slice_dir(slice_id)
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)

    def reset_state(self, *, remove_checkpoints: bool = True) -> None:
        for child in list(self.state_root.iterdir()):
            if child.name == ".gitkeep":
                continue
            if child.name in {"workspace.json"} and not remove_checkpoints:
                continue
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()

    def summary(self) -> dict[str, Any]:
        meta = self._load_meta(required=False)
        if not meta:
            return {"imported": False, "state_root": str(self.state_root), "stop_requested": self.stop_requested()}
        manifest = self.manifest()
        config = self.config()
        return {
            "imported": True,
            "state_root": str(self.state_root),
            "manifest_path": meta.get("manifest_path"),
            "config_path": meta.get("config_path"),
            "manifest_digest": manifest.digest(),
            "config_digest": digest_json(config.to_dict()),
            "slice_count": len(manifest.slices),
            "total_samples": manifest.total_samples,
            "sample_rate": manifest.sample_rate,
            "slice_ids": manifest.slice_ids(),
            "source_overrides": self.raw_overrides(),
            "stop_requested": self.stop_requested(),
        }

