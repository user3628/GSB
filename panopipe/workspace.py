"""Workspace layout, manifest import, runtime configuration and control files."""

from __future__ import annotations

import io
import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import numpy as np
from PIL import Image

from .checkpoints import CheckpointStore
from .codecs import CodecOptions
from .errors import E_INVALID_CONFIG, E_NOT_FOUND, PipelineError
from .fingerprint import digest_json
from .fsutil import atomic_write_bytes, atomic_write_json, fsync_dir
from .manifest import (
    Manifest,
    ParseOptions,
    ShardSpec,
    load_manifest,
    parse_manifest,
)


DEFAULT_CONFIG: dict[str, Any] = {
    "palette_max_colors": 256,
    "segment_id_bits": 24,
    "png_compress_level": 6,
    "export_formats": ["png_palette", "rle", "coco_json"],
    "validate_formats": ["png_palette", "rle", "coco_json"],
    "default_instance_start_id": 100,
    "min_instance_area": 4,
    "confidence_threshold": 0.5,
    "mask_format": "png_palette",
}


@dataclass(frozen=True)
class RuntimeConfig:
    palette_max_colors: int = 256
    segment_id_bits: int = 24
    png_compress_level: int = 6
    export_formats: tuple[str, ...] = ("png_palette", "rle", "coco_json")
    validate_formats: tuple[str, ...] = ("png_palette", "rle", "coco_json")

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> "RuntimeConfig":
        merged = dict(DEFAULT_CONFIG)
        merged.update(value)
        export_formats = tuple(str(item) for item in merged["export_formats"])
        validate_formats = tuple(str(item) for item in merged["validate_formats"])
        allowed = {"png_palette", "rle", "coco_json"}
        if not export_formats or not set(export_formats) <= allowed or len(set(export_formats)) != len(export_formats):
            raise PipelineError(E_INVALID_CONFIG, "Invalid export_formats configuration")
        if not validate_formats or not set(validate_formats) <= allowed or len(set(validate_formats)) != len(validate_formats):
            raise PipelineError(E_INVALID_CONFIG, "Invalid validate_formats configuration")
        config = cls(
            palette_max_colors=int(merged["palette_max_colors"]),
            segment_id_bits=int(merged["segment_id_bits"]),
            png_compress_level=int(merged["png_compress_level"]),
            export_formats=export_formats,
            validate_formats=validate_formats,
        )
        config.codec_options().validate()
        return config

    def codec_options(self) -> CodecOptions:
        return CodecOptions(
            palette_max_colors=self.palette_max_colors,
            segment_id_bits=self.segment_id_bits,
            png_compress_level=self.png_compress_level,
        )

    def fingerprint(self) -> str:
        return digest_json(
            {
                "palette_max_colors": self.palette_max_colors,
                "segment_id_bits": self.segment_id_bits,
                "png_compress_level": self.png_compress_level,
                "export_formats": list(self.export_formats),
                "validate_formats": list(self.validate_formats),
            }
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "palette_max_colors": self.palette_max_colors,
            "segment_id_bits": self.segment_id_bits,
            "png_compress_level": self.png_compress_level,
            "export_formats": list(self.export_formats),
            "validate_formats": list(self.validate_formats),
        }


class Workspace:
    def __init__(self, root: Path):
        self.root = root.resolve()
        self.manifest_path = self.root / "manifest.json"
        self.config_path = self.root / "config.json"
        self.import_meta_path = self.root / "state" / "import.json"
        self.invalidations_path = self.root / "state" / "invalidations.json"
        self.stop_path = self.root / "control" / "stop.json"
        self.diagnostics_dir = self.root / "diagnostics"
        self.checkpoints = CheckpointStore(self.root)
        self.build_lock_path = self.root / "locks" / "build.lock"

    @classmethod
    def create(cls, root: Path, *, config: Mapping[str, Any] | None = None) -> "Workspace":
        workspace = cls(root)
        for path in (
            workspace.root,
            workspace.root / "inputs",
            workspace.root / "shards",
            workspace.root / "checkpoints",
            workspace.root / "locks",
            workspace.root / "state",
            workspace.root / "control",
            workspace.diagnostics_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
        if not workspace.config_path.exists():
            workspace.write_config(RuntimeConfig.from_dict(config or {}))
        elif config is not None:
            workspace.write_config(RuntimeConfig.from_dict(config))
        return workspace

    def exists(self) -> bool:
        return self.manifest_path.is_file()

    def require_imported(self) -> None:
        if not self.root.exists():
            raise PipelineError(E_NOT_FOUND, "Workspace does not exist", {"path": str(self.root)})
        if not self.manifest_path.is_file():
            raise PipelineError(
                E_NOT_FOUND,
                "Workspace has not imported a manifest",
                {"path": str(self.root)},
            )

    def write_config(self, config: RuntimeConfig) -> None:
        value = config.to_dict()
        value.update(
            {
                "default_instance_start_id": DEFAULT_CONFIG["default_instance_start_id"],
                "min_instance_area": DEFAULT_CONFIG["min_instance_area"],
                "confidence_threshold": DEFAULT_CONFIG["confidence_threshold"],
                "mask_format": DEFAULT_CONFIG["mask_format"],
            }
        )
        atomic_write_json(self.config_path, value, pretty=True)

    def load_config(self) -> RuntimeConfig:
        if not self.config_path.is_file():
            return RuntimeConfig.from_dict({})
        try:
            value = json.loads(self.config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise PipelineError(E_INVALID_CONFIG, "Workspace config is invalid", {"reason": str(exc)}) from exc
        if not isinstance(value, Mapping):
            raise PipelineError(E_INVALID_CONFIG, "Workspace config must be an object")
        return RuntimeConfig.from_dict(value)

    def import_manifest(
        self,
        source_path: Path,
        *,
        parse_options: ParseOptions | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> Manifest:
        source_path = source_path.resolve()
        manifest = load_manifest(source_path, options=parse_options)
        prepared: list[tuple[ShardSpec, bytes, int, int]] = []
        for shard in manifest.shards:
            if not shard.image_path.is_file():
                raise PipelineError(
                    E_NOT_FOUND,
                    "Shard image not found",
                    {"shard_id": shard.shard_id, "path": str(shard.image_path)},
                )
            source_bytes = shard.image_path.read_bytes()
            try:
                with Image.open(io.BytesIO(source_bytes)) as image:
                    if image.format != "PNG":
                        raise PipelineError(
                            E_INVALID_CONFIG,
                            "Imported shard images must be PNG",
                            {"shard_id": shard.shard_id, "format": image.format},
                        )
                    width, height = image.size
            except PipelineError:
                raise
            except Exception as exc:
                raise PipelineError(
                    E_INVALID_CONFIG,
                    "Imported shard image is not decodable",
                    {"shard_id": shard.shard_id, "reason": str(exc)},
                ) from exc
            if shard.width is not None and shard.width != width:
                raise PipelineError(E_INVALID_CONFIG, "Manifest width does not match imported image")
            if shard.height is not None and shard.height != height:
                raise PipelineError(E_INVALID_CONFIG, "Manifest height does not match imported image")
            prepared.append((shard, source_bytes, width, height))

        workspace = Workspace.create(self.root, config=config)
        imported_shards: list[ShardSpec] = []
        for shard, source_bytes, width, height in prepared:
            image_ref = f"inputs/{shard.shard_id}.png"
            target = workspace.root / image_ref
            atomic_write_bytes(target, source_bytes)
            imported_shards.append(
                ShardSpec(
                    shard_id=shard.shard_id,
                    image_path=target,
                    image_ref=image_ref,
                    categories=shard.categories,
                    instance_start_id=shard.instance_start_id,
                    min_instance_area=shard.min_instance_area,
                    confidence_threshold=shard.confidence_threshold,
                    mask_format=shard.mask_format,
                    source_encoding=shard.source_encoding,
                    width=width,
                    height=height,
                )
            )
        imported_manifest = Manifest(
            project=manifest.project,
            schema_version=manifest.schema_version,
            shards=tuple(imported_shards),
            source_path=workspace.manifest_path,
        )
        value = imported_manifest.to_dict(version=2, for_workspace=True)
        atomic_write_json(workspace.manifest_path, value, pretty=True)
        atomic_write_json(
            workspace.import_meta_path,
            {
                "source_schema_version": int(manifest.schema_version),
                "source_file": source_path.name,
                "project": manifest.project,
                "overall_manifest_fingerprint": imported_manifest.overall_fingerprint(),
            },
            pretty=True,
        )
        return imported_manifest

    def load_manifest(self) -> Manifest:
        self.require_imported()
        try:
            value = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise PipelineError(E_INVALID_CONFIG, "Workspace manifest is invalid", {"reason": str(exc)}) from exc
        if not isinstance(value, Mapping):
            raise PipelineError(E_INVALID_CONFIG, "Workspace manifest must be an object")
        manifest = parse_manifest(value, base_dir=self.root, source_path=self.manifest_path)
        origin = value.get("import_metadata", {}).get("schema_version") if isinstance(value.get("import_metadata"), Mapping) else None
        if origin is None and self.import_meta_path.is_file():
            try:
                origin = json.loads(self.import_meta_path.read_text(encoding="utf-8")).get("source_schema_version")
            except (OSError, json.JSONDecodeError):
                origin = None
        if origin is not None:
            try:
                manifest.schema_version = int(origin)
            except (TypeError, ValueError):
                pass
        return manifest

    def shard_dir(self, shard_id: str, stage: str | None = None) -> Path:
        path = self.root / "shards" / shard_id
        if stage is not None:
            path = path / stage
        return path

    def artifact_paths(self, shard_id: str, stage: str) -> list[str]:
        directory = self.shard_dir(shard_id, stage)
        if not directory.exists():
            return []
        result: list[str] = []
        for path in sorted(directory.rglob("*")):
            if path.is_file() and not path.name.startswith("."):
                result.append(path.relative_to(self.root).as_posix())
        return result

    def remove_stage_artifacts(self, shard_id: str, stages: list[str] | tuple[str, ...]) -> None:
        for stage in stages:
            directory = self.shard_dir(shard_id, stage)
            if directory.exists():
                shutil.rmtree(directory)

    def write_diagnostic(self, shard_id: str, error: PipelineError) -> str:
        path = self.diagnostics_dir / f"{shard_id}.{error.code}.json"
        atomic_write_json(path, error.to_dict(), pretty=True)
        return path.relative_to(self.root).as_posix()

    def request_stop(self, *, reason: str = "requested") -> None:
        atomic_write_json(self.stop_path, {"stop": True, "reason": reason}, pretty=True)

    def clear_stop(self) -> None:
        try:
            self.stop_path.unlink()
        except FileNotFoundError:
            return

    def stop_requested(self) -> bool:
        return self.stop_path.is_file()

    def save_npy(self, path: Path, array: np.ndarray) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        tmp = Path(tmp_name)
        try:
            with os.fdopen(fd, "wb") as fh:
                np.save(fh, array, allow_pickle=False)
                fh.flush()
                os.fsync(fh.fileno())
            tmp.replace(path)
            fsync_dir(path.parent)
        finally:
            try:
                tmp.unlink()
            except FileNotFoundError:
                pass

    def load_npy(self, path: Path) -> np.ndarray:
        try:
            with path.open("rb") as fh:
                return np.load(fh, allow_pickle=False)
        except FileNotFoundError as exc:
            raise PipelineError(E_NOT_FOUND, "Artifact file is missing", {"path": str(path)}) from exc

    def write_invalidations(self, value: Mapping[str, Any]) -> None:
        atomic_write_json(self.invalidations_path, value, pretty=True)

    def load_invalidations(self) -> dict[str, Any]:
        if not self.invalidations_path.is_file():
            return {"shards": {}}
        try:
            value = json.loads(self.invalidations_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"shards": {}}
        return value if isinstance(value, dict) else {"shards": {}}
