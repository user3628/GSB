"""Local manifest import/validation (no remote resources)."""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Dict, Optional

from .canonical import atomic_write_json, digest_file, dump_bytes, sha256_bytes
from .errors import E_INVALID_INPUT, PipelineError
from .pipeline import Pipeline


def import_inputs(
    root: str | Path,
    manifest_path: str | Path,
    *,
    work_dir: Optional[str | Path] = None,
    output_dir: Optional[str | Path] = None,
    copy_to: Optional[str | Path] = None,
) -> Dict[str, Any]:
    root_path = Path(root).resolve()
    source = Path(manifest_path).resolve()
    if str(manifest_path).startswith(("http://", "https://")):
        raise PipelineError(E_INVALID_INPUT, "remote manifests are not accepted")
    if copy_to is not None:
        destination = Path(copy_to).resolve()
        source_dir = source.parent
        destination.mkdir(parents=True, exist_ok=True)
        target_manifest = destination / source.name
        if source != target_manifest:
            shutil.copy2(source, target_manifest)
        images_source = source_dir / "images"
        images_target = destination / "images"
        if images_source.is_dir():
            images_target.mkdir(parents=True, exist_ok=True)
            for image in sorted(images_source.glob("*.png")):
                target = images_target / image.name
                if image.resolve() != target:
                    shutil.copy2(image, target)
        source = target_manifest
    work = Path(work_dir).resolve() if work_dir is not None else root_path / "work"
    output = Path(output_dir).resolve() if output_dir is not None else root_path / "output"
    pipeline = Pipeline(root_path, source, work_dir=work, output_dir=output)
    record = {
        "schema_version": 1,
        "manifest": str(source),
        "manifest_sha256": digest_file(source),
        "work_dir": str(work),
        "output_dir": str(output),
        "shard_count": len(pipeline.shards),
        "accepted_shard_ids": pipeline.accepted_ids,
        "unprocessed": pipeline.schedule["unprocessed"],
        "images": {
            str(shard["id"]): {
                "path": str(shard["_image_path"]),
                "sha256": pipeline.image_sha256[str(shard["id"])],
            }
            for shard in pipeline.shards
        },
        "import_digest": sha256_bytes(
            dump_bytes(
                {
                    "manifest_sha256": digest_file(source),
                    "images": pipeline.image_sha256,
                }
            )
        ),
    }
    work.mkdir(parents=True, exist_ok=True)
    atomic_write_json(work / "import.json", record)
    return record
