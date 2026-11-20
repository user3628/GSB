"""Workspace lifecycle and single-shard atomic update entry points."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import yaml

from .checkpoints import CheckpointStore
from .config import WorkbenchConfig, load_config, parse_config
from .errors import BusinessError, ErrorCode
from .pipeline import BuildHooks, BuildReport, build_workspace, load_all_references
from .sampling import load_reference_tensor
from .util import atomic_write_bytes, exclusive_file_lock, sha256_bytes


UPDATABLE_FIELDS = {
    "prompt",
    "guidance_scale",
    "steps",
    "seed",
    "blend_strength",
    "reference",
    "source_image",
}


def write_config(path: Path, raw: dict[str, Any]) -> None:
    text = yaml.safe_dump(raw, allow_unicode=True, sort_keys=True, default_flow_style=False)
    atomic_write_bytes(path, text.encode("utf-8"))


def update_shard(
    root: Path,
    shard_id: str,
    changes: dict[str, Any] | None = None,
    *,
    reference_file: Path | None = None,
    build: bool = True,
    hooks: BuildHooks | None = None,
) -> dict[str, Any]:
    """Atomically change one shard's configuration or reference bytes.

    The new configuration is fully validated, including reference dimensions,
    before the visible YAML is replaced. Reference bytes are content addressed so
    a failed update never destroys the previous valid reference.
    """

    root = root.resolve()
    store = CheckpointStore(root)
    changes = dict(changes or {})
    unknown = sorted(set(changes) - UPDATABLE_FIELDS)
    if unknown:
        raise BusinessError(
            ErrorCode.E_INVALID_CONFIG,
            f"unsupported shard fields: {', '.join(unknown)}",
            shard=shard_id,
            field=unknown[0],
        )
    if reference_file is None and not changes:
        raise BusinessError(
            ErrorCode.E_INVALID_CONFIG,
            "no shard update was supplied",
            shard=shard_id,
            field="update",
        )

    with exclusive_file_lock(store.workspace_lock_path()):
        config = load_config(root / "config.yaml")
        shard = config.shard(shard_id)
        raw = copy.deepcopy(config.raw)
        index = next(i for i, item in enumerate(raw["shards"]) if item["id"] == shard_id)
        shard_raw = dict(raw["shards"][index])
        shard_raw.update(changes)

        if reference_file is not None:
            source = reference_file.resolve()
            # Validate all numeric/config fields before creating a content file.
            shard_raw["reference"] = str(source)
            raw["shards"][index] = shard_raw
            parse_config(raw, root=root)
            shape = (
                config.global_config.image.height,
                config.global_config.image.width,
                config.global_config.image.channels,
            )
            _, digest = load_reference_tensor(source, shape, shard_id=shard_id)
            source_bytes = source.read_bytes()
            suffix = source.suffix.lower() if source.suffix else ".png"
            relative = Path("references") / f"{shard_id}-{digest[:16]}{suffix}"
            destination = root / relative
            atomic_write_bytes(destination, source_bytes)
            shard_raw["reference"] = relative.as_posix()

        raw["shards"][index] = shard_raw
        candidate = parse_config(raw, root=root)
        load_all_references(candidate)
        write_config(root / "config.yaml", candidate.raw)

    if not build:
        return {
            "ok": True,
            "updated": shard_id,
            "built": False,
            "config_version": candidate.config_version,
        }

    try:
        report = build_workspace(root, hooks=hooks)
    except BusinessError:
        raise
    return {
        "ok": True,
        "updated": shard_id,
        "built": True,
        "config_version": report.config_version,
        "build": report.to_dict(),
    }


def load_workspace(root: Path) -> WorkbenchConfig:
    return load_config(root.resolve() / "config.yaml")

