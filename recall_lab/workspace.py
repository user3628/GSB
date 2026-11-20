from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .config import load_config
from .data import Manifest, load_manifest
from .errors import BusinessError, ErrorCode
from .util import atomic_write_bytes, canonical_json_bytes, digest_bytes, safe_relative_path


WORKSPACE_META_FORMAT = 1


def workspace_id_for(root: Path, manifest_digest: str) -> str:
    material = f"recall-lab-workspace-v1\x1f{root.resolve()}\x1f{manifest_digest}".encode("utf-8")
    return digest_bytes(material)


def read_meta(root: Path) -> dict[str, Any]:
    path = root / "meta.json"
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise BusinessError(
            ErrorCode.INVALID_INPUT,
            "workspace is not initialized",
            {"path": str(path), "hint": "run the init command first"},
        ) from exc
    except json.JSONDecodeError as exc:
        raise BusinessError(
            ErrorCode.STALE_CHECKPOINT,
            "workspace metadata is invalid",
            {"path": str(path), "reason": str(exc)},
        ) from exc
    if raw.get("format_version") != WORKSPACE_META_FORMAT or not isinstance(raw.get("workspace_id"), str):
        raise BusinessError(
            ErrorCode.STALE_CHECKPOINT,
            "workspace metadata format is unsupported",
            {"path": str(path)},
        )
    return raw


def init_workspace(
    root: str | Path,
    manifest_path: str | Path,
    config_path: str | Path,
    *,
    force: bool = False,
) -> dict[str, Any]:
    root_path = Path(root).resolve()
    source_manifest = Path(manifest_path).resolve()
    source_config = Path(config_path).resolve()
    source_manifest_bytes = source_manifest.read_bytes()
    source_config_bytes = source_config.read_bytes()
    manifest = load_manifest(source_manifest)
    # Parse once at import time so an invalid YAML never enters a workspace.
    load_config(source_config)

    input_dir = root_path / "input"
    config_target = root_path / "config" / "fusion.yaml"
    manifest_target = input_dir / "manifest.jsonl"
    if manifest_target.exists() and not force and (root_path / "meta.json").exists():
        # Re-import is allowed, but the flag makes destructive intent explicit.
        raise BusinessError(
            ErrorCode.INVALID_INPUT,
            "workspace inputs already exist; pass --force to replace them",
            {"workspace": str(root_path), "reason": "INPUT_EXISTS"},
        )

    for entry in manifest.entries:
        for relative in (entry.items_file, entry.queries_file):
            safe_rel = safe_relative_path(relative)
            source = (source_manifest.parent / safe_rel).resolve()
            try:
                payload = source.read_bytes()
            except FileNotFoundError as exc:
                raise BusinessError(
                    ErrorCode.INVALID_INPUT,
                    "manifest data file is missing",
                    {"scale": entry.scale, "path": str(source)},
                ) from exc
            atomic_write_bytes(input_dir / safe_rel, payload)

    atomic_write_bytes(manifest_target, source_manifest_bytes)
    atomic_write_bytes(config_target, source_config_bytes)

    meta_path = root_path / "meta.json"
    if meta_path.exists():
        meta = read_meta(root_path)
    else:
        workspace_id = workspace_id_for(root_path, digest_bytes(source_manifest_bytes))
        meta = {
            "format_version": WORKSPACE_META_FORMAT,
            "workspace_id": workspace_id,
            "initial_manifest_digest": digest_bytes(source_manifest_bytes),
        }
        atomic_write_bytes(meta_path, canonical_json_bytes(meta))
    (root_path / "control").mkdir(parents=True, exist_ok=True)
    (root_path / "exports").mkdir(parents=True, exist_ok=True)
    return meta


def load_workspace_manifest(root: Path) -> Manifest:
    return load_manifest(root / "input" / "manifest.jsonl")
