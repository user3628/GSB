from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .config import WorkbenchConfig
from .origin import content_identity
from .policy import CachePolicy
from .store import Store


def _resolve_sample_path(config: WorkbenchConfig, value: str) -> Path:
    candidate = Path(value).expanduser()
    if candidate.is_absolute():
        return candidate
    return (config.samples_dir / candidate).resolve()


def load_origin_manifest(store: Store, config: WorkbenchConfig, manifest_path: str | Path) -> list[dict[str, Any]]:
    path = Path(manifest_path).expanduser()
    if not path.is_absolute():
        path = (config.samples_dir / path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Origin content manifest not found: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    items = raw.get("items") if isinstance(raw, dict) else None
    if not isinstance(items, list):
        raise ValueError("Origin content manifest must contain an 'items' array")
    results: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("Origin manifest items must be objects")
        item_path = str(item.get("path", ""))
        if not item_path.startswith("/"):
            raise ValueError("Origin manifest paths must start with '/'")
        if "body" in item:
            body = str(item["body"]).encode("utf-8")
        else:
            body_file = _resolve_sample_path(config, str(item.get("file", "")))
            body = body_file.read_bytes()
        etag, version = content_identity(body)
        results.append(
            store.upsert_origin_content(
                path=item_path,
                body=body,
                content_type=str(item.get("content_type", "application/octet-stream")),
                tags=[str(tag) for tag in item.get("tags", [])],
                etag=etag,
                version=version,
            )
        )
    return results


def seed_origin_if_empty(store: Store, config: WorkbenchConfig) -> list[dict[str, Any]]:
    if store.list_origin_content():
        return []
    if config.seed_content_manifest is None:
        return []
    manifest = config.seed_content_manifest
    if not manifest.exists():
        return []
    return load_origin_manifest(store, config, manifest)


def seed_policy_if_empty(store: Store, config: WorkbenchConfig) -> dict[str, Any] | None:
    if config.seed_policy_file is None or store.get_current_policy() is not None:
        return None
    if not config.seed_policy_file.exists():
        return None
    raw = json.loads(config.seed_policy_file.read_text(encoding="utf-8"))
    policy = CachePolicy.from_snapshot(raw, config.edge_nodes)
    return store.submit_policy(policy)

