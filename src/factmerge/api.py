"""Convenience library functions for embedding FactMerge."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

from .config import load_config
from .errors import FactMergeError
from .evidence import load_manifest, rebuild_manifest, validate_manifest
from .migration import migrate_legacy_v1_file
from .pipeline import FactMergeEngine
from .state import StateStore


def verify_entity(
    config: dict[str, Any] | str | Path | None,
    corpus: str | Path | Iterable[dict[str, Any]],
    entity_id: str,
    *,
    state_dir: str | Path | None = None,
    expected: str | Path | dict[str, Any] | None = None,
) -> dict[str, Any]:
    engine = FactMergeEngine(config, corpus, state_dir=state_dir, expected=expected)
    return engine.verify_entity(entity_id)


def run_batch(
    config: dict[str, Any] | str | Path | None,
    corpus: str | Path | Iterable[dict[str, Any]],
    *,
    state_dir: str | Path | None = None,
    expected: str | Path | dict[str, Any] | None = None,
    stop_after_entities: int | None = None,
) -> dict[str, Any]:
    engine = FactMergeEngine(config, corpus, state_dir=state_dir, expected=expected)
    return engine.run(stop_after_entities=stop_after_entities)


def resume_batch(
    config: dict[str, Any] | str | Path | None,
    corpus: str | Path | Iterable[dict[str, Any]],
    *,
    state_dir: str | Path,
    expected: str | Path | dict[str, Any] | None = None,
) -> dict[str, Any]:
    engine = FactMergeEngine(config, corpus, state_dir=state_dir, expected=expected)
    return engine.run(resume=True)


def query_claim(manifest_path: str | Path, claim_id: str) -> dict[str, Any]:
    manifest = load_manifest(manifest_path)
    for entity in manifest.get("entities", []):
        for claim in entity.get("claims", []):
            if claim.get("claim_id") == claim_id:
                return claim
    raise FactMergeError("E_NOT_FOUND", f"claim not found: {claim_id}", claim_id=claim_id)


def demotion_reasons(manifest_path: str | Path, entity_id: str) -> list[dict[str, Any]]:
    manifest = load_manifest(manifest_path)
    for entity in manifest.get("entities", []):
        if entity.get("entity_id") == entity_id:
            return list(entity.get("decision", {}).get("demoted", []))
    raise FactMergeError("E_NOT_FOUND", f"entity not found: {entity_id}", entity_id=entity_id)


def version_status(
    config: dict[str, Any] | str | Path | None,
    corpus: str | Path | Iterable[dict[str, Any]],
    *,
    state_dir: str | Path,
) -> dict[str, Any]:
    engine = FactMergeEngine(config, corpus, state_dir=state_dir)
    return engine.version_status()


def request_stop(state_dir: str | Path) -> None:
    StateStore(state_dir).request_stop()


def validate_evidence(manifest_path: str | Path) -> dict[str, Any]:
    return validate_manifest(load_manifest(manifest_path))


def rebuild_evidence(manifest_path: str | Path) -> dict[str, Any]:
    return rebuild_manifest(load_manifest(manifest_path))


def migrate_v1(
    path: str | Path,
    config: dict[str, Any] | str | Path | None = None,
) -> dict[str, Any]:
    return migrate_legacy_v1_file(path, load_config(config))
