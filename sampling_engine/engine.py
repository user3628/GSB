"""Workspace orchestration, incremental invalidation, stop/resume, and queries."""

from __future__ import annotations

import json
import threading
import uuid
from pathlib import Path
from typing import Any, Iterable, Mapping

import numpy as np

from .canonical import digest_file, digest_json
from .checkpoints import RequestStore
from .config import EngineConfig
from .errors import (
    EngineError,
    E_CONFLICT,
    E_NOT_FOUND,
    invalid_manifest,
    invalid_stage,
    snapshot_conflict,
)
from .manifest import ManifestCodec
from .metrics import corpus_metrics
from .models import Manifest, STAGE_INDEX, STAGES, SamplingRequest
from .stages import (
    ALGORITHM_VERSIONS,
    load_decode_artifact,
    load_normalize_artifact,
    load_postprocess_artifact,
    load_summary_artifact,
    run_decode,
    run_normalize,
    run_postprocess,
    run_summarize,
)
from .storage import (
    FileLock,
    atomic_write_json,
    copy_atomically,
    export_snapshot,
    import_snapshot,
    read_json,
    staging_path,
)


FIELD_STAGE = {
    "request_id": "postprocess",
    "logits_content_sha256": "postprocess",
    "temperature": "postprocess",
    "repetition_penalty": "postprocess",
    "manifest_version": "postprocess",
    "vocab_digest": "postprocess",
    "prompt_tokens": "postprocess",
    "top_k": "normalize",
    "top_p": "normalize",
    "seed": "decode",
    "steps": "decode",
}


class SamplingEngine:
    """Local, deterministic sampling engine backed by a writable workspace."""

    def __init__(
        self,
        workspace: str | Path,
        config: EngineConfig | None = None,
    ) -> None:
        self.config = config or EngineConfig()
        self.workspace = Path(workspace).resolve()
        self.workspace.mkdir(parents=True, exist_ok=True)
        self.requests_dir = self.workspace / "requests"
        self.logits_dir = self.workspace / "logits"
        self.errors_dir = self.workspace / "errors"
        for folder in (self.requests_dir, self.logits_dir, self.errors_dir):
            folder.mkdir(parents=True, exist_ok=True)
        self.registry_path = self.workspace / "registry.json"
        self.meta_path = self.workspace / "workspace_meta.json"
        self.registry_lock_path = self.workspace / "registry.lock"
        self.meta_lock_path = self.workspace / "meta.lock"
        self.guard_path = (
            self.workspace.parent
            / f".{self.workspace.name}.sampling-engine-guard.json"
        )
        self._meta = self._ensure_workspace_meta()
        self.codec = ManifestCodec(self.config)
        self._run_lock = threading.Lock()
        self._stop_event = threading.Event()

    # ------------------------------------------------------------------
    # Workspace metadata and snapshots
    # ------------------------------------------------------------------
    def _read_json_or_none(self, path: Path) -> Any:
        if not path.exists():
            return None
        try:
            return read_json(path)
        except (OSError, json.JSONDecodeError) as exc:
            raise snapshot_conflict(
                "workspace metadata is unreadable",
                reason="digest_mismatch",
                path=str(path),
                detail=str(exc),
            )

    def _ensure_workspace_meta(self) -> dict[str, Any]:
        with FileLock(self.meta_lock_path, timeout=self.config.lock_timeout_seconds):
            meta = self._read_json_or_none(self.meta_path)
            if meta is None:
                meta = {
                    "meta_schema": 1,
                    "workspace_id": uuid.uuid4().hex,
                    "generation": 0,
                }
            if not isinstance(meta, dict) or not isinstance(
                meta.get("workspace_id"), str
            ):
                raise snapshot_conflict(
                    "workspace metadata is invalid",
                    reason="digest_mismatch",
                    path=str(self.meta_path),
                )
            guard = self._read_json_or_none(self.guard_path)
            if isinstance(guard, dict) and guard.get("workspace_id") == meta.get(
                "workspace_id"
            ):
                highest = int(guard.get("highest_generation", 0))
                if int(meta.get("generation", 0)) < highest:
                    raise snapshot_conflict(
                        "workspace generation is lower than the previously observed generation",
                        reason="version_rollback",
                        workspace_id=meta["workspace_id"],
                        current_generation=int(meta.get("generation", 0)),
                        highest_generation=highest,
                    )
            atomic_write_json(self.meta_path, meta)
            self._write_guard_locked(meta)
            return meta

    def _write_guard_locked(self, meta: Mapping[str, Any]) -> None:
        generation = int(meta.get("generation", 0))
        existing = self._read_json_or_none(self.guard_path)
        highest = generation
        if (
            isinstance(existing, dict)
            and existing.get("workspace_id") == meta.get("workspace_id")
        ):
            highest = max(highest, int(existing.get("highest_generation", 0)))
        atomic_write_json(
            self.guard_path,
            {
                "guard_schema": 1,
                "workspace_id": meta.get("workspace_id"),
                "highest_generation": highest,
            },
        )
        self._meta = dict(meta)

    def _bump_generation(self) -> int:
        with FileLock(self.meta_lock_path, timeout=self.config.lock_timeout_seconds):
            meta = self._read_json_or_none(self.meta_path)
            if not isinstance(meta, dict):
                raise snapshot_conflict(
                    "workspace metadata disappeared", reason="digest_mismatch"
                )
            meta["generation"] = int(meta.get("generation", 0)) + 1
            atomic_write_json(self.meta_path, meta)
            self._write_guard_locked(meta)
            return meta["generation"]

    def workspace_metadata(self) -> dict[str, Any]:
        meta = self._read_json_or_none(self.meta_path)
        if not isinstance(meta, dict):
            raise snapshot_conflict(
                "workspace metadata disappeared", reason="digest_mismatch"
            )
        return meta

    def export_workspace_snapshot(self, destination: str | Path) -> dict[str, Any]:
        return export_snapshot(self.workspace, destination)

    def validate_workspace_snapshot(
        self, snapshot_path: str | Path, *, allow_rollback: bool = False
    ) -> dict[str, Any]:
        return import_snapshot(
            snapshot_path, self.workspace, allow_rollback=allow_rollback
        )

    # ------------------------------------------------------------------
    # Registry and manifest imports
    # ------------------------------------------------------------------
    def _initial_registry(self) -> dict[str, Any]:
        return {
            "registry_schema": 1,
            "workspace_id": self._meta["workspace_id"],
            "manifests": {},
            "requests": {},
            "request_order": [],
        }

    def _load_registry_locked(self) -> dict[str, Any]:
        if not self.registry_path.exists():
            return self._initial_registry()
        try:
            registry = read_json(self.registry_path)
        except (OSError, json.JSONDecodeError) as exc:
            raise snapshot_conflict(
                "workspace registry is unreadable",
                reason="digest_mismatch",
                detail=str(exc),
            )
        if not isinstance(registry, dict):
            raise snapshot_conflict(
                "workspace registry is invalid", reason="digest_mismatch"
            )
        if registry.get("workspace_id") != self._meta["workspace_id"]:
            raise snapshot_conflict(
                "registry belongs to a different workspace snapshot",
                reason="digest_mismatch",
                expected=self._meta["workspace_id"],
                actual=registry.get("workspace_id"),
            )
        for field in ("manifests", "requests", "request_order"):
            if field not in registry:
                raise snapshot_conflict(
                    "workspace registry is missing a field",
                    reason="digest_mismatch",
                    field=field,
                )
        return registry

    def _write_registry_locked(self, registry: dict[str, Any]) -> None:
        atomic_write_json(self.registry_path, registry)

    def _validate_logits_reference(self, request: SamplingRequest) -> None:
        path = Path(request.resolved_logits_path())
        if not path.exists():
            raise invalid_manifest(
                "logits shard referenced by the manifest does not exist",
                request_id=request.request_id,
                field="logits_path",
                path=str(path),
            )
        if not path.is_file():
            raise invalid_manifest(
                "logits_path does not point to a file",
                request_id=request.request_id,
                field="logits_path",
                path=str(path),
            )

    def import_manifest_path(self, path: str | Path) -> dict[str, Any]:
        manifest = self.codec.decode_path(path)
        return self._import_manifest(manifest, manifest_id_source=str(Path(path).resolve()))

    def import_manifest_document(
        self,
        payload: Mapping[str, Any],
        *,
        base_dir: str | Path = ".",
        manifest_id_source: str | None = None,
    ) -> dict[str, Any]:
        manifest = self.codec.decode_document(payload, base_dir=base_dir)
        return self._import_manifest(
            manifest,
            manifest_id_source=manifest_id_source or manifest.manifest_digest,
        )

    def _import_manifest(
        self, manifest: Manifest, *, manifest_id_source: str
    ) -> dict[str, Any]:
        for request in manifest.requests:
            self._validate_logits_reference(request)
        manifest_id = "m_" + digest_json(
            {"source": manifest_id_source, "version": manifest.version}
        )[:24]
        manifest_record = {
            "manifest_id": manifest_id,
            "version": manifest.version,
            "vocab": list(manifest.vocab),
            "metadata": manifest.metadata,
            "conversions": manifest.conversions,
            "warnings": list(manifest.warnings),
            "source_path": manifest.source_path,
            "manifest_digest": manifest.manifest_digest,
            "request_ids": list(manifest.request_ids),
        }
        with FileLock(
            self.registry_lock_path, timeout=self.config.lock_timeout_seconds
        ):
            registry = self._load_registry_locked()
            existing_manifest = registry["manifests"].get(manifest_id)
            request_entries = {
                request.request_id: {
                    **request.registry_dict(),
                    "manifest_id": manifest_id,
                    "conversion": manifest.conversions[request.request_id],
                }
                for request in manifest.requests
            }
            same = (
                existing_manifest == manifest_record
                and all(
                    registry["requests"].get(request_id) == entry
                    for request_id, entry in request_entries.items()
                )
            )
            if same:
                return {
                    "ok": True,
                    "reused": True,
                    "manifest_id": manifest_id,
                    "manifest_version": manifest.version,
                    "request_ids": list(manifest.request_ids),
                    "conversions": manifest.conversions,
                    "warnings": list(manifest.warnings),
                    "manifest_digest": manifest.manifest_digest,
                }
            for request_id, entry in request_entries.items():
                registry["requests"][request_id] = entry
                if request_id not in registry["request_order"]:
                    registry["request_order"].append(request_id)
            old_ids = (
                existing_manifest.get("request_ids", [])
                if isinstance(existing_manifest, dict)
                else []
            )
            new_ids = set(request_entries)
            for request_id in old_ids:
                if request_id in new_ids:
                    continue
                other_reference = any(
                    request_id in group.get("request_ids", [])
                    for group_id, group in registry["manifests"].items()
                    if group_id != manifest_id
                )
                if not other_reference:
                    registry["requests"].pop(request_id, None)
                    if request_id in registry["request_order"]:
                        registry["request_order"].remove(request_id)
            registry["manifests"][manifest_id] = manifest_record
            self._write_registry_locked(registry)
        generation = self._bump_generation()
        return {
            "ok": True,
            "reused": False,
            "manifest_id": manifest_id,
            "manifest_version": manifest.version,
            "request_ids": list(manifest.request_ids),
            "conversions": manifest.conversions,
            "warnings": list(manifest.warnings),
            "manifest_digest": manifest.manifest_digest,
            "workspace_generation": generation,
        }

    def _registry_snapshot(self) -> dict[str, Any]:
        with FileLock(
            self.registry_lock_path, timeout=self.config.lock_timeout_seconds
        ):
            return self._load_registry_locked()

    def _get_request_entry(self, request_id: str) -> dict[str, Any]:
        registry = self._registry_snapshot()
        entry = registry["requests"].get(request_id)
        if entry is None:
            raise EngineError(
                E_NOT_FOUND, "request not found", {"request_id": request_id}
            )
        return entry

    def _request_from_entry(self, entry: Mapping[str, Any]) -> SamplingRequest:
        return SamplingRequest(
            request_id=str(entry["request_id"]),
            logits_path=str(entry["logits_path"]),
            temperature=float(entry["temperature"]),
            top_k=None if entry["top_k"] is None else int(entry["top_k"]),
            top_p=float(entry["top_p"]),
            repetition_penalty=float(entry["repetition_penalty"]),
            seed=int(entry["seed"]),
            steps=int(entry["steps"]),
            prompt_tokens=tuple(int(value) for value in entry["prompt_tokens"]),
            manifest_version=int(entry["manifest_version"]),
            vocab=tuple(str(value) for value in entry["vocab"]),
            base_dir=str(entry["base_dir"]),
        )

    def _all_request_entries(self) -> list[dict[str, Any]]:
        registry = self._registry_snapshot()
        entries: list[dict[str, Any]] = []
        for request_id in registry["request_order"]:
            entry = registry["requests"].get(request_id)
            if entry is not None:
                entries.append(entry)
        return entries

    def list_requests(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for entry in self._all_request_entries():
            status = self._status_from_entry(entry)
            results.append(
                {
                    "request_id": entry["request_id"],
                    "manifest_id": entry.get("manifest_id"),
                    "manifest_version": entry["manifest_version"],
                    "temperature": entry["temperature"],
                    "top_k": entry["top_k"],
                    "top_p": entry["top_p"],
                    "seed": entry["seed"],
                    "steps": entry["steps"],
                    "status": status["status"],
                    "conversion": entry.get("conversion"),
                }
            )
        return results

    def import_logits(self, path: str | Path) -> dict[str, Any]:
        source = Path(path).resolve()
        if not source.exists():
            raise EngineError(
                E_NOT_FOUND, "logits file not found", {"path": str(source)}
            )
        try:
            array = np.load(source, allow_pickle=False)
        except (OSError, ValueError) as exc:
            raise invalid_manifest(
                "logits file is not a valid .npy array", path=str(source), detail=str(exc)
            )
        if array.ndim != 2:
            raise invalid_manifest(
                "logits shard must be two-dimensional",
                path=str(source),
                shape=list(array.shape),
            )
        content_digest = digest_file(source)
        target = self.logits_dir / f"{content_digest[:24]}.npy"
        if not target.exists() or digest_file(target) != content_digest:
            copy_atomically(source, target)
        return {
            "ok": True,
            "content_digest": content_digest,
            "shape": list(array.shape),
            "stored_path": str(target),
            "workspace_relative_path": target.relative_to(self.workspace).as_posix(),
        }

    # ------------------------------------------------------------------
    # Fingerprints and invalidation
    # ------------------------------------------------------------------
    def _logits_content_digest(self, request: SamplingRequest) -> str:
        path = Path(request.resolved_logits_path())
        if not path.exists():
            raise EngineError(
                E_NOT_FOUND,
                "logits shard is missing",
                {"request_id": request.request_id, "path": str(path)},
            )
        return digest_file(path)

    def compute_input_fingerprints(
        self, request: SamplingRequest, logits_content_sha256: str | None = None
    ) -> dict[str, Any]:
        logits_digest = logits_content_sha256 or self._logits_content_digest(request)
        vocab_digest = digest_json(list(request.vocab))
        fields = {
            "request_id": request.request_id,
            "logits_content_sha256": logits_digest,
            "temperature": request.temperature,
            "top_k": request.top_k,
            "top_p": request.top_p,
            "repetition_penalty": request.repetition_penalty,
            "seed": request.seed,
            "steps": request.steps,
            "manifest_version": request.manifest_version,
            "vocab_digest": vocab_digest,
            "prompt_tokens": list(request.prompt_tokens),
        }
        postprocess = digest_json(
            {
                "stage": "postprocess",
                "algorithm_version": ALGORITHM_VERSIONS["postprocess"],
                "logits_content_sha256": logits_digest,
                "temperature": request.temperature,
                "repetition_penalty": request.repetition_penalty,
                "manifest_version": request.manifest_version,
                "vocab_digest": vocab_digest,
                "prompt_tokens": list(request.prompt_tokens),
            }
        )
        normalize = digest_json(
            {
                "stage": "normalize",
                "algorithm_version": ALGORITHM_VERSIONS["normalize"],
                "upstream": postprocess,
                "top_k": request.top_k,
                "top_p": request.top_p,
            }
        )
        decode = digest_json(
            {
                "stage": "decode",
                "algorithm_version": ALGORITHM_VERSIONS["decode"],
                "upstream": normalize,
                "seed": request.seed,
                "steps": request.steps,
            }
        )
        summarize = digest_json(
            {
                "stage": "summarize",
                "algorithm_version": ALGORITHM_VERSIONS["summarize"],
                "upstream": decode,
            }
        )
        stage_inputs = {
            "postprocess": postprocess,
            "normalize": normalize,
            "decode": decode,
            "summarize": summarize,
        }
        stage_fingerprints = {
            stage: {
                "input_fingerprint": stage_inputs[stage],
                "algorithm_version": ALGORITHM_VERSIONS[stage],
            }
            for stage in STAGES
        }
        config_version = digest_json(
            {
                "sampling": {
                    key: value
                    for key, value in fields.items()
                    if key not in {"logits_content_sha256"}
                },
                "algorithm_versions": ALGORITHM_VERSIONS,
            }
        )
        version_fingerprint = digest_json(
            {
                "request_id": request.request_id,
                "fields": fields,
                "stage_fingerprints": stage_fingerprints,
            }
        )
        return {
            "fields": fields,
            "stage_inputs": stage_inputs,
            "stage_fingerprints": stage_fingerprints,
            "config_version": config_version,
            "version_fingerprint": version_fingerprint,
        }

    def _earliest_changed_stage(
        self, old_fields: Mapping[str, Any], new_fields: Mapping[str, Any]
    ) -> tuple[str | None, list[str]]:
        changed = sorted(
            key
            for key in set(old_fields) | set(new_fields)
            if old_fields.get(key) != new_fields.get(key)
        )
        if not changed:
            return None, []
        stages = [FIELD_STAGE[key] for key in changed if key in FIELD_STAGE]
        if not stages:
            return None, changed
        return min(stages, key=lambda stage: STAGE_INDEX[stage]), changed

    def _verify_checkpoint_artifact(
        self, request: SamplingRequest, stage: str, checkpoint: Mapping[str, Any]
    ) -> None:
        store = self._store(request.request_id)
        path = store.artifact_path(stage)
        metadata = checkpoint.get("stage_metadata", {})
        digest = checkpoint["artifact_digest"]
        if stage == "postprocess":
            load_postprocess_artifact(path, request, metadata, digest)
        elif stage == "normalize":
            load_normalize_artifact(path, request, metadata, digest)
        elif stage == "decode":
            load_decode_artifact(path, request, digest)
        elif stage == "summarize":
            load_summary_artifact(path, request, digest)
        else:
            raise invalid_stage(
                "unknown stage", request_id=request.request_id, stage=stage
            )

    def _require_stage_chain(
        self, request_id: str, active: Mapping[str, Mapping[str, Any]]
    ) -> None:
        for index, stage in enumerate(STAGES):
            if stage not in active:
                continue
            if index == 0:
                continue
            previous = STAGES[index - 1]
            if previous not in active:
                raise invalid_stage(
                    "checkpoint chain has a missing predecessor",
                    request_id=request_id,
                    stage=stage,
                    missing_stage=previous,
                )

    def _prepare_request(
        self, request: SamplingRequest
    ) -> tuple[RequestStore, dict[str, Any], dict[str, Any], dict[str, Any]]:
        store = self._store(request.request_id)
        store.recover_pending_invalidation()
        state = store.load_state()
        expected = self.compute_input_fingerprints(request)
        active = store.load_active_checkpoints()
        self._require_stage_chain(request.request_id, active)
        for stage in STAGES:
            checkpoint = active.get(stage)
            if checkpoint is not None:
                self._verify_checkpoint_artifact(request, stage, checkpoint)
        new_fields = expected["fields"]
        earliest: str | None = None
        changed_fields: list[str] = []
        reasons: list[str] = []
        if state.get("fields"):
            earliest_from_fields, changed_fields = self._earliest_changed_stage(
                state["fields"], new_fields
            )
            if earliest_from_fields is not None:
                earliest = earliest_from_fields
                reasons.append("content_fingerprint_changed")
        for stage in STAGES:
            checkpoint = active.get(stage)
            if checkpoint is None:
                continue
            if (
                checkpoint.get("input_fingerprint")
                != expected["stage_inputs"][stage]
            ):
                candidate = stage
                if earliest is None or STAGE_INDEX[candidate] < STAGE_INDEX[earliest]:
                    earliest = candidate
                reasons.append(f"{stage}_input_fingerprint_changed")
            if (
                checkpoint.get("algorithm_version")
                != ALGORITHM_VERSIONS[stage]
            ):
                candidate = stage
                if earliest is None or STAGE_INDEX[candidate] < STAGE_INDEX[earliest]:
                    earliest = candidate
                reasons.append(f"{stage}_algorithm_version_changed")
        if earliest is not None:
            old_fingerprints = state.get("stage_fingerprints", {})
            entry = store.invalidate_from(
                earliest,
                changed_fields=changed_fields,
                reasons=sorted(set(reasons)),
                old_input_fingerprints=old_fingerprints,
                new_input_fingerprints=expected["stage_fingerprints"],
                fields=new_fields,
                stage_fingerprints=expected["stage_fingerprints"],
                version_fingerprint=expected["version_fingerprint"],
                config_version=expected["config_version"],
            )
            state = store.load_state()
            active = store.load_active_checkpoints()
            self._require_stage_chain(request.request_id, active)
        else:
            previous_fingerprint = state.get("version_fingerprint")
            if previous_fingerprint != expected["version_fingerprint"]:
                store.update_expected_state(
                    fields=new_fields,
                    stage_fingerprints=expected["stage_fingerprints"],
                    version_fingerprint=expected["version_fingerprint"],
                    config_version=expected["config_version"],
                )
            state = store.load_state()
        return store, state, expected, active

    def _store(self, request_id: str) -> RequestStore:
        return RequestStore(
            self.workspace, request_id, lock_timeout=self.config.lock_timeout_seconds
        )

    # ------------------------------------------------------------------
    # Stage execution
    # ------------------------------------------------------------------
    def _load_stage_data(
        self,
        request: SamplingRequest,
        store: RequestStore,
        stage: str,
        checkpoint: Mapping[str, Any],
    ) -> Any:
        path = store.artifact_path(stage)
        metadata = checkpoint.get("stage_metadata", {})
        digest = checkpoint["artifact_digest"]
        if stage == "postprocess":
            return load_postprocess_artifact(path, request, metadata, digest)
        if stage == "normalize":
            return load_normalize_artifact(path, request, metadata, digest)
        if stage == "decode":
            return load_decode_artifact(path, request, digest)
        if stage == "summarize":
            return load_summary_artifact(path, request, digest)
        raise invalid_stage(
            "unknown stage", request_id=request.request_id, stage=stage
        )

    def _publish_with_retry(
        self,
        store: RequestStore,
        request: SamplingRequest,
        stage: str,
        staging: Path,
        artifact: Any,
        expected: Mapping[str, Any],
    ) -> dict[str, Any]:
        last_error: EngineError | None = None
        for _ in range(4):
            sequence = store.next_sequence()
            record = {
                "request_id": request.request_id,
                "stage": stage,
                "sequence": sequence,
                "input_fingerprint": expected["stage_inputs"][stage],
                "config_version": expected["config_version"],
                "artifact_digest": artifact.artifact_digest,
                "algorithm_version": ALGORITHM_VERSIONS[stage],
                "stage_metadata": artifact.metadata,
            }
            try:
                return store.publish_checkpoint(record, staging)
            except EngineError as exc:
                if exc.code != "E_STALE_CHECKPOINT":
                    raise
                last_error = exc
                active = store.load_active_checkpoints()
                existing = active.get(stage)
                if (
                    existing is not None
                    and existing.get("input_fingerprint")
                    == expected["stage_inputs"][stage]
                    and existing.get("artifact_digest") == artifact.artifact_digest
                ):
                    if staging.exists():
                        staging.unlink()
                    return existing
                if staging.exists() and stage != "summarize":
                    continue
                raise
        if last_error is not None:
            raise last_error
        raise EngineError(
            E_CONFLICT,
            "checkpoint publication retry exhausted",
            {"request_id": request.request_id, "stage": stage},
        )

    def _run_stages(
        self, request: SamplingRequest
    ) -> tuple[dict[str, Any], list[str], list[str], list[dict[str, Any]]]:
        store, state, expected, active = self._prepare_request(request)
        invalidations = list(state.get("invalidations", []))
        logits_path = request.resolved_logits_path()
        reused: list[str] = []
        rebuilt: list[str] = []
        checkpoints: dict[str, dict[str, Any]] = {}
        stage_data: dict[str, Any] = {}
        for index, stage in enumerate(STAGES):
            checkpoint = active.get(stage)
            if checkpoint is not None:
                if checkpoint.get("input_fingerprint") != expected["stage_inputs"][stage]:
                    raise invalid_stage(
                        "stage fingerprint changed after invalidation",
                        request_id=request.request_id,
                        stage=stage,
                    )
                stage_data[stage] = self._load_stage_data(
                    request, store, stage, checkpoint
                )
                checkpoints[stage] = checkpoint
                reused.append(stage)
                continue
            if index > 0:
                previous = STAGES[index - 1]
                if previous not in checkpoints:
                    raise invalid_stage(
                        "previous stage checkpoint is missing",
                        request_id=request.request_id,
                        stage=stage,
                        missing_stage=previous,
                    )
            with staging_path(store.root, stage) as staged:
                if stage == "postprocess":
                    artifact = run_postprocess(
                        request,
                        logits_path,
                        staged,
                        max_temperature=self.config.max_temperature,
                    )
                elif stage == "normalize":
                    post_cp = checkpoints["postprocess"]
                    artifact = run_normalize(
                        request,
                        store.artifact_path("postprocess"),
                        post_cp.get("stage_metadata", {}),
                        post_cp["artifact_digest"],
                        staged,
                    )
                elif stage == "decode":
                    norm_cp = checkpoints["normalize"]
                    artifact = run_decode(
                        request,
                        store.artifact_path("normalize"),
                        norm_cp.get("stage_metadata", {}),
                        norm_cp["artifact_digest"],
                        staged,
                    )
                else:
                    post_cp = checkpoints["postprocess"]
                    norm_cp = checkpoints["normalize"]
                    decode_cp = checkpoints["decode"]
                    artifact = run_summarize(
                        request,
                        store.artifact_path("postprocess"),
                        post_cp.get("stage_metadata", {}),
                        post_cp["artifact_digest"],
                        store.artifact_path("normalize"),
                        norm_cp.get("stage_metadata", {}),
                        norm_cp["artifact_digest"],
                        store.artifact_path("decode"),
                        decode_cp["artifact_digest"],
                        staged,
                        version_fingerprint=expected["version_fingerprint"],
                        config_version=expected["config_version"],
                    )
                checkpoint = self._publish_with_retry(
                    store, request, stage, staged, artifact, expected
                )
            self._bump_generation()
            checkpoints[stage] = checkpoint
            rebuilt.append(stage)
            stage_data[stage] = self._load_stage_data(
                request, store, stage, checkpoint
            )
        store.update_expected_state(
            fields=expected["fields"],
            stage_fingerprints=expected["stage_fingerprints"],
            version_fingerprint=expected["version_fingerprint"],
            config_version=expected["config_version"],
        )
        return checkpoints, reused, rebuilt, invalidations

    def _record_error(
        self,
        request: SamplingRequest,
        expected: Mapping[str, Any] | None,
        stage: str,
        error: EngineError,
    ) -> None:
        payload = {
            "error_schema": 1,
            "request_id": request.request_id,
            "stage": stage,
            "error": error.to_dict(),
            "config_version": expected.get("config_version") if expected else None,
            "version_fingerprint": (
                expected.get("version_fingerprint") if expected else None
            ),
        }
        atomic_write_json(self.errors_dir / f"{request.request_id}.json", payload)

    def run_request(self, request_id: str) -> dict[str, Any]:
        entry = self._get_request_entry(request_id)
        request = self._request_from_entry(entry)
        expected: dict[str, Any] | None = None
        try:
            expected = self.compute_input_fingerprints(request)
            checkpoints, reused, rebuilt, invalidations = self._run_stages(request)
            summary = load_summary_artifact(
                self._store(request_id).artifact_path("summarize"),
                request,
                checkpoints["summarize"]["artifact_digest"],
            )
            error_path = self.errors_dir / f"{request_id}.json"
            if error_path.exists():
                try:
                    error_path.unlink()
                except OSError:
                    pass
            return {
                "request_id": request_id,
                "status": "ok",
                "manifest_version": request.manifest_version,
                "tokens": summary["tokens"],
                "token_text": summary["token_text"],
                "metrics": summary["metrics"],
                "precision": summary["precision"],
                "version_fingerprint": expected["version_fingerprint"],
                "config_version": expected["config_version"],
                "stage_artifact_digests": {
                    stage: checkpoints[stage]["artifact_digest"] for stage in STAGES
                },
                "reused_stages": reused,
                "rebuilt_stages": rebuilt,
                "invalidations": invalidations,
            }
        except EngineError as exc:
            stage = str(exc.context.get("stage", "unknown"))
            self._record_error(request, expected, stage, exc)
            raise

    # ------------------------------------------------------------------
    # Bulk execution, stop, and resume
    # ------------------------------------------------------------------
    def stop(self) -> dict[str, Any]:
        self._stop_event.set()
        return {"ok": True, "stop_requested": True}

    def clear_stop(self) -> None:
        self._stop_event.clear()

    def run(
        self,
        request_ids: Iterable[str] | None = None,
        *,
        stop_after_requests: int | None = None,
        continue_on_error: bool = True,
        clear_stop: bool = True,
    ) -> dict[str, Any]:
        if stop_after_requests is not None and stop_after_requests < 0:
            raise EngineError(
                E_CONFLICT,
                "stop_after_requests must be non-negative",
                {"stop_after_requests": stop_after_requests},
            )
        if clear_stop:
            self._stop_event.clear()
        with self._run_lock:
            if request_ids is None:
                ids = [entry["request_id"] for entry in self._all_request_entries()]
            else:
                ids = [str(request_id) for request_id in request_ids]
            processed: list[dict[str, Any]] = []
            stopped = False
            for request_id in ids:
                if self._stop_event.is_set() or (
                    stop_after_requests is not None
                    and len(processed) >= stop_after_requests
                ):
                    stopped = True
                    break
                try:
                    result = self.run_request(request_id)
                except EngineError as exc:
                    if not continue_on_error:
                        raise
                    result = {
                        "request_id": request_id,
                        "status": "error",
                        "error": exc.to_dict(),
                    }
                processed.append(result)
            if self._stop_event.is_set() and len(processed) < len(ids):
                stopped = True
            return {
                "ok": True,
                "stopped": stopped,
                "processed_count": len(processed),
                "processed": processed,
                "result": self.export_result(),
            }

    def resume(
        self,
        request_ids: Iterable[str] | None = None,
        *,
        continue_on_error: bool = True,
    ) -> dict[str, Any]:
        self.clear_stop()
        return self.run(
            request_ids,
            continue_on_error=continue_on_error,
            clear_stop=False,
        )

    # ------------------------------------------------------------------
    # Query APIs
    # ------------------------------------------------------------------
    def _status_from_entry(self, entry: Mapping[str, Any]) -> dict[str, Any]:
        request = self._request_from_entry(entry)
        store = self._store(request.request_id)
        state = store.load_state()
        expected = self.compute_input_fingerprints(request)
        active = store.load_active_checkpoints()
        stages: dict[str, Any] = {}
        complete = True
        for stage in STAGES:
            checkpoint = active.get(stage)
            if checkpoint is None:
                complete = False
                stages[stage] = {
                    "status": "missing",
                    "expected_input_fingerprint": expected["stage_inputs"][stage],
                    "algorithm_version": ALGORITHM_VERSIONS[stage],
                }
                continue
            current = (
                checkpoint.get("input_fingerprint")
                == expected["stage_inputs"][stage]
                and checkpoint.get("algorithm_version")
                == ALGORITHM_VERSIONS[stage]
            )
            if not current:
                complete = False
            stages[stage] = {
                "status": "current" if current else "stale",
                "checkpoint": checkpoint,
                "expected_input_fingerprint": expected["stage_inputs"][stage],
                "algorithm_version": ALGORITHM_VERSIONS[stage],
            }
        error_payload = None
        error_path = self.errors_dir / f"{request.request_id}.json"
        if error_path.exists():
            try:
                candidate = read_json(error_path)
                if candidate.get("config_version") == expected["config_version"]:
                    error_payload = candidate
            except (OSError, json.JSONDecodeError):
                error_payload = {"error": {"code": "E_SNAPSHOT_CONFLICT"}}
        return {
            "request_id": request.request_id,
            "status": (
                "error"
                if error_payload
                else ("complete" if complete else "pending")
            ),
            "manifest_version": request.manifest_version,
            "config_version": expected["config_version"],
            "version_fingerprint": expected["version_fingerprint"],
            "stage_input_fingerprints": expected["stage_inputs"],
            "stages": stages,
            "invalidations": state.get("invalidations", []),
            "sequence": store.history_summary(),
            "error": error_payload,
        }

    def request_status(self, request_id: str) -> dict[str, Any]:
        return self._status_from_entry(self._get_request_entry(request_id))

    def request_fingerprints(self, request_id: str) -> dict[str, Any]:
        status = self.request_status(request_id)
        return {
            "request_id": request_id,
            "version_fingerprint": status["version_fingerprint"],
            "config_version": status["config_version"],
            "stage_input_fingerprints": status["stage_input_fingerprints"],
            "stages": {
                stage: {
                    "input_fingerprint": info.get(
                        "expected_input_fingerprint"
                    ),
                    "artifact_digest": (
                        info.get("checkpoint", {}).get("artifact_digest")
                    ),
                    "status": info["status"],
                }
                for stage, info in status["stages"].items()
            },
        }

    def request_tokens(self, request_id: str) -> dict[str, Any]:
        entry = self._get_request_entry(request_id)
        request = self._request_from_entry(entry)
        store = self._store(request_id)
        checkpoint = store.checkpoint_for("decode")
        if checkpoint is None:
            raise invalid_stage(
                "decode stage has not completed",
                request_id=request_id,
                stage="decode",
                missing_stage="decode",
            )
        payload = load_decode_artifact(
            store.artifact_path("decode"),
            request,
            checkpoint["artifact_digest"],
        )
        steps = []
        for index, token in enumerate(payload["tokens"]):
            steps.append(
                {
                    "step": index,
                    "token_id": token,
                    "token": request.vocab[token],
                    "log_prob": payload["step_log_probs"][index],
                    "entropy_nats": payload["step_entropies"][index],
                    "candidate_count": payload["candidate_counts"][index],
                    "gumbel_score": payload["gumbel_scores"][index],
                }
            )
        return {
            "request_id": request_id,
            "seed": request.seed,
            "sampler": payload["sampler"],
            "tokens": payload["tokens"],
            "token_text": payload["token_text"],
            "steps": steps,
        }

    def request_temperature_detail(self, request_id: str) -> dict[str, Any]:
        entry = self._get_request_entry(request_id)
        return {
            "request_id": request_id,
            "manifest_version": entry["manifest_version"],
            "conversion": entry.get("conversion"),
            "effective_temperature": entry["temperature"],
            "top_k": entry["top_k"],
            "top_p": entry["top_p"],
            "seed": entry["seed"],
            "manifest_id": entry.get("manifest_id"),
        }

    def request_summary(self, request_id: str) -> dict[str, Any]:
        entry = self._get_request_entry(request_id)
        request = self._request_from_entry(entry)
        store = self._store(request_id)
        checkpoint = store.checkpoint_for("summarize")
        if checkpoint is None:
            raise invalid_stage(
                "summary stage has not completed",
                request_id=request_id,
                stage="summarize",
                missing_stage="summarize",
            )
        return load_summary_artifact(
            store.artifact_path("summarize"),
            request,
            checkpoint["artifact_digest"],
        )

    def corpus_summary(self) -> dict[str, Any]:
        summaries: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        for entry in self._all_request_entries():
            request_id = entry["request_id"]
            status = self._status_from_entry(entry)
            if status["status"] == "complete":
                summaries.append(self.request_summary(request_id))
            elif status["status"] == "error" and status.get("error"):
                failures.append(status["error"])
        metrics = corpus_metrics(summaries)
        metrics["request_count"] = len(summaries)
        metrics["failed_request_count"] = len(failures)
        metrics["successful_request_count"] = len(summaries)
        metrics["failures"] = failures
        return metrics

    def export_result(self) -> dict[str, Any]:
        requests: list[dict[str, Any]] = []
        successful_summaries: list[dict[str, Any]] = []
        for entry in self._all_request_entries():
            request_id = entry["request_id"]
            status = self._status_from_entry(entry)
            if status["status"] == "complete":
                summary = self.request_summary(request_id)
                successful_summaries.append(summary)
                requests.append(
                    {
                        "request_id": request_id,
                        "status": "ok",
                        "manifest_version": entry["manifest_version"],
                        "tokens": summary["tokens"],
                        "token_text": summary["token_text"],
                        "metrics": summary["metrics"],
                        "precision": summary["precision"],
                        "summary_digest": digest_json(summary),
                        "version_fingerprint": summary["version_fingerprint"],
                    }
                )
            elif status["status"] == "error":
                requests.append(
                    {
                        "request_id": request_id,
                        "status": "error",
                        "manifest_version": entry["manifest_version"],
                        "error": status["error"]["error"],
                    }
                )
            else:
                requests.append(
                    {
                        "request_id": request_id,
                        "status": "pending",
                        "manifest_version": entry["manifest_version"],
                    }
                )
        corpus = corpus_metrics(successful_summaries)
        corpus["failed_request_count"] = sum(
            1 for request in requests if request["status"] == "error"
        )
        corpus["successful_request_count"] = len(successful_summaries)
        corpus["pending_request_count"] = sum(
            1 for request in requests if request["status"] == "pending"
        )
        business = {
            "result_schema": "sampling-engine-result-v1",
            "requests": requests,
            "corpus": corpus,
        }
        return {
            **business,
            "result_digest": digest_json(business),
            "workspace_generation": self.workspace_metadata().get("generation"),
        }

    def export_manifest(self, manifest_id: str, target_version: int) -> dict[str, Any]:
        registry = self._registry_snapshot()
        group = registry["manifests"].get(manifest_id)
        if group is None:
            raise EngineError(
                E_NOT_FOUND,
                "manifest group not found",
                {"manifest_id": manifest_id},
            )
        requests: list[SamplingRequest] = []
        conversions: dict[str, dict[str, Any]] = {}
        for request_id in group["request_ids"]:
            entry = registry["requests"].get(request_id)
            if entry is None:
                raise snapshot_conflict(
                    "imported manifest references a missing request",
                    reason="digest_mismatch",
                    manifest_id=manifest_id,
                    request_id=request_id,
                )
            requests.append(self._request_from_entry(entry))
            conversions[request_id] = entry.get("conversion", {})
        manifest = Manifest(
            version=int(group["version"]),
            requests=tuple(requests),
            vocab=tuple(str(token) for token in group["vocab"]),
            metadata=dict(group.get("metadata", {})),
            conversions=conversions,
            warnings=tuple(group.get("warnings", [])),
            source_path=group.get("source_path"),
            manifest_digest=group.get("manifest_digest"),
        )
        return self.codec.export_document(manifest, target_version)

    def manifest_groups(self) -> list[dict[str, Any]]:
        registry = self._registry_snapshot()
        return [
            {
                "manifest_id": group["manifest_id"],
                "version": group["version"],
                "source_path": group.get("source_path"),
                "request_ids": group["request_ids"],
                "warnings": group.get("warnings", []),
            }
            for group in registry["manifests"].values()
        ]
