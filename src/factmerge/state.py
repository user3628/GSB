"""Atomic checkpoints, monotonic sequences and resumable execution state."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .util import atomic_write_json, digest_obj, read_json


STAGES = ("stage1", "stage2", "stage3", "stage4")


def _safe_name(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", value)[:80]
    suffix = digest_obj({"entity_id": value})[:10]
    return f"{safe or 'entity'}-{suffix}"


class StateStore:
    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.checkpoint_dir = self.root / "checkpoints"
        self.manifest_path = self.root / "state.json"
        self.stop_path = self.root / "STOP"
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self._state = self._load()

    def _default_state(self) -> dict[str, Any]:
        return {
            "schema_version": "1.0",
            "next_seq": 1,
            "run_index": 0,
            "version": None,
            "last_invalidation": None,
            "content_invalidations": [],
            "last_run": {
                "status": "not_started",
                "completed_entities": [],
                "next_entity_id": None,
                "reuse_counts": {stage: 0 for stage in STAGES},
            },
            "artifacts": {},
        }

    def _load(self) -> dict[str, Any]:
        if not self.manifest_path.exists():
            return self._default_state()
        try:
            value = read_json(self.manifest_path)
        except (OSError, ValueError):
            return self._default_state()
        if not isinstance(value, dict):
            return self._default_state()
        default = self._default_state()
        for key, val in default.items():
            value.setdefault(key, val)
        return value

    def _save(self) -> None:
        atomic_write_json(self.manifest_path, self._state)

    @property
    def state(self) -> dict[str, Any]:
        return self._state

    def begin_run(self, version: dict[str, Any], invalidation: dict[str, Any] | None) -> None:
        self._state["run_index"] = int(self._state.get("run_index", 0)) + 1
        self._state["version"] = version
        self._state["last_invalidation"] = invalidation
        self._state["last_run"] = {
            "status": "running",
            "completed_entities": [],
            "next_entity_id": None,
            "reuse_counts": {stage: 0 for stage in STAGES},
        }
        self._save()

    def mark_invalidations(self, fingerprints: dict[str, str], invalidated_stages: list[int]) -> None:
        for entity_id, stages in self._state.get("artifacts", {}).items():
            for stage_index, stage_name in enumerate(STAGES, 1):
                entry = stages.get(stage_name)
                if not entry:
                    continue
                if stage_index in invalidated_stages or entry.get("fingerprint") != fingerprints.get(stage_name):
                    entry["status"] = "invalid"
                    entry["invalidated_stage"] = min(
                        [value for value in invalidated_stages if value <= stage_index] or [stage_index]
                    )
        self._save()

    def allocate_seq(self) -> int:
        seq = int(self._state.get("next_seq", 1))
        self._state["next_seq"] = seq + 1
        self._save()
        return seq

    def _invalidate_downstream(self, entity_id: str, broken_stage: str, reason: str) -> None:
        stage_index = STAGES.index(broken_stage) + 1
        invalidated: list[str] = []
        entity_artifacts = self._state.get("artifacts", {}).get(entity_id, {})
        for candidate in STAGES[stage_index - 1 :]:
            entry = entity_artifacts.get(candidate)
            if entry is None:
                continue
            entry["status"] = "invalid"
            entry["invalidated_stage"] = stage_index
            entry["invalidated_reason"] = reason
            invalidated.append(candidate)
        self._state.setdefault("content_invalidations", []).append(
            {
                "entity_id": entity_id,
                "broken_stage": broken_stage,
                "reason": reason,
                "invalidated_stages": invalidated,
            }
        )
        self._state["content_invalidations"] = self._state["content_invalidations"][-256:]
        self._save()

    def write_checkpoint(
        self,
        entity_id: str,
        stage: str,
        fingerprint: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        if stage not in STAGES:
            raise ValueError(f"unknown stage: {stage}")
        seq = self.allocate_seq()
        envelope = {
            "seq": seq,
            "stage": stage,
            "entity_id": entity_id,
            "fingerprint": fingerprint,
            "payload": payload,
        }
        content_digest = digest_obj(payload)
        relative = f"checkpoints/{seq:08d}-{stage}-{_safe_name(entity_id)}.json"
        atomic_write_json(self.root / relative, envelope)
        entry = {
            "seq": seq,
            "file": relative,
            "fingerprint": fingerprint,
            "content_digest": content_digest,
            "status": "valid",
        }
        self._state["artifacts"].setdefault(entity_id, {})[stage] = entry
        self._save()
        return entry

    def load_checkpoint(self, entity_id: str, stage: str, fingerprint: str) -> dict[str, Any] | None:
        entry = self._state.get("artifacts", {}).get(entity_id, {}).get(stage)
        if not entry or entry.get("fingerprint") != fingerprint or entry.get("status") != "valid":
            return None
        path = self.root / entry["file"]
        try:
            envelope = read_json(path)
        except (OSError, ValueError):
            self._invalidate_downstream(entity_id, stage, "CHECKPOINT_UNREADABLE")
            return None
        if (
            envelope.get("entity_id") != entity_id
            or envelope.get("stage") != stage
            or envelope.get("fingerprint") != fingerprint
            or int(envelope.get("seq", -1)) != int(entry.get("seq", -2))
        ):
            self._invalidate_downstream(entity_id, stage, "CHECKPOINT_ENVELOPE_MISMATCH")
            return None
        payload = envelope.get("payload")
        if not isinstance(payload, dict) or digest_obj(payload) != entry.get("content_digest"):
            self._invalidate_downstream(entity_id, stage, "CHECKPOINT_CONTENT_DIGEST_MISMATCH")
            return None
        return payload

    def record_reuse(self, stage: str) -> None:
        counts = self._state["last_run"].setdefault("reuse_counts", {})
        counts[stage] = int(counts.get(stage, 0)) + 1

    def set_run_status(
        self,
        status: str,
        completed_entities: list[str],
        next_entity_id: str | None,
    ) -> None:
        self._state["last_run"]["status"] = status
        self._state["last_run"]["completed_entities"] = sorted(completed_entities)
        self._state["last_run"]["next_entity_id"] = next_entity_id
        self._save()

    def update_content_version(self, version: dict[str, Any]) -> None:
        self._state["version"] = version
        self._save()

    def request_stop(self) -> None:
        from .util import atomic_write_bytes

        atomic_write_bytes(self.stop_path, b"stop\n")

    def clear_stop_request(self) -> None:
        self.stop_path.unlink(missing_ok=True)

    def stop_requested(self) -> bool:
        return self.stop_path.exists()

    def version_status(self) -> dict[str, Any]:
        artifacts = self._state.get("artifacts", {})
        counts = {stage: 0 for stage in STAGES}
        valid_counts = {stage: 0 for stage in STAGES}
        invalid_counts = {stage: 0 for stage in STAGES}
        for entity_stages in artifacts.values():
            for stage in STAGES:
                entry = entity_stages.get(stage)
                if not entry:
                    continue
                counts[stage] += 1
                if entry.get("status") == "valid":
                    valid_counts[stage] += 1
                else:
                    invalid_counts[stage] += 1
        version = self._state.get("version") or {}
        invalidation = self._state.get("last_invalidation")
        return {
            "schema_version": "1.0",
            "state_dir": str(self.root),
            "run_index": int(self._state.get("run_index", 0)),
            "version": version,
            "corpus_digest": version.get("corpus_digest"),
            "config_fingerprint": version.get("config_fingerprint"),
            "stage_fingerprints": version.get("stage_fingerprints", {}),
            "component_digests": version.get("component_digests", {}),
            "earliest_affected_stage": invalidation.get("earliest_stage") if invalidation else None,
            "invalidations": invalidation,
            "content_invalidations": self._state.get("content_invalidations", []),
            "checkpoint_sequence": {
                "next_seq": int(self._state.get("next_seq", 1)),
                "last_seq": int(self._state.get("next_seq", 1)) - 1,
            },
            "artifact_counts": counts,
            "valid_artifact_counts": valid_counts,
            "invalid_artifact_counts": invalid_counts,
            "last_run": self._state.get("last_run", {}),
            "stop_requested": self.stop_requested(),
        }
