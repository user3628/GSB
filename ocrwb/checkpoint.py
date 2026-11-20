"""Persistent stage checkpoints, file locks and incremental invalidation.

On-disk layout for a run workspace::

    <root>/
      manifest.json                 imported task manifest
      config.json                   workbench configuration
      input_fingerprints.json       per-shard pixel + truth fingerprints
      locks/<image_id>.lock         fcntl flock files (one per shard)
      journal/<image_id>.jsonl      append-only, atomically flushed records
      artifacts/<image_id>/<stage>.json   stage outputs (tmp + atomic rename)
      diagnostics/report.json       global diagnostic report

Every artifact write goes through a temp file + ``os.replace`` so a crash can
never leave a half-written record, and journal appends are ``fsync``-flushed.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os

from .config import STAGES, WorkbenchConfig
from .errors import InvalidStage, StaleCheckpoint
from .hashing import canonical_json, digest_file, digest_json, digest_many


class CheckpointStore:
    def __init__(self, root: str):
        self.root = os.path.abspath(root)
        self.locks_dir = os.path.join(self.root, "locks")
        self.journal_dir = os.path.join(self.root, "journal")
        self.artifacts_dir = os.path.join(self.root, "artifacts")
        self.diagnostics_dir = os.path.join(self.root, "diagnostics")
        for directory in (self.root, self.locks_dir, self.journal_dir, self.artifacts_dir, self.diagnostics_dir):
            os.makedirs(directory, exist_ok=True)

    # ------------------------------------------------------------------ paths
    @property
    def manifest_path(self) -> str:
        return os.path.join(self.root, "manifest.json")

    @property
    def config_path(self) -> str:
        return os.path.join(self.root, "config.json")

    @property
    def input_fingerprint_path(self) -> str:
        return os.path.join(self.root, "input_fingerprints.json")

    def _shard_dir(self, image_id: str) -> str:
        path = os.path.join(self.artifacts_dir, image_id)
        os.makedirs(path, exist_ok=True)
        return path

    def artifact_path(self, image_id: str, stage: str) -> str:
        self._require_stage(stage)
        return os.path.join(self._shard_dir(image_id), f"{stage}.json")

    def journal_path(self, image_id: str) -> str:
        return os.path.join(self.journal_dir, f"{image_id}.jsonl")

    # ------------------------------------------------------------------ locks
    @contextlib.contextmanager
    def shard_lock(self, image_id: str):
        lock_path = os.path.join(self.locks_dir, f"{image_id}.lock")
        with open(lock_path, "w") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    # ------------------------------------------------------------------ io
    @staticmethod
    def _require_stage(stage: str) -> None:
        if stage not in STAGES:
            raise InvalidStage(
                f"unknown stage '{stage}'",
                {"stage": stage, "known_stages": list(STAGES)},
            )

    def atomic_write_json(self, path: str, payload: dict) -> None:
        tmp_path = f"{path}.tmp.{os.getpid()}"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            handle.write(canonical_json(payload))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)

    def write_json(self, relative_path: str, payload: dict) -> str:
        path = os.path.join(self.root, relative_path)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.atomic_write_json(path, payload)
        return path

    def read_json(self, path: str) -> dict:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)

    def load_manifest(self) -> dict:
        return self.read_json(self.manifest_path)

    def load_config(self) -> WorkbenchConfig:
        return WorkbenchConfig.from_dict(self.read_json(self.config_path))

    def has_artifact(self, image_id: str, stage: str) -> bool:
        return os.path.exists(self.artifact_path(image_id, stage))

    def load_artifact(self, image_id: str, stage: str) -> dict:
        return self.read_json(self.artifact_path(image_id, stage))

    # ------------------------------------------------------------------ journal
    def read_journal(self, image_id: str) -> list[dict]:
        path = self.journal_path(image_id)
        if not os.path.exists(path):
            return []
        records = []
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
        return records

    def _append_journal(self, image_id: str, record: dict) -> None:
        path = self.journal_path(image_id)
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(canonical_json(record) + "\n")
            handle.flush()
            os.fsync(handle.fileno())

    @staticmethod
    def _stage_index(stage: str) -> int:
        return STAGES.index(stage)

    def publish(
        self,
        image_id: str,
        stage: str,
        sequence: int,
        input_fingerprint: str,
        config_version: str,
        upstream_fingerprint: str | None,
        stage_fingerprint: str,
        payload: dict,
    ) -> dict:
        """Publish one stage artifact under the shard lock.

        Rejects stale/duplicate/out-of-order sequence numbers and stage skips
        with ``E_STALE_CHECKPOINT`` / ``E_INVALID_STAGE``.  A valid publish is
        all-or-nothing: artifact + journal entry only become visible together.
        """
        self._require_stage(stage)
        with self.shard_lock(image_id):
            return self._publish_locked(
                image_id,
                stage,
                sequence,
                input_fingerprint,
                config_version,
                upstream_fingerprint,
                stage_fingerprint,
                payload,
            )

    def _publish_locked(
        self,
        image_id: str,
        stage: str,
        sequence: int,
        input_fingerprint: str,
        config_version: str,
        upstream_fingerprint: str | None,
        stage_fingerprint: str,
        payload: dict,
    ) -> dict:
        """Publishing logic for callers already holding the shard lock."""
        journal = self.read_journal(image_id)
        stage_index = self._stage_index(stage)
        if stage == "diagnose":
            prerequisite = "recognize"
        elif stage_index > 0:
            prerequisite = STAGES[stage_index - 1]
        else:
            prerequisite = None
        if prerequisite is not None:
            if not self.has_artifact(image_id, prerequisite):
                raise InvalidStage(
                    f"stage '{stage}' requires prerequisite '{prerequisite}'",
                    {"image_id": image_id, "stage": stage, "missing": prerequisite},
                )
        if any(record["stage"] == stage for record in journal):
            raise StaleCheckpoint(
                "stage artifact already published",
                {"image_id": image_id, "stage": stage},
            )
        last_sequence = journal[-1]["sequence"] if journal else 0
        expected_sequence = last_sequence + 1
        if sequence != expected_sequence:
            raise StaleCheckpoint(
                "checkpoint sequence is not the next monotonic value",
                {
                    "image_id": image_id,
                    "stage": stage,
                    "received_sequence": sequence,
                    "expected_sequence": expected_sequence,
                },
            )
        artifact_summary = digest_json(payload)[:16]
        record = {
            "stage": stage,
            "image_id": image_id,
            "sequence": sequence,
            "input_fingerprint": input_fingerprint,
            "config_version": config_version,
            "upstream_fingerprint": upstream_fingerprint,
            "stage_fingerprint": stage_fingerprint,
            "artifact_summary": artifact_summary,
        }
        target = self.artifact_path(image_id, stage)
        self.atomic_write_json(target, payload)
        self._append_journal(image_id, record)
        return record

    # ------------------------------------------------------- fingerprints
    def compute_input_fingerprints(self, sample_dir: str) -> dict:
        manifest = self.load_manifest()
        fingerprints = {}
        for shard in manifest["shards"]:
            image_path = os.path.join(sample_dir, shard["path"])
            truth_payload = [
                {"text": item["text"], "language": item["language"], "box": item["box"]}
                for item in shard["truth"]
            ]
            fingerprints[shard["image_id"]] = digest_many(
                [digest_file(image_path), digest_json(truth_payload), str(shard["char_count"])]
            )[:16]
        self.atomic_write_json(self.input_fingerprint_path, fingerprints)
        return fingerprints

    def load_input_fingerprints(self) -> dict:
        if os.path.exists(self.input_fingerprint_path):
            return self.read_json(self.input_fingerprint_path)
        return {}

    @staticmethod
    def stage_fingerprint(
        stage: str,
        input_fingerprint: str,
        config_version: str,
        upstream_fingerprint: str | None,
        payload: dict,
    ) -> str:
        parts = [stage, input_fingerprint, config_version]
        if upstream_fingerprint is not None:
            parts.append(upstream_fingerprint)
        parts.append(digest_json(payload)[:16])
        return digest_many(parts)[:16]

    def status(self) -> dict:
        manifest = self.load_manifest()
        result = {"shards": [], "stages": list(STAGES)}
        for shard in manifest["shards"]:
            journal = self.read_journal(shard["image_id"])
            result["shards"].append(
                {
                    "image_id": shard["image_id"],
                    "group": shard["group"],
                    "published_stages": [record["stage"] for record in journal],
                    "journal": journal,
                    "artifacts": {
                        stage: {
                            "exists": self.has_artifact(shard["image_id"], stage),
                            "path": self.artifact_path(shard["image_id"], stage),
                        }
                        for stage in STAGES
                    },
                }
            )
        return result

    def invalidate_shard_from(self, image_id: str, stage: str) -> list[str]:
        """Remove ``stage`` and every downstream artifact/journal entry."""
        self._require_stage(stage)
        with self.shard_lock(image_id):
            return self._invalidate_locked(image_id, stage)

    def _invalidate_locked(self, image_id: str, stage: str) -> list[str]:
        """Invalidation logic for callers that already hold the shard lock."""
        removed: list[str] = []
        start = self._stage_index(stage)
        for downstream in STAGES[start:]:
            path = self.artifact_path(image_id, downstream)
            if os.path.exists(path):
                os.remove(path)
                removed.append(path)
        surviving = [
            record
            for record in self.read_journal(image_id)
            if self._stage_index(record["stage"]) < start
        ]
        path = self.journal_path(image_id)
        if os.path.exists(path):
            os.remove(path)
        for record in surviving:
            self._append_journal(image_id, record)
        return removed
