"""Stage checkpoint persistence with atomic writes and publish protection.

Layout inside a workspace directory:
  checkpoints/<shard_id>/<stage>.json   checkpoint records
  artifacts/<shard_id>/<stage>.json     stage artifacts
  checkpoints/manifest.json             {"last_seq": N}
  checkpoints/publish.lock              cross-process publish lock

Every checkpoint record carries: shard_id, stage, seq (monotonic),
input_fingerprint, config_version, artifact_digest, artifact_path.
Writes are atomic (temp file + fsync + os.replace) so an interrupted
write never leaves a half-written record.
"""

from __future__ import annotations

import json
import os

from .errors import PipelineError
from .errors import E_STALE_CHECKPOINT
from .hashing import content_digest, file_digest
from .locking import file_lock
from .stages import STAGE_ORDER


def atomic_write_json(path, obj) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp.{os.getpid()}"
    blob = json.dumps(obj, ensure_ascii=False, sort_keys=True,
                     separators=(",", ":")).encode("utf-8")
    with open(tmp, "wb") as fh:
        fh.write(blob)
        fh.write(b"\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def read_json(path, default=None):
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


class CheckpointStore:
    def __init__(self, workspace):
        self.workspace = workspace
        self.ckpt_dir = os.path.join(workspace, "checkpoints")
        self.art_dir = os.path.join(workspace, "artifacts")
        self.manifest_path = os.path.join(self.ckpt_dir, "manifest.json")
        self.lock_path = os.path.join(self.ckpt_dir, "publish.lock")
        os.makedirs(self.ckpt_dir, exist_ok=True)
        os.makedirs(self.art_dir, exist_ok=True)

    def record_path(self, shard_id: str, stage: str) -> str:
        return os.path.join(self.ckpt_dir, shard_id, f"{stage}.json")

    def artifact_path(self, shard_id: str, stage: str) -> str:
        return os.path.join(self.art_dir, shard_id, f"{stage}.json")

    def _last_seq(self) -> int:
        manifest = read_json(self.manifest_path, default={"last_seq": 0})
        return int(manifest.get("last_seq", 0))

    def _set_last_seq(self, seq: int) -> None:
        atomic_write_json(self.manifest_path, {"last_seq": seq})

    def publish(self, shard_id: str, stage: str, input_fingerprint: str,
                config_version: str, artifact: dict) -> dict:
        """Atomically write artifact + checkpoint under the publish lock."""
        if stage not in STAGE_ORDER:
            raise PipelineError(
                "E_INVALID_STAGE", f"unknown stage {stage}", stage=stage)
        art_path = self.artifact_path(shard_id, stage)
        with file_lock(self.lock_path):
            seq = self._last_seq() + 1
            atomic_write_json(art_path, artifact)
            record = {
                "shard_id": shard_id,
                "stage": stage,
                "seq": seq,
                "input_fingerprint": input_fingerprint,
                "config_version": config_version,
                "artifact_digest": file_digest(art_path),
                "artifact_path": os.path.relpath(art_path, self.workspace),
            }
            atomic_write_json(self.record_path(shard_id, stage), record)
            self._set_last_seq(seq)
        return record

    def publish_with_seq(self, record: dict, artifact: dict) -> dict:
        """Publish a record carrying an explicit seq (used by workers).

        Enforces strictly-continuous advancement of the global sequence.
        """
        shard_id = record["shard_id"]
        stage = record["stage"]
        art_path = self.artifact_path(shard_id, stage)
        with file_lock(self.lock_path):
            expected = self._last_seq() + 1
            if record.get("seq") != expected:
                raise PipelineError(
                    E_STALE_CHECKPOINT,
                    f"stale checkpoint publish for {shard_id}/{stage}: "
                    f"seq {record.get('seq')} != expected {expected}",
                    shard_id=shard_id, stage=stage,
                    got_seq=record.get("seq"), expected_seq=expected,
                )
            atomic_write_json(art_path, artifact)
            stored = dict(record)
            stored["artifact_digest"] = file_digest(art_path)
            stored["artifact_path"] = os.path.relpath(art_path, self.workspace)
            atomic_write_json(self.record_path(shard_id, stage), stored)
            self._set_last_seq(record["seq"])
        return stored

    def get_record(self, shard_id: str, stage: str):
        return read_json(self.record_path(shard_id, stage))

    def get_artifact(self, shard_id: str, stage: str):
        return read_json(self.artifact_path(shard_id, stage))

    def is_valid(self, shard_id: str, stage: str, expected_fingerprint: str) -> bool:
        rec = self.get_record(shard_id, stage)
        if rec is None:
            return False
        if rec.get("input_fingerprint") != expected_fingerprint:
            return False
        art = self.artifact_path(shard_id, stage)
        if not os.path.exists(art):
            return False
        return file_digest(art) == rec.get("artifact_digest")

    def invalidate(self, shard_id: str, stage: str) -> None:
        for path in (self.record_path(shard_id, stage),
                     self.artifact_path(shard_id, stage)):
            if os.path.exists(path):
                os.remove(path)

    def all_records(self) -> list:
        out = []
        if not os.path.isdir(self.ckpt_dir):
            return out
        for shard_id in sorted(os.listdir(self.ckpt_dir)):
            shard_dir = os.path.join(self.ckpt_dir, shard_id)
            if not os.path.isdir(shard_dir):
                continue
            for name in sorted(os.listdir(shard_dir)):
                if name.endswith(".json"):
                    rec = read_json(os.path.join(shard_dir, name))
                    if rec:
                        out.append(rec)
        return out


def stage_fingerprint(stage: str, input_digest: str, config_stage_digest: str) -> str:
    return content_digest({
        "stage": stage,
        "input": input_digest,
        "config": config_stage_digest,
    })
