"""Atomic checkpoint persistence, sequence validation and publish protection."""
from __future__ import annotations

import os

from .errors import E_INVALID_STAGE, E_STALE_CHECKPOINT, E_STALE_SNAPSHOT, raise_error
from .util import atomic_write, digest_obj, file_lock, read_json, sha256_hex, write_json

STAGES = ["normalize", "features", "scoring", "summary"]
CHECKPOINT_FILE = "checkpoints.jsonl"
LOCK_FILE = ".workbench.lock"


class CheckpointStore:
    def __init__(self, workdir: str):
        self.workdir = workdir
        os.makedirs(workdir, exist_ok=True)
        self.lock_path = os.path.join(workdir, LOCK_FILE)
        self.checkpoint_path = os.path.join(workdir, CHECKPOINT_FILE)

    def lock(self):
        return file_lock(self.lock_path)

    def load_records(self) -> list[dict]:
        """Ignore corrupt/half-written trailing or inline records."""
        if not os.path.exists(self.checkpoint_path):
            return []
        records = []
        with open(self.checkpoint_path, "rb") as fh:
            for line_no, raw in enumerate(fh, start=1):
                try:
                    import json

                    record = json.loads(raw.decode("utf-8"))
                except (ValueError, UnicodeDecodeError):
                    continue
                required = {
                    "seq",
                    "stage",
                    "input_digest",
                    "config_version",
                    "config_fingerprint",
                    "artifact_digest",
                    "artifact_path",
                    "record_digest",
                }
                if not required.issubset(record):
                    continue
                expected = _record_digest(record)
                if expected != record["record_digest"]:
                    continue
                records.append(record)
        return records

    def publish(
        self,
        stage: str,
        input_digest: str,
        config_fingerprint: str,
        config_version: str,
        artifact_path: str,
        artifact: object,
        expected_seq: int | None = None,
    ) -> dict:
        if stage not in STAGES:
            raise_error(E_INVALID_STAGE, f"unknown stage: {stage}", {"stage": stage})
        stage_index = STAGES.index(stage)
        with self.lock():
            records = self.load_records()
            if expected_seq is not None and (
                (records and records[-1]["seq"] != expected_seq)
                or (not records and expected_seq != 0)
            ):
                raise_error(
                    E_STALE_CHECKPOINT,
                    "publish sequence mismatch",
                    {"expected_seq": expected_seq, "actual_tail": records[-1]["seq"] if records else 0},
                )
            existing_stages = {r["stage"]: r for r in records}
            if stage in existing_stages:
                raise_error(
                    E_STALE_CHECKPOINT,
                    "duplicate stage publish",
                    {"stage": stage, "seq": existing_stages[stage]["seq"]},
                )
            if stage_index > 0 and STAGES[stage_index - 1] not in existing_stages:
                raise_error(
                    E_INVALID_STAGE,
                    "missing prerequisite stage artifact",
                    {"stage": stage, "prerequisite": STAGES[stage_index - 1]},
                )
            tail_seq = records[-1]["seq"] if records else 0
            if expected_seq is not None and tail_seq != expected_seq:
                raise_error(E_STALE_CHECKPOINT, "non-contiguous checkpoint sequence")
            next_seq = tail_seq + 1
            artifact_digest = write_json(os.path.join(self.workdir, artifact_path), artifact)
            record = {
                "seq": next_seq,
                "stage": stage,
                "input_digest": input_digest,
                "config_version": config_version,
                "config_fingerprint": config_fingerprint,
                "artifact_digest": artifact_digest,
                "artifact_path": artifact_path,
            }
            record["record_digest"] = _record_digest(record)
            # Append is itself atomic for small writes, but temp+replace guarantees
            # no half-written record even under interruption.
            self._append_atomic(record)
            return record

    def _append_atomic(self, record: dict) -> None:
        from .util import canonical_dumps

        existing = b""
        if os.path.exists(self.checkpoint_path):
            with open(self.checkpoint_path, "rb") as fh:
                existing = fh.read()
        atomic_write(self.checkpoint_path, existing + canonical_dumps(record))

    def verify_chain(self, config_fingerprint: str, input_digest: str) -> list[dict]:
        records = self.load_records()
        for idx, record in enumerate(records, start=1):
            if record["seq"] != idx:
                raise_error(
                    E_STALE_SNAPSHOT,
                    "checkpoint sequence regressed or has a gap",
                    {"expected": idx, "actual": record["seq"]},
                )
            if record["stage"] != STAGES[idx - 1]:
                raise_error(
                    E_STALE_SNAPSHOT,
                    "checkpoint stage ordering mismatch",
                    {"expected_stage": STAGES[idx - 1], "actual_stage": record["stage"]},
                )
            artifact_path = os.path.join(self.workdir, record["artifact_path"])
            if not os.path.exists(artifact_path):
                raise_error(
                    E_STALE_SNAPSHOT,
                    "artifact referenced by checkpoint is missing",
                    {"artifact_path": record["artifact_path"]},
                )
            with open(artifact_path, "rb") as fh:
                actual_digest = sha256_hex(fh.read())
            if actual_digest != record["artifact_digest"]:
                raise_error(
                    E_STALE_SNAPSHOT,
                    "artifact digest mismatch: snapshot was replaced",
                    {"stage": record["stage"]},
                )
            if record["config_fingerprint"] != config_fingerprint:
                raise_error(
                    E_STALE_SNAPSHOT,
                    "config fingerprint mismatch",
                    {"stage": record["stage"]},
                )
            if record["input_digest"] != input_digest:
                raise_error(
                    E_STALE_SNAPSHOT,
                    "input shard digest mismatch",
                    {"stage": record["stage"]},
                )
        return records

    def stage_artifacts(self) -> dict[str, dict]:
        return {r["stage"]: r for r in self.load_records()}

    def load_artifact(self, stage: str):
        records = self.stage_artifacts()
        if stage not in records:
            raise_error(E_INVALID_STAGE, f"no checkpoint artifact for stage: {stage}")
        return read_json(os.path.join(self.workdir, records[stage]["artifact_path"]))

    def reset(self) -> None:
        with self.lock():
            for name in os.listdir(self.workdir):
                path = os.path.join(self.workdir, name)
                if name in (CHECKPOINT_FILE, LOCK_FILE):
                    if os.path.exists(path):
                        os.unlink(path)
                elif name.endswith(".json") and os.path.isfile(path):
                    os.unlink(path)

    def rollback_to(self, keep_stage_index: int) -> list[str]:
        """Remove checkpoints/artifacts from stage index keep_stage_index onward.

        keep_stage_index == -1 clears every stage. Returns removed stage names.
        """
        from .util import canonical_dumps

        with self.lock():
            records = self.load_records()
            kept = [r for r in records if STAGES.index(r["stage"]) <= keep_stage_index]
            removed = [r for r in records if STAGES.index(r["stage"]) > keep_stage_index]
            for record in removed:
                path = os.path.join(self.workdir, record["artifact_path"])
                if os.path.exists(path):
                    os.unlink(path)
            payload = b"".join(canonical_dumps(r) for r in kept)
            atomic_write(self.checkpoint_path, payload)
            return [r["stage"] for r in removed]


def _record_digest(record: dict) -> str:
    payload = {k: v for k, v in record.items() if k != "record_digest"}
    return digest_obj(payload)
