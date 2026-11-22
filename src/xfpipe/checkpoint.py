"""Persistent stage checkpoints with tamper-evident sequencing.

Every completed stage appends a record to a hash-chained journal:
``{seq, stage, sample_fingerprint, config_version, artifact_digest,
prev_digest, record_digest}``.  Records are written atomically (temp file +
``os.replace``) so an interrupted write never leaves a half-written record,
and publication is serialised with an advisory file lock so concurrent
workers can only advance the sequence by exactly one.

Snapshot classification on publish/verify:
* normal advance  -> accepted
* sequence rollback or duplicate -> E_STALE_CHECKPOINT (kind=rollback)
* broken chain / digest mismatch -> E_STALE_CHECKPOINT (kind=tamper)
* missing upstream stage artefact -> E_INVALID_STAGE
"""
from __future__ import annotations

import json
import os

from .atomic import atomic_write_bytes, file_lock
from .config import STAGE_ORDER
from .errors import E_INVALID_STAGE, E_STALE_CHECKPOINT, PipelineError
from .fingerprint import canonical_json_bytes, hash_obj

GENESIS_DIGEST = "0" * 64


class CheckpointStore:
    def __init__(self, directory: str, artifacts_dir: str | None = None) -> None:
        self.directory = directory
        self.artifacts_dir = artifacts_dir or os.path.join(
            os.path.dirname(directory.rstrip(os.sep)), "artifacts"
        )
        os.makedirs(self.directory, exist_ok=True)

    # ------------------------------------------------------------------ io
    @property
    def _lock_path(self) -> str:
        return os.path.join(self.directory, ".publish.lock")

    @property
    def _latest_path(self) -> str:
        return os.path.join(self.directory, "latest.json")

    def _record_path(self, seq: int) -> str:
        return os.path.join(self.directory, f"{seq:06d}.json")

    def _read_json(self, path: str) -> dict | None:
        try:
            with open(path, "rb") as fh:
                return json.loads(fh.read().decode("utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

    def latest(self) -> dict | None:
        return self._read_json(self._latest_path)

    def read_record(self, seq: int) -> dict | None:
        return self._read_json(self._record_path(seq))

    def records(self) -> list[dict]:
        out: list[dict] = []
        seq = 1
        while True:
            rec = self.read_record(seq)
            if rec is None:
                break
            out.append(rec)
            seq += 1
        return out

    # ------------------------------------------------------------- publish
    def next_seq(self) -> int:
        """The sequence number a worker should prepare its record with."""
        latest = self.latest()
        return 1 if latest is None else int(latest["seq"]) + 1

    def publish(
        self,
        stage: str,
        sample_fingerprint: str,
        config_version: str,
        artifact_digest: str,
        expected_seq: int,
    ) -> dict:
        """Append a stage record; only a continuous advance is accepted.

        The worker prepares its record with ``next_seq()`` and publishes
        under the file lock; if the journal moved on meanwhile (a stale or
        duplicate publish) the sequence comparison fails with
        E_STALE_CHECKPOINT.
        """
        if stage not in STAGE_ORDER:
            raise PipelineError(E_INVALID_STAGE, f"unknown stage {stage!r}", stage=stage)
        with file_lock(self._lock_path):
            latest = self.latest()
            journal_next = 1 if latest is None else int(latest["seq"]) + 1
            if expected_seq != journal_next:
                raise PipelineError(
                    E_STALE_CHECKPOINT,
                    "stale or duplicate publish: sequence is not a continuous advance",
                    kind="rollback",
                    expected_seq=expected_seq,
                    journal_next_seq=journal_next,
                    stage=stage,
                )
            prev_digest = GENESIS_DIGEST if latest is None else latest["record_digest"]
            self._require_upstream_artifacts(stage)
            record = {
                "seq": expected_seq,
                "stage": stage,
                "sample_fingerprint": sample_fingerprint,
                "config_version": config_version,
                "artifact_digest": artifact_digest,
                "prev_digest": prev_digest,
            }
            record["record_digest"] = hash_obj(record)
            atomic_write_bytes(
                self._record_path(expected_seq), canonical_json_bytes(record)
            )
            atomic_write_bytes(
                self._latest_path,
                canonical_json_bytes(
                    {"seq": expected_seq, "record_digest": record["record_digest"]}
                ),
            )
            return record

    def _require_upstream_artifacts(self, stage: str) -> None:
        idx = STAGE_ORDER.index(stage)
        for upstream in STAGE_ORDER[:idx]:
            path = os.path.join(self.artifacts_dir, f"{upstream}.json")
            if not os.path.exists(path):
                raise PipelineError(
                    E_INVALID_STAGE,
                    f"cannot publish {stage!r}: upstream artefact {upstream!r} is missing",
                    stage=stage, missing_upstream=upstream,
                )

    # -------------------------------------------------------------- verify
    def classify_snapshot(self) -> dict:
        """Classify the on-disk journal as normal / rollback / tamper."""
        latest = self.latest()
        records = self.records()
        if latest is None:
            return {"status": "empty", "records": 0}
        if records and int(latest["seq"]) < len(records):
            return {
                "status": "rollback",
                "detail": f"latest seq {latest['seq']} behind {len(records)} records",
            }
        prev_digest = GENESIS_DIGEST
        for expected_seq, rec in enumerate(records, start=1):
            if rec.get("seq") != expected_seq:
                return {
                    "status": "rollback",
                    "detail": f"sequence gap at record {expected_seq}: got seq {rec.get('seq')}",
                }
            if rec.get("prev_digest") != prev_digest:
                return {
                    "status": "tamper",
                    "detail": f"prev_digest mismatch at seq {expected_seq}",
                }
            body = {k: v for k, v in rec.items() if k != "record_digest"}
            if hash_obj(body) != rec.get("record_digest"):
                return {
                    "status": "tamper",
                    "detail": f"record_digest mismatch at seq {expected_seq}",
                }
            prev_digest = rec["record_digest"]
        if records and records[-1]["record_digest"] != latest["record_digest"]:
            return {"status": "tamper", "detail": "latest pointer does not match journal tail"}
        if int(latest["seq"]) != len(records):
            return {
                "status": "rollback",
                "detail": f"latest seq {latest['seq']} != {len(records)} records",
            }
        return {"status": "normal", "records": len(records)}

    def verify_or_raise(self) -> dict:
        snapshot = self.classify_snapshot()
        if snapshot["status"] in ("rollback", "tamper"):
            raise PipelineError(
                E_STALE_CHECKPOINT,
                f"checkpoint journal is not a normal advance: {snapshot['status']}",
                kind=snapshot["status"], detail=snapshot.get("detail"),
            )
        return snapshot

    def completed_stages(self, sample_fingerprint: str, config_version: str) -> list[str]:
        """Stages with valid checkpoints for this sample/config pair."""
        self.verify_or_raise()
        return [
            rec["stage"]
            for rec in self.records()
            if rec["sample_fingerprint"] == sample_fingerprint
            and rec["config_version"] == config_version
        ]
