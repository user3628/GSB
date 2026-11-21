"""Persistent stage checkpoints: atomic publish, scan, recovery.

The checkpoint log lives in ``<workspace>/checkpoints/``. Every record is a
canonical-JSON file named ``<seq:08d>_<checksum8>.json`` containing: stage
name, stage instance id, input shard content fingerprint, config version,
config snapshot, upstream fingerprints, artifact path + digest, a monotonic
sequence number and a self checksum. Records are written atomically
(tmp + fsync + os.replace), so an interrupted write never leaves a
half-written record.

Recovery ignores corrupted/incomplete records and records with duplicate
sequence numbers, verifies the contiguous chain from seq 1 (including
artifact digests) and folds the chain into the *effective* state: a record
is effective only if every declared upstream instance is effective with a
matching fingerprint; superseded or misaligned records become orphans.
"""

from __future__ import annotations

import json
from pathlib import Path

from .errors import E_INVALID_STAGE, E_STALE_CHECKPOINT, EngineError
from .locking import FileLock
from .serialization import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_json,
    read_file_bytes,
    sha256_hex,
)

PUBLISHED_SNAPSHOT = "state/published.json"


def _record_checksum(record: dict) -> str:
    body = {key: value for key, value in record.items() if key != "checksum"}
    return sha256_hex(canonical_json(body))


def make_record(
    *,
    seq: int,
    stage: str,
    instance: str,
    view_id,
    fingerprint: str,
    input_fingerprint: str,
    config_version: str,
    config: dict,
    upstream_fingerprints: dict,
    artifact_path: str,
    artifact_digest: str,
    prev_seq: int,
) -> dict:
    record = {
        "seq": seq,
        "stage": stage,
        "instance": instance,
        "view_id": view_id,
        "fingerprint": fingerprint,
        "input_fingerprint": input_fingerprint,
        "config_version": config_version,
        "config": config,
        "upstream_fingerprints": dict(upstream_fingerprints),
        "artifact_path": artifact_path,
        "artifact_digest": artifact_digest,
        "prev_seq": prev_seq,
    }
    record["checksum"] = _record_checksum(record)
    return record


def _is_valid(record: dict, effective: dict) -> bool:
    for upstream, fp in record["upstream_fingerprints"].items():
        current = effective.get(upstream)
        if current is None or current["fingerprint"] != fp:
            return False
    return True


def fold_chain(chain: list):
    """Fold an ordered valid chain into (effective_state, orphaned_records)."""
    effective = {}
    orphaned = []
    for record in chain:
        if _is_valid(record, effective):
            effective[record["instance"]] = record
            changed = True
            while changed:
                changed = False
                for inst, current in list(effective.items()):
                    if not _is_valid(current, effective):
                        orphaned.append(current)
                        del effective[inst]
                        changed = True
        else:
            orphaned.append(record)
    return effective, orphaned


class CheckpointStore:
    def __init__(self, workspace):
        self.workspace = Path(workspace)
        self.dir = self.workspace / "checkpoints"
        self.lock_path = self.workspace / "locks" / "publish.lock"
        self.snapshot_path = self.workspace / PUBLISHED_SNAPSHOT

    # ------------------------------------------------------------ scanning
    def scan(self):
        """Return (chain, ignored, truncation) from the on-disk log."""
        ignored = []
        candidates = {}
        if self.dir.is_dir():
            for path in sorted(self.dir.glob("*.json")):
                try:
                    record = json.loads(read_file_bytes(path).decode("utf-8"))
                except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                    ignored.append({"file": path.name, "reason": "unparseable"})
                    continue
                if not isinstance(record, dict) or "checksum" not in record:
                    ignored.append({"file": path.name, "reason": "incomplete"})
                    continue
                if record.get("checksum") != _record_checksum(record):
                    ignored.append({"file": path.name, "reason": "checksum_mismatch"})
                    continue
                seq = record.get("seq")
                if not isinstance(seq, int) or seq < 1:
                    ignored.append({"file": path.name, "reason": "bad_seq"})
                    continue
                if seq in candidates:
                    ignored.append({"file": path.name, "reason": "duplicate_seq"})
                    continue
                candidates[seq] = record

        chain = []
        truncation = None
        expected = 1
        while expected in candidates:
            record = candidates[expected]
            if record.get("prev_seq") != expected - 1:
                truncation = {"seq": expected, "reason": "prev_seq_mismatch"}
                break
            artifact = self.workspace / record["artifact_path"]
            try:
                digest = sha256_hex(read_file_bytes(artifact))
            except OSError:
                truncation = {"seq": expected, "reason": "artifact_missing"}
                break
            if digest != record["artifact_digest"]:
                truncation = {"seq": expected, "reason": "artifact_digest_mismatch"}
                break
            chain.append(record)
            expected += 1
        return chain, ignored, truncation

    def effective_state(self):
        chain, _ignored, _truncation = self.scan()
        return fold_chain(chain)

    # ----------------------------------------------------------- publishing
    def publish(
        self,
        *,
        stage: str,
        instance: str,
        view_id,
        fingerprint: str,
        input_fingerprint: str,
        config_version: str,
        config: dict,
        upstream_fingerprints: dict,
        artifact_bytes: bytes,
        artifact_rel_path: str,
        expected_prev_seq: int,
        lock_timeout_ms: int = 5000,
    ) -> dict:
        """Atomically publish one stage artifact + checkpoint under the lock."""
        with FileLock(self.lock_path, timeout_ms=lock_timeout_ms):
            chain, _ignored, _truncation = self.scan()
            effective, _orphaned = fold_chain(chain)
            current_seq = len(chain)
            if expected_prev_seq != current_seq:
                raise EngineError(
                    E_STALE_CHECKPOINT,
                    "checkpoint sequence is not the chain tip",
                    {
                        "instance": instance,
                        "expected_prev_seq": expected_prev_seq,
                        "current_seq": current_seq,
                    },
                )
            existing = effective.get(instance)
            if existing is not None and existing["fingerprint"] == fingerprint:
                raise EngineError(
                    E_STALE_CHECKPOINT,
                    "duplicate publish of an identical stage instance",
                    {"instance": instance, "fingerprint": fingerprint},
                )
            for upstream, fp in upstream_fingerprints.items():
                upstream_rec = effective.get(upstream)
                if upstream_rec is None:
                    raise EngineError(
                        E_INVALID_STAGE,
                        f"prerequisite stage '{upstream}' has no published artifact",
                        {"instance": instance, "missing_upstream": upstream},
                    )
                if upstream_rec["fingerprint"] != fp:
                    raise EngineError(
                        E_STALE_CHECKPOINT,
                        f"upstream fingerprint mismatch for '{upstream}'",
                        {
                            "instance": instance,
                            "upstream": upstream,
                            "declared": fp,
                            "published": upstream_rec["fingerprint"],
                        },
                    )

            seq = current_seq + 1
            artifact_abs = self.workspace / artifact_rel_path
            atomic_write_bytes(artifact_abs, artifact_bytes)
            record = make_record(
                seq=seq,
                stage=stage,
                instance=instance,
                view_id=view_id,
                fingerprint=fingerprint,
                input_fingerprint=input_fingerprint,
                config_version=config_version,
                config=config,
                upstream_fingerprints=upstream_fingerprints,
                artifact_path=artifact_rel_path,
                artifact_digest=sha256_hex(artifact_bytes),
                prev_seq=current_seq,
            )
            record_name = f"{seq:08d}_{record['checksum'][:8]}.json"
            atomic_write_bytes(self.dir / record_name, canonical_json(record))

            effective[instance] = record
            self._write_snapshot(effective, seq)
            return record

    def _write_snapshot(self, effective: dict, last_seq: int) -> None:
        snapshot = {
            "last_seq": last_seq,
            "instances": {
                instance: {
                    "seq": rec["seq"],
                    "stage": rec["stage"],
                    "view_id": rec["view_id"],
                    "fingerprint": rec["fingerprint"],
                    "input_fingerprint": rec["input_fingerprint"],
                    "config_version": rec["config_version"],
                    "config": rec["config"],
                    "upstream_fingerprints": rec["upstream_fingerprints"],
                    "artifact_path": rec["artifact_path"],
                    "artifact_digest": rec["artifact_digest"],
                }
                for instance, rec in sorted(effective.items())
            },
        }
        atomic_write_json(self.snapshot_path, snapshot)

    # ------------------------------------------------------------- recovery
    def recover(self) -> dict:
        """Rebuild the published snapshot from the log; return a report."""
        with FileLock(self.lock_path, timeout_ms=5000):
            chain, ignored, truncation = self.scan()
            effective, orphaned = fold_chain(chain)
            last_seq = len(chain)
            self._write_snapshot(effective, last_seq)
            return {
                "resumed_seq": last_seq,
                "valid_checkpoints": len(chain),
                "effective_instances": sorted(effective.keys()),
                "ignored_records": ignored,
                "orphaned_records": [
                    {"seq": rec["seq"], "instance": rec["instance"]} for rec in orphaned
                ],
                "truncation": truncation,
            }
