from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from .canonical import atomic_write_json, digest_obj, read_json
from .errors import CabinError


class SnapshotStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.records_dir = self.root / "snapshots"
        self.head_path = self.records_dir / "head.json"

    def _record_path(self, seq: int, stage: str) -> Path:
        return self.records_dir / f"{seq:08d}-{stage}.json"

    def read_head(self) -> dict[str, Any] | None:
        if not self.head_path.exists():
            return None
        try:
            return read_json(self.head_path)
        except (OSError, ValueError) as exc:
            raise CabinError(
                "E_SNAPSHOT_TAMPERED",
                "snapshot head is unreadable",
                details={"path": str(self.head_path), "reason": str(exc)},
                http_status=409,
            ) from exc

    def _records(self) -> list[dict[str, Any]]:
        if not self.records_dir.exists():
            return []
        records: list[dict[str, Any]] = []
        for path in sorted(self.records_dir.glob("*.json")):
            if path.name == "head.json":
                continue
            try:
                record = read_json(path)
            except (OSError, ValueError) as exc:
                raise CabinError(
                    "E_SNAPSHOT_TAMPERED",
                    "snapshot record is unreadable",
                    details={"path": str(path), "reason": str(exc)},
                    http_status=409,
                ) from exc
            record["_path"] = str(path)
            records.append(record)
        return records

    @staticmethod
    def _record_digest(record: dict[str, Any]) -> str:
        payload = {key: value for key, value in record.items() if key not in {"record_digest", "_path"}}
        return digest_obj(payload)

    @staticmethod
    def _chain_digest(previous_chain_digest: str, record_digest: str) -> str:
        return digest_obj(
            {
                "previous_chain_digest": previous_chain_digest,
                "record_digest": record_digest,
            }
        )

    def append(
        self,
        *,
        stage: str,
        shard_fingerprint: str,
        config_version: int,
        edit_version: int,
        artifact: Any,
    ) -> dict[str, Any]:
        self.records_dir.mkdir(parents=True, exist_ok=True)
        head = self.read_head()
        records = self._records()
        if head is not None:
            self.verify()
        if head is None and records:
            raise CabinError(
                "E_SNAPSHOT_TAMPERED",
                "snapshot records exist without a head",
                details={"record_count": len(records)},
                http_status=409,
            )
        next_seq = 1 if head is None else int(head["seq"]) + 1
        previous_chain = "GENESIS" if head is None else str(head["chain_digest"])
        artifact_digest = digest_obj(artifact)
        existing = self._record_path(next_seq, stage)
        if existing.exists():
            existing_record = read_json(existing)
            record_digest = self._record_digest(existing_record)
            if (
                existing_record.get("stage") == stage
                and existing_record.get("artifact_digest") == artifact_digest
                and existing_record.get("shard_fingerprint") == shard_fingerprint
                and existing_record.get("config_version") == config_version
                and existing_record.get("edit_version") == edit_version
                and existing_record.get("previous_chain_digest") == previous_chain
                and existing_record.get("record_digest") == record_digest
            ):
                chain_digest = self._chain_digest(previous_chain, record_digest)
                head_payload = {
                    "seq": next_seq,
                    "stage": stage,
                    "record_digest": record_digest,
                    "artifact_digest": artifact_digest,
                    "chain_digest": chain_digest,
                    "shard_fingerprint": shard_fingerprint,
                    "config_version": config_version,
                    "edit_version": edit_version,
                }
                atomic_write_json(self.head_path, head_payload)
                return existing_record
            raise CabinError(
                "E_SNAPSHOT_TAMPERED",
                "uncommitted snapshot record conflicts with deterministic replay",
                details={"path": str(existing), "seq": next_seq, "stage": stage},
                http_status=409,
            )
        record_payload = {
            "seq": next_seq,
            "stage": stage,
            "shard_fingerprint": shard_fingerprint,
            "config_version": config_version,
            "edit_version": edit_version,
            "artifact_digest": artifact_digest,
            "artifact": artifact,
            "previous_chain_digest": previous_chain,
            "created_ns": time.time_ns(),
        }
        record_digest = self._record_digest(record_payload)
        record = {**record_payload, "record_digest": record_digest}
        chain_digest = self._chain_digest(previous_chain, record_digest)
        atomic_write_json(existing, record)
        head_payload = {
            "seq": next_seq,
            "stage": stage,
            "record_digest": record_digest,
            "artifact_digest": artifact_digest,
            "chain_digest": chain_digest,
            "shard_fingerprint": shard_fingerprint,
            "config_version": config_version,
            "edit_version": edit_version,
        }
        atomic_write_json(self.head_path, head_payload)
        return record

    def verify(
        self,
        *,
        expected_state_head: dict[str, Any] | None = None,
        expected_stage_digests: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        head = self.read_head()
        records = self._records()
        if head is None and not records:
            return {"ok": True, "status": "empty", "seq": 0, "records": 0}
        if head is None:
            raise CabinError(
                "E_SNAPSHOT_TAMPERED",
                "snapshot head is missing",
                details={"records": len(records)},
                http_status=409,
            )
        if expected_state_head is not None:
            if int(expected_state_head.get("seq", 0)) > int(head.get("seq", 0)):
                raise CabinError(
                    "E_SNAPSHOT_ROLLBACK",
                    "snapshot head moved backwards relative to committed workspace state",
                    details={
                        "state_seq": expected_state_head.get("seq", 0),
                        "head_seq": head.get("seq", 0),
                    },
                    http_status=409,
                )
            if expected_state_head.get("chain_digest") and (
                expected_state_head.get("chain_digest") != head.get("chain_digest")
                or expected_state_head.get("record_digest") != head.get("record_digest")
            ):
                raise CabinError(
                    "E_SNAPSHOT_TAMPERED",
                    "snapshot head fingerprint differs from committed workspace state",
                    details={
                        "state_chain_digest": expected_state_head.get("chain_digest"),
                        "head_chain_digest": head.get("chain_digest"),
                    },
                    http_status=409,
                )
        if not records:
            raise CabinError(
                "E_SNAPSHOT_ROLLBACK",
                "snapshot head exists but records are absent",
                details={"head_seq": head.get("seq", 0)},
                http_status=409,
            )
        ordered = sorted(records, key=lambda record: int(record.get("seq", -1)))
        expected_seq = 1
        previous_chain = "GENESIS"
        last_stage_digests: dict[str, str] = {}
        for record in ordered:
            seq = record.get("seq")
            if seq != expected_seq:
                raise CabinError(
                    "E_SNAPSHOT_TAMPERED",
                    "snapshot sequence is non-contiguous",
                    details={"expected_seq": expected_seq, "actual_seq": seq},
                    http_status=409,
                )
            record_digest = self._record_digest(record)
            if record.get("record_digest") != record_digest:
                raise CabinError(
                    "E_SNAPSHOT_TAMPERED",
                    "snapshot record digest mismatch",
                    details={"seq": seq, "path": record.get("_path")},
                    http_status=409,
                )
            if record.get("previous_chain_digest") != previous_chain:
                raise CabinError(
                    "E_SNAPSHOT_TAMPERED",
                    "snapshot chain predecessor mismatch",
                    details={"seq": seq},
                    http_status=409,
                )
            if digest_obj(record.get("artifact")) != record.get("artifact_digest"):
                raise CabinError(
                    "E_SNAPSHOT_TAMPERED",
                    "snapshot artifact digest mismatch",
                    details={"seq": seq, "stage": record.get("stage")},
                    http_status=409,
                )
            chain_digest = self._chain_digest(previous_chain, record_digest)
            previous_chain = chain_digest
            last_stage_digests[str(record.get("stage"))] = str(record.get("artifact_digest"))
            expected_seq += 1
        last = ordered[-1]
        head_seq = int(head.get("seq", -1))
        if expected_seq - 1 < head_seq:
            raise CabinError(
                "E_SNAPSHOT_ROLLBACK",
                "snapshot tail has been removed",
                details={
                    "highest_record_seq": expected_seq - 1,
                    "head_seq": head_seq,
                },
                http_status=409,
            )
        if expected_seq - 1 > head_seq:
            raise CabinError(
                "E_SNAPSHOT_ROLLBACK",
                "snapshot head lags behind committed records",
                details={
                    "highest_record_seq": expected_seq - 1,
                    "head_seq": head_seq,
                },
                http_status=409,
            )
        if head.get("record_digest") != last.get("record_digest") or head.get(
            "chain_digest"
        ) != previous_chain:
            raise CabinError(
                "E_SNAPSHOT_TAMPERED",
                "snapshot head does not match the terminal record",
                details={"head_seq": head_seq},
                http_status=409,
            )
        if expected_stage_digests:
            for stage, expected_digest in expected_stage_digests.items():
                actual = last_stage_digests.get(stage)
                if actual is not None and actual != expected_digest:
                    raise CabinError(
                        "E_SNAPSHOT_TAMPERED",
                        "snapshot stage artifact differs from workspace state",
                        details={
                            "stage": stage,
                            "expected_artifact_digest": expected_digest,
                            "snapshot_artifact_digest": actual,
                        },
                        http_status=409,
                    )
        return {
            "ok": True,
            "status": "verified",
            "seq": head_seq,
            "chain_digest": previous_chain,
            "records": len(ordered),
            "stage_digests": last_stage_digests,
        }
